"""
日本株スクリーニング Web アプリ
Flask + yfinance + Plotly
JPX上場銘柄一覧から自動取得 → 2段階スクリーニング
バックグラウンドジョブ + ポーリング (Render 30秒タイムアウト対策)
ジョブ状態はファイルに永続化 (インスタンス再起動対策)
"""

import io
import json
import os
import tempfile
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import plotly
import plotly.graph_objects as go
import requests as http_requests
from plotly.subplots import make_subplots
import yfinance as yf
from flask import Flask, jsonify, render_template

app = Flask(__name__)

# ── 設定 ───────────────────────────────────────────
JPX_URL = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
MIN_MARKET_CAP = 7_000_000_000  # 70億円
MAX_WORKERS = 50

DAILY_CHANGE_THRESHOLD = 3.0
NEAR_EXTREME_PCT = 5.0
MA_PERIOD = 25
MA_DEVIATION_THRESHOLD = 5.0
ANOMALY_SIGMA = 2.0
LOOKBACK_DAYS = 60

SELLOFF_VOLATILITY_MULT = 1.5
SELLOFF_BOUNCE_RATIO = 0.30
SELLOFF_TAIL_BARS = 5

# JPX 銘柄名キャッシュ
_jpx_names: dict[str, str] = {}

# ── ファイルベース ジョブ管理 ──────────────────────
JOBS_DIR = Path(tempfile.gettempdir()) / "stock_screener_jobs"
JOBS_DIR.mkdir(exist_ok=True)

_jobs_lock = threading.Lock()
# インメモリキャッシュ (実行中のジョブのみ高速アクセス用)
_jobs_mem: dict[str, dict] = {}


def _job_path(job_id: str) -> Path:
    # job_id は hex のみなので安全
    return JOBS_DIR / f"{job_id}.json"


def _save_job(job_id: str, data: dict):
    """ジョブ状態をファイルに原子的に書き出す"""
    path = _job_path(job_id)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)  # atomic on POSIX; best-effort on Windows


def _new_job() -> str:
    job_id = uuid.uuid4().hex[:12]
    data = {
        "status": "starting",
        "message": "JPX銘柄一覧を取得中...",
        "total": 0,
        "processed": 0,
        "hits": 0,
        "results": [],
    }
    with _jobs_lock:
        _jobs_mem[job_id] = data
    _save_job(job_id, data)
    return job_id


def _get_job(job_id: str) -> dict | None:
    """メモリ → ファイルの順でジョブを探す"""
    with _jobs_lock:
        if job_id in _jobs_mem:
            return _jobs_mem[job_id]
    # メモリに無い場合はファイルから復元 (再起動後)
    path = _job_path(job_id)
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            # 実行中だったジョブはサーバー再起動で死んでいる
            if data["status"] in ("starting", "running"):
                data["status"] = "lost"
                data["message"] = "サーバーが再起動されたためジョブが中断されました"
                _save_job(job_id, data)
            with _jobs_lock:
                _jobs_mem[job_id] = data
            return data
        except (json.JSONDecodeError, KeyError):
            return None
    return None


def _update_job(job_id: str, **kwargs):
    with _jobs_lock:
        if job_id in _jobs_mem:
            _jobs_mem[job_id].update(kwargs)
            data = _jobs_mem[job_id]
    _save_job(job_id, data)


def _append_result(job_id: str, result: dict):
    with _jobs_lock:
        if job_id in _jobs_mem:
            _jobs_mem[job_id]["results"].append(result)
            _jobs_mem[job_id]["hits"] = len(_jobs_mem[job_id]["results"])


def _increment_processed(job_id: str):
    with _jobs_lock:
        if job_id in _jobs_mem:
            _jobs_mem[job_id]["processed"] += 1


def _flush_job(job_id: str):
    """現在のメモリ状態をファイルに書き出す (重い処理中は間引いて呼ぶ)"""
    with _jobs_lock:
        if job_id in _jobs_mem:
            data = _jobs_mem[job_id].copy()
    _save_job(job_id, data)


# ── JPX 銘柄一覧取得 ──────────────────────────────
def fetch_jpx_tickers() -> list[str]:
    global _jpx_names
    r = http_requests.get(JPX_URL, timeout=30)
    r.raise_for_status()

    df = pd.read_excel(io.BytesIO(r.content), engine="xlrd")
    df.columns = [
        "date", "code", "name", "market",
        "sec33_code", "sec33", "sec17_code", "sec17",
        "scale_code", "scale",
    ]

    stocks = df[~df["market"].str.contains("ETF|REIT|PRO", na=False)].copy()

    tickers = []
    for _, row in stocks.iterrows():
        code = str(row["code"]).strip()
        if not code:
            continue
        ticker = f"{code}.T"
        tickers.append(ticker)
        _jpx_names[ticker] = str(row["name"]).strip()

    return tickers


def get_ticker_name(ticker: str) -> str:
    if ticker in _jpx_names and _jpx_names[ticker] != "-":
        return _jpx_names[ticker]
    try:
        info = yf.Ticker(ticker).info
        return info.get("longName") or info.get("shortName") or ticker
    except Exception:
        return ticker


# ── 1銘柄の日足スクリーニング ──────────────────────
def screen_worker(ticker: str) -> dict | None:
    try:
        tk = yf.Ticker(ticker)

        try:
            mcap = tk.fast_info.get("marketCap", 0)
        except Exception:
            mcap = 0
        if not mcap or mcap < MIN_MARKET_CAP:
            return None

        end = datetime.now()
        start = end - timedelta(days=LOOKBACK_DAYS)
        df = tk.history(start=start, end=end, auto_adjust=True)
        if df.empty:
            return None
        df = df.dropna(subset=["Close"])
        close = df["Close"].values.flatten()
        if len(close) < MA_PERIOD + 1:
            return None

        current = float(close[-1])
        prev = float(close[-2])
        change_pct = (current - prev) / prev * 100

        ytd_high = float(np.max(close))
        ytd_low = float(np.min(close))

        ma25 = float(np.mean(close[-MA_PERIOD:]))
        ma_dev = (current - ma25) / ma25 * 100

        returns_30d = np.diff(close[-31:]) / close[-31:-1] * 100
        std_30d = float(np.std(returns_30d))

        alerts = []

        if abs(change_pct) >= DAILY_CHANGE_THRESHOLD:
            tag = "急騰" if change_pct > 0 else "急落"
            alerts.append({
                "text": f"{tag} {change_pct:+.1f}%",
                "type": "up" if change_pct > 0 else "down",
            })

        if ytd_high > 0:
            d = (ytd_high - current) / ytd_high * 100
            if d <= NEAR_EXTREME_PCT:
                alerts.append({"text": f"高値圏 (高値比 -{d:.1f}%)", "type": "up"})
        if ytd_low > 0:
            d = (current - ytd_low) / ytd_low * 100
            if d <= NEAR_EXTREME_PCT:
                alerts.append({"text": f"安値圏 (安値比 +{d:.1f}%)", "type": "down"})

        if abs(ma_dev) >= MA_DEVIATION_THRESHOLD:
            alerts.append({
                "text": f"MA{MA_PERIOD}乖離 {ma_dev:+.1f}%",
                "type": "up" if ma_dev > 0 else "down",
            })

        if std_30d > 0 and abs(change_pct) >= std_30d * ANOMALY_SIGMA:
            alerts.append({
                "text": f"統計異常 ({change_pct:+.1f}% / 2σ={std_30d * ANOMALY_SIGMA:.1f}%)",
                "type": "up" if change_pct > 0 else "down",
            })

        if not alerts:
            return None

        mcap_b = mcap / 1e8
        name = get_ticker_name(ticker)

        return {
            "ticker": ticker,
            "name": name,
            "price": round(current, 1),
            "change_pct": round(change_pct, 2),
            "market_cap": f"{mcap_b:,.0f}億",
            "alerts": alerts,
        }

    except Exception:
        return None


# ── バックグラウンドスクリーニング処理 ────────────
def _run_screening(job_id: str):
    try:
        tickers = fetch_jpx_tickers()
    except Exception as e:
        _update_job(job_id, status="error", message=f"JPX一覧の取得に失敗: {e}")
        return

    total = len(tickers)
    _update_job(job_id, status="running", total=total, processed=0, message="スクリーニング中...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(screen_worker, t): t for t in tickers}

        count = 0
        for future in as_completed(futures):
            result = future.result()
            _increment_processed(job_id)
            if result:
                _append_result(job_id, result)
            count += 1
            # 20銘柄ごとにファイルへ書き出し
            if count % 20 == 0:
                _flush_job(job_id)

    _update_job(job_id, status="done", message="完了")


# ── 一方的下落の検知 ──────────────────────────────
def detect_selloff(ticker: str) -> dict | None:
    try:
        tk = yf.Ticker(ticker)
        df5 = tk.history(period="1d", interval="5m", auto_adjust=True)

        if df5.empty:
            df5 = tk.history(period="5d", interval="5m", auto_adjust=True)
            if df5.empty:
                return None
            last_date = df5.index[-1].date()
            df5 = df5[df5.index.date == last_date]

        df5 = df5[["Open", "High", "Low", "Close"]].dropna()
        if len(df5) < SELLOFF_TAIL_BARS + 1:
            return None

        highs = df5["High"].values.flatten()
        closes = df5["Close"].values.flatten()
        opens = df5["Open"].values.flatten()

        day_high = float(np.max(highs))
        current = float(closes[-1])
        drop = day_high - current

        if drop <= 0:
            return None

        daily = tk.history(period="30d", interval="1d", auto_adjust=True)
        if daily.empty or len(daily) < 5:
            return None
        intraday_ranges = (daily["High"] - daily["Low"]).values.flatten()
        range_std = float(np.std(intraday_ranges))

        if range_std <= 0 or drop < range_std * SELLOFF_VOLATILITY_MULT:
            return None

        peak_idx = int(np.argmax(highs))

        for i in range(peak_idx + 1, len(df5)):
            candle_up = float(closes[i]) - float(opens[i])
            if candle_up > 0 and candle_up >= drop * SELLOFF_BOUNCE_RATIO:
                return None

        for i in range(-SELLOFF_TAIL_BARS, 0):
            if float(closes[i]) > float(opens[i]):
                return None

        drop_pct = drop / day_high * 100
        return {
            "drop": round(drop_pct, 1),
            "peak_idx": peak_idx,
            "end_idx": len(df5) - 1,
        }
    except Exception:
        return None


# ── チャート生成 ───────────────────────────────────
def build_chart_json(ticker: str) -> str | None:
    tk = yf.Ticker(ticker)
    df = tk.history(period="1d", interval="5m", auto_adjust=True)

    if df.empty:
        df = tk.history(period="5d", interval="5m", auto_adjust=True)
        if df.empty:
            return None
        last_date = df.index[-1].date()
        df = df[df.index.date == last_date]
        if df.empty:
            return None

    df = df[["Open", "High", "Low", "Close", "Volume"]].dropna()
    if len(df) < 2:
        return None

    name = get_ticker_name(ticker)
    chart_date = df.index[-1].strftime("%Y/%m/%d")

    ma_len = min(25, len(df))
    ma = df["Close"].rolling(window=ma_len).mean()

    colors = [
        "#e74c3c" if c >= o else "#3498db"
        for o, c in zip(df["Open"], df["Close"])
    ]

    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.75, 0.25],
    )

    fig.add_trace(go.Candlestick(
        x=df.index,
        open=df["Open"], high=df["High"],
        low=df["Low"], close=df["Close"],
        increasing_line_color="#e74c3c",
        decreasing_line_color="#3498db",
        increasing_fillcolor="#e74c3c",
        decreasing_fillcolor="#3498db",
        name="価格",
    ), row=1, col=1)

    fig.add_trace(go.Scatter(
        x=df.index, y=ma,
        line=dict(color="#f39c12", width=1.5),
        name=f"MA{ma_len}",
    ), row=1, col=1)

    fig.add_trace(go.Bar(
        x=df.index, y=df["Volume"],
        marker_color=colors,
        name="出来高",
        showlegend=False,
    ), row=2, col=1)

    selloff = detect_selloff(ticker)
    shapes = []
    if selloff:
        s_idx = selloff["peak_idx"]
        e_idx = selloff["end_idx"]
        shapes.append(dict(
            type="rect", xref="x", yref="y",
            x0=max(0, s_idx - 0.5),
            x1=min(len(df) - 1, e_idx + 0.5),
            y0=float(df["Low"].iloc[s_idx:e_idx + 1].min()) * 0.999,
            y1=float(df["High"].iloc[s_idx:e_idx + 1].max()) * 1.001,
            fillcolor="rgba(248, 81, 73, 0.15)",
            line=dict(color="rgba(248, 81, 73, 0.5)", width=1, dash="dot"),
            layer="below",
        ))

    fig.update_layout(
        title=dict(text=f"{ticker}  {name}  ({chart_date})", font=dict(size=16)),
        template="plotly_dark",
        height=560,
        margin=dict(l=50, r=30, t=60, b=30),
        xaxis_rangeslider_visible=False,
        showlegend=True,
        legend=dict(orientation="h", y=1.02, x=0.5, xanchor="center"),
        shapes=shapes,
    )
    fig.update_xaxes(type="category", nticks=10, row=1, col=1)
    fig.update_xaxes(type="category", nticks=10, row=2, col=1)
    fig.update_yaxes(title_text="価格", row=1, col=1)
    fig.update_yaxes(title_text="出来高", row=2, col=1)

    tickvals = list(range(0, len(df), max(1, len(df) // 10)))
    ticktext = [df.index[i].strftime("%H:%M") for i in tickvals]
    fig.update_xaxes(tickvals=tickvals, ticktext=ticktext, row=2, col=1)
    fig.update_xaxes(tickvals=tickvals, ticktext=ticktext, row=1, col=1)

    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)


# ── ルーティング ───────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/screen/start", methods=["POST"])
def api_screen_start():
    """スクリーニングをバックグラウンドで開始し、job_id を返す"""
    job_id = _new_job()
    t = threading.Thread(target=_run_screening, args=(job_id,), daemon=True)
    t.start()
    return jsonify({"ok": True, "job_id": job_id})


@app.route("/api/screen/poll/<job_id>")
def api_screen_poll(job_id):
    """進捗と新しい結果を返す (cursor ベースの差分配信)"""
    job = _get_job(job_id)
    if job is None:
        return jsonify({"ok": False, "error": "ジョブが見つかりません"}), 404

    # クエリパラメータで cursor を受け取る
    from flask import request
    cursor = int(request.args.get("cursor", 0))

    with _jobs_lock:
        new_results = job["results"][cursor:]
        new_cursor = len(job["results"])

    return jsonify({
        "ok": True,
        "status": job["status"],
        "message": job.get("message", ""),
        "total": job["total"],
        "processed": job["processed"],
        "hits": job["hits"],
        "new_results": new_results,
        "cursor": new_cursor,
    })


@app.route("/api/chart/<ticker>")
def api_chart(ticker):
    chart_json = build_chart_json(ticker)
    if chart_json is None:
        return jsonify({"ok": False, "error": "チャートデータを取得できませんでした"})
    return jsonify({"ok": True, "chart": json.loads(chart_json)})


if __name__ == "__main__":
    app.run(debug=True, port=5000)
