"""
日本株スクリーニング Web アプリ — 累積下落＋リバウンドなし検知
Flask + yfinance + Plotly
JPX上場銘柄一覧から自動取得 → 時価総額70億円以上を対象
バックグラウンドジョブ + ポーリング (Render 30秒タイムアウト対策)
ジョブ状態はファイルに永続化 (インスタンス再起動対策)
"""

import io
import json
import logging
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
from flask import Flask, jsonify, render_template, request

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ── 設定 ───────────────────────────────────────────
JPX_URL = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
MIN_MARKET_CAP = 7_000_000_000  # 70億円
MAX_WORKERS = 50
LOOKBACK_DAYS = 60

# チャート用: 一方的下落検知パラメータ
SELLOFF_VOLATILITY_MULT = 1.5
SELLOFF_BOUNCE_RATIO = 0.30
SELLOFF_TAIL_BARS = 5

# JPX 銘柄名キャッシュ
_jpx_names: dict[str, str] = {}

# ── ファイルベース ジョブ管理 ──────────────────────
JOBS_DIR = Path(tempfile.gettempdir()) / "stock_screener_jobs"
JOBS_DIR.mkdir(exist_ok=True)
PROGRESS_FILE = JOBS_DIR / "progress.json"

_jobs_lock = threading.Lock()
_jobs_mem: dict[str, dict] = {}


def _job_path(job_id: str) -> Path:
    return JOBS_DIR / f"{job_id}.json"


def _save_job(job_id: str, data: dict):
    """ジョブ状態をファイルに原子的に書き出す"""
    path = _job_path(job_id)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


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
    path = _job_path(job_id)
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
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
    """現在のメモリ状態をファイルに書き出す"""
    with _jobs_lock:
        if job_id in _jobs_mem:
            data = _jobs_mem[job_id].copy()
    _save_job(job_id, data)


# ── 中断再開用プログレス管理 ──────────────────────
def _save_progress(settings: dict, tickers: list[str],
                   processed: list[str], results: list[dict]):
    data = {
        "settings": settings,
        "tickers": tickers,
        "processed": processed,
        "results": results,
    }
    tmp = PROGRESS_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    tmp.replace(PROGRESS_FILE)


def _load_progress() -> dict | None:
    if not PROGRESS_FILE.exists():
        return None
    try:
        return json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, KeyError):
        return None


def _delete_progress():
    try:
        PROGRESS_FILE.unlink(missing_ok=True)
    except Exception:
        pass


# ── JPX 銘柄一覧取得 ──────────────────────────────
def fetch_jpx_tickers() -> list[str]:
    """JPX Excelからプライム市場の銘柄のみ取得する"""
    global _jpx_names
    r = http_requests.get(JPX_URL, timeout=30)
    r.raise_for_status()

    df = pd.read_excel(io.BytesIO(r.content), engine="xlrd")
    df.columns = [
        "date", "code", "name", "market",
        "sec33_code", "sec33", "sec17_code", "sec17",
        "scale_code", "scale",
    ]

    total_rows = len(df)
    stocks = df[~df["market"].str.contains("ETF|REIT|PRO", na=False)].copy()
    after_etf = len(stocks)
    stocks = stocks[stocks["market"].str.contains("プライム", na=False)]
    after_prime = len(stocks)

    logging.info(
        "JPXフィルタ: 全%d行 → ETF等除外 %d → プライム市場 %d 銘柄",
        total_rows, after_etf, after_prime,
    )

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


# ── 設定パース ────────────────────────────────────
def _parse_settings(raw: dict | None) -> dict:
    defaults = {"threshold": -1.0, "days": 4, "wick_pct": 50.0}
    if not raw:
        return defaults
    return {
        "threshold": float(raw.get("threshold", defaults["threshold"])),
        "days": max(1, min(10, int(raw.get("days", defaults["days"])))),
        "wick_pct": float(raw.get("wick_pct", defaults["wick_pct"])),
    }


# ── 1銘柄のスクリーニング ─────────────────────────
def screen_worker(ticker: str, settings: dict) -> dict | None:
    """累積下落＋日中リバウンドなし判定
    閾値以下の銘柄を全て返す。上髭はデータとして返し、フロントで表示する。
    """
    try:
        tk = yf.Ticker(ticker)

        try:
            mcap = tk.fast_info.get("marketCap", 0) or 0
        except Exception:
            mcap = 0
        if mcap < MIN_MARKET_CAP:
            return None

        n_days = settings["days"]
        end = datetime.now()
        start = end - timedelta(days=LOOKBACK_DAYS)
        df = tk.history(start=start, end=end, auto_adjust=True)
        if df.empty:
            return None
        df = df.dropna(subset=["Close"])
        if len(df) < n_days + 1:
            return None

        current = float(df["Close"].iloc[-1])
        ref_close = float(df["Close"].iloc[-(n_days + 1)])
        cum_change = (current - ref_close) / ref_close * 100

        threshold = settings["threshold"]

        # 累積下落率が閾値超なら対象外
        if cum_change > threshold:
            return None

        # 期間中の各日の上髭を計算 (フィルタではなく表示用データ)
        max_wick = 0.0
        for i in range(n_days):
            d_idx = -(n_days - i)  # 古い日 → 新しい日の順
            o = float(df["Open"].iloc[d_idx])
            h = float(df["High"].iloc[d_idx])
            l = float(df["Low"].iloc[d_idx])
            c = float(df["Close"].iloc[d_idx])
            upper_wick = h - max(o, c)
            day_range = h - l
            wick = (upper_wick / day_range * 100) if day_range > 0 else 0.0
            if wick > max_wick:
                max_wick = wick

        # 上髭が許容値以下かどうか (表示用フラグ)
        wick_tol = settings["wick_pct"]
        no_rebound = max_wick <= wick_tol

        name = get_ticker_name(ticker)
        return {
            "ticker": ticker,
            "name": name,
            "price": round(current, 1),
            "ref_close": round(ref_close, 1),
            "cum_change": round(cum_change, 2),
            "max_wick": round(max_wick, 1),
            "no_rebound": no_rebound,
        }

    except Exception:
        return None


# ── バックグラウンドスクリーニング処理 ────────────
def _run_screening(job_id: str, settings: dict, resume_data: dict | None = None):
    if resume_data:
        tickers = resume_data["tickers"]
        processed_set = set(resume_data["processed"])
        prev_results = resume_data["results"]
        logging.info(
            "スクリーニング再開: %d 銘柄中 %d 処理済み (%d 件検知済み)",
            len(tickers), len(processed_set), len(prev_results),
        )
    else:
        try:
            tickers = fetch_jpx_tickers()
        except Exception as e:
            _update_job(job_id, status="error", message=f"JPX一覧の取得に失敗: {e}")
            return
        processed_set = set()
        prev_results = []
        logging.info(
            "スクリーニング開始: %d 銘柄 (閾値=%s%%, 日数=%s, 上髭=%s%%)",
            len(tickers), settings["threshold"], settings["days"], settings["wick_pct"],
        )

    total = len(tickers)
    remaining = [t for t in tickers if t not in processed_set]

    # ジョブ状態を再開データで初期化
    with _jobs_lock:
        if job_id in _jobs_mem:
            _jobs_mem[job_id]["results"] = list(prev_results)
            _jobs_mem[job_id]["hits"] = len(prev_results)

    _update_job(job_id, status="running", total=total,
                processed=total - len(remaining), message="スクリーニング中...")

    # 進捗を保存 (新規開始時は tickers リストを記録)
    processed_list = list(processed_set)
    results_list = list(prev_results)
    _save_progress(settings, tickers, processed_list, results_list)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(screen_worker, t, settings): t for t in remaining}

        count = 0
        for future in as_completed(futures):
            ticker = futures[future]
            result = future.result()
            _increment_processed(job_id)
            processed_list.append(ticker)
            if result:
                _append_result(job_id, result)
                results_list.append(result)
            count += 1
            if count % 20 == 0:
                _flush_job(job_id)
                _save_progress(settings, tickers, processed_list, results_list)

    _delete_progress()

    job = _get_job(job_id)
    hits = job["hits"] if job else 0
    no_rebound_cnt = 0
    if job:
        no_rebound_cnt = sum(1 for r in job["results"] if r.get("no_rebound"))
    logging.info(
        "スクリーニング完了: %d 銘柄処理 / %d 件検知 (うちリバウンドなし %d 件)",
        total, hits, no_rebound_cnt,
    )
    _update_job(job_id, status="done", message="完了")


# ── 一方的下落の検知 (チャート用) ─────────────────
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
    raw = request.get_json(silent=True) or {}
    settings = _parse_settings(raw.get("settings"))
    fresh = raw.get("fresh", False)

    resume_data = None
    if fresh:
        _delete_progress()
    else:
        progress = _load_progress()
        if progress and progress.get("settings") == settings:
            resume_data = progress
        elif progress:
            _delete_progress()

    job_id = _new_job()
    t = threading.Thread(target=_run_screening,
                         args=(job_id, settings, resume_data), daemon=True)
    t.start()
    return jsonify({"ok": True, "job_id": job_id, "resumed": resume_data is not None})


@app.route("/api/screen/poll/<job_id>")
def api_screen_poll(job_id):
    """進捗と新しい結果を返す (cursor ベースの差分配信)"""
    job = _get_job(job_id)
    if job is None:
        return jsonify({"ok": False, "error": "ジョブが見つかりません"}), 404

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


@app.route("/api/progress/check")
def api_progress_check():
    """中断された進捗があるか確認"""
    progress = _load_progress()
    if progress is None:
        return jsonify({"ok": True, "has_progress": False})
    return jsonify({
        "ok": True,
        "has_progress": True,
        "processed": len(progress.get("processed", [])),
        "total": len(progress.get("tickers", [])),
        "hits": len(progress.get("results", [])),
        "settings": progress.get("settings", {}),
    })


@app.route("/api/progress/reset", methods=["POST"])
def api_progress_reset():
    """中断データを削除"""
    _delete_progress()
    return jsonify({"ok": True})


@app.route("/api/chart/<ticker>")
def api_chart(ticker):
    chart_json = build_chart_json(ticker)
    if chart_json is None:
        return jsonify({"ok": False, "error": "チャートデータを取得できませんでした"})
    return jsonify({"ok": True, "chart": json.loads(chart_json)})


if __name__ == "__main__":
    app.run(debug=True, port=5000)
