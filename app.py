"""
日本株スクリーニング Web アプリ
Flask + yfinance + Plotly
JPX上場銘柄一覧から自動取得 → 2段階スクリーニング
バックグラウンドジョブ + ポーリング (Render 30秒タイムアウト対策)
ジョブ状態はファイルに永続化 (インスタンス再起動対策)
"""

import io
import json
import logging
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
from flask import Flask, jsonify, render_template, request

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

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

    # ETF / REIT / PRO Market を除外
    stocks = df[~df["market"].str.contains("ETF|REIT|PRO", na=False)].copy()
    after_etf = len(stocks)

    # プライム市場のみに絞る (時価総額70億円以上の銘柄はほぼプライム市場)
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


# ── デフォルト条件設定 ─────────────────────────────
DEFAULT_SETTINGS = {
    "daily_change":  {"enabled": True, "value": DAILY_CHANGE_THRESHOLD},
    "near_extreme":  {"enabled": True, "value": NEAR_EXTREME_PCT},
    "ma_deviation":  {"enabled": True, "value": MA_DEVIATION_THRESHOLD},
    "anomaly_sigma": {"enabled": True, "value": ANOMALY_SIGMA},
}


def _parse_settings(raw: dict | None) -> dict:
    """フロントから送られた設定をマージ (不足分はデフォルト補完)"""
    s = {}
    for key, default in DEFAULT_SETTINGS.items():
        if raw and key in raw:
            s[key] = {
                "enabled": bool(raw[key].get("enabled", default["enabled"])),
                "value":   float(raw[key].get("value", default["value"])),
            }
        else:
            s[key] = default.copy()

    # 比較対象日 (営業日ベース): [1] = 前日のみ, [1,2,3] = 前日+2日前+3日前
    if raw and "compare_days" in raw:
        days = [int(d) for d in raw["compare_days"] if int(d) in (1, 2, 3, 4)]
        s["compare_days"] = sorted(days) if days else [1]
    else:
        s["compare_days"] = [1]

    # 連続下落+リバウンドなし
    if raw and "decline" in raw:
        d = raw["decline"]
        th_raw = d.get("thresholds", [-1, -1, -2, -1])
        s["decline"] = {
            "enabled": bool(d.get("enabled", False)),
            "thresholds": [float(th_raw[i]) for i in range(4)],
            "wick_pct": float(d.get("wick_pct", 30)),
        }
    else:
        s["decline"] = {
            "enabled": False,
            "thresholds": [-1.0, -1.0, -2.0, -1.0],
            "wick_pct": 30.0,
        }

    # 累積下落+日中リバウンドなし
    if raw and "cumulative_decline" in raw:
        cd = raw["cumulative_decline"]
        s["cumulative_decline"] = {
            "enabled": bool(cd.get("enabled", False)),
            "threshold": float(cd.get("threshold", -15)),
            "days": max(1, min(10, int(cd.get("days", 4)))),
            "wick_pct": float(cd.get("wick_pct", 30)),
        }
    else:
        s["cumulative_decline"] = {
            "enabled": False,
            "threshold": -15.0,
            "days": 4,
            "wick_pct": 30.0,
        }

    return s


# ── 1銘柄の日足スクリーニング ──────────────────────
def screen_worker(ticker: str, settings: dict) -> dict | None:
    try:
        tk = yf.Ticker(ticker)

        try:
            mcap = tk.fast_info.get("marketCap", 0) or 0
        except Exception:
            mcap = 0
        if mcap < MIN_MARKET_CAP:
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

        # 複数営業日の騰落率を計算 (日足データのインデックスで遡る)
        compare_days = settings.get("compare_days", [1])
        max_day = max(compare_days)
        if len(close) < max_day + 1:
            return None

        changes: dict[int, float] = {}
        for d in compare_days:
            ref = float(close[-(d + 1)])
            changes[d] = (current - ref) / ref * 100

        change_pct = changes.get(1, 0.0)  # 前日比 (他条件でも使用)

        ytd_high = float(np.max(close))
        ytd_low = float(np.min(close))

        ma25 = float(np.mean(close[-MA_PERIOD:]))
        ma_dev = (current - ma25) / ma25 * 100

        returns_30d = np.diff(close[-31:]) / close[-31:-1] * 100
        std_30d = float(np.std(returns_30d))

        alerts = []

        # 前日比条件: いずれかの比較日が閾値を超えれば検知
        sc = settings["daily_change"]
        if sc["enabled"]:
            for d, pct in changes.items():
                if abs(pct) >= sc["value"]:
                    label = "前日比" if d == 1 else f"{d}日前比"
                    tag = "急騰" if pct > 0 else "急落"
                    alerts.append({
                        "text": f"{tag} {label} {pct:+.1f}%",
                        "type": "up" if pct > 0 else "down",
                    })

        sc = settings["near_extreme"]
        if sc["enabled"]:
            if ytd_high > 0:
                d = (ytd_high - current) / ytd_high * 100
                if d <= sc["value"]:
                    alerts.append({"text": f"高値圏 (高値比 -{d:.1f}%)", "type": "up"})
            if ytd_low > 0:
                d = (current - ytd_low) / ytd_low * 100
                if d <= sc["value"]:
                    alerts.append({"text": f"安値圏 (安値比 +{d:.1f}%)", "type": "down"})

        sc = settings["ma_deviation"]
        if sc["enabled"] and abs(ma_dev) >= sc["value"]:
            alerts.append({
                "text": f"MA{MA_PERIOD}乖離 {ma_dev:+.1f}%",
                "type": "up" if ma_dev > 0 else "down",
            })

        sc = settings["anomaly_sigma"]
        if sc["enabled"] and std_30d > 0 and abs(change_pct) >= std_30d * sc["value"]:
            alerts.append({
                "text": f"統計異常 ({change_pct:+.1f}% / {sc['value']:.0f}σ={std_30d * sc['value']:.1f}%)",
                "type": "up" if change_pct > 0 else "down",
            })

        # ── 連続下落+リバウンドなし ──
        decline_detail = None
        sc_dec = settings.get("decline", {})
        if sc_dec.get("enabled") and len(df) >= 5:
            thresholds = sc_dec.get("thresholds", [-1, -1, -2, -1])
            wick_tol = sc_dec.get("wick_pct", 30.0)

            detail = []
            all_met = True
            for i in range(4):
                d_idx = -(i + 1)  # -1, -2, -3, -4
                c_now = float(df["Close"].iloc[d_idx])
                c_prev = float(df["Close"].iloc[d_idx - 1])
                day_chg = (c_now - c_prev) / c_prev * 100

                o = float(df["Open"].iloc[d_idx])
                h = float(df["High"].iloc[d_idx])
                l = float(df["Low"].iloc[d_idx])
                c = float(df["Close"].iloc[d_idx])

                upper_wick = h - max(o, c)
                day_range = h - l
                wick = (upper_wick / day_range * 100) if day_range > 0 else 0.0

                detail.append({"change": round(day_chg, 2), "wick": round(wick, 1)})

                if day_chg > thresholds[i]:
                    all_met = False
                if wick > wick_tol:
                    all_met = False

            decline_detail = detail
            if all_met:
                alerts.append({
                    "text": "連続下落(リバウンドなし)",
                    "type": "down",
                })

        # ── 累積下落+日中リバウンドなし ──
        cum_detail = None
        sc_cum = settings.get("cumulative_decline", {})
        if sc_cum.get("enabled"):
            n_days = sc_cum.get("days", 4)
            if len(df) >= n_days + 1:
                cum_threshold = sc_cum.get("threshold", -15.0)
                cum_wick_tol = sc_cum.get("wick_pct", 30.0)

                ref_close = float(df["Close"].iloc[-(n_days + 1)])
                cum_change = (current - ref_close) / ref_close * 100

                # 期間中の各日の上髭を計算し最大値を取得
                max_wick = 0.0
                wick_ok = True
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
                    if wick > cum_wick_tol:
                        wick_ok = False

                cum_detail = {
                    "ref_close": round(ref_close, 1),
                    "cum_change": round(cum_change, 2),
                    "max_wick": round(max_wick, 1),
                    "days": n_days,
                }

                if cum_change <= cum_threshold and wick_ok:
                    alerts.append({
                        "text": f"累積下落{n_days}日 {cum_change:+.1f}%",
                        "type": "down",
                    })

        if not alerts:
            return None

        mcap_b = mcap / 1e8
        name = get_ticker_name(ticker)

        result = {
            "ticker": ticker,
            "name": name,
            "price": round(current, 1),
            "change_pct": round(change_pct, 2),
            "changes": {str(d): round(pct, 2) for d, pct in changes.items()},
            "market_cap": f"{mcap_b:,.0f}億",
            "alerts": alerts,
        }
        if decline_detail is not None:
            result["decline_detail"] = decline_detail
        if cum_detail is not None:
            result["cum_detail"] = cum_detail
        return result

    except Exception:
        return None


# ── バックグラウンドスクリーニング処理 ────────────
def _run_screening(job_id: str, settings: dict):
    try:
        tickers = fetch_jpx_tickers()
    except Exception as e:
        _update_job(job_id, status="error", message=f"JPX一覧の取得に失敗: {e}")
        return

    total = len(tickers)
    logging.info("スクリーニング開始: %d 銘柄", total)
    _update_job(job_id, status="running", total=total, processed=0,
                message="スクリーニング中...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(screen_worker, t, settings): t for t in tickers}

        count = 0
        for future in as_completed(futures):
            result = future.result()
            _increment_processed(job_id)
            if result:
                _append_result(job_id, result)
            count += 1
            if count % 20 == 0:
                _flush_job(job_id)

    job = _get_job(job_id)
    hits = job["hits"] if job else 0
    logging.info("スクリーニング完了: %d 銘柄処理 / %d 件検知", total, hits)
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
    raw = request.get_json(silent=True) or {}
    settings = _parse_settings(raw.get("settings"))
    job_id = _new_job()
    # フロントがテーブルヘッダーを構築するための情報をジョブに保存
    _update_job(job_id,
                compare_days=settings["compare_days"],
                decline_enabled=settings["decline"]["enabled"],
                cum_decline_enabled=settings["cumulative_decline"]["enabled"])
    t = threading.Thread(target=_run_screening, args=(job_id, settings), daemon=True)
    t.start()
    return jsonify({"ok": True, "job_id": job_id})


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
        "compare_days": job.get("compare_days", [1]),
        "decline_enabled": job.get("decline_enabled", False),
        "cum_decline_enabled": job.get("cum_decline_enabled", False),
    })


@app.route("/api/chart/<ticker>")
def api_chart(ticker):
    chart_json = build_chart_json(ticker)
    if chart_json is None:
        return jsonify({"ok": False, "error": "チャートデータを取得できませんでした"})
    return jsonify({"ok": True, "chart": json.loads(chart_json)})


if __name__ == "__main__":
    app.run(debug=True, port=5000)
