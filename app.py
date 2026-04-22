"""
日本株スクリーニング Web アプリ — 1〜5日間の累積下落を一括検知
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
    seen = set()
    for _, row in stocks.iterrows():
        code = str(row["code"]).strip()
        if not code:
            continue
        ticker = f"{code}.T"
        if ticker in seen:
            continue
        seen.add(ticker)
        tickers.append(ticker)
        _jpx_names[ticker] = str(row["name"]).strip()

    return tickers


def get_ticker_name(ticker: str) -> str:
    if ticker in _jpx_names and _jpx_names[ticker] != "-":
        return _jpx_names[ticker]
    try:
        info = yf.Ticker(ticker).info
        return info.get("shortName") or info.get("longName") or ticker
    except Exception:
        return ticker


# ── 設定パース ────────────────────────────────────
_DEFAULT_DAYS_CONFIG = {
    "1": {"enabled": True, "threshold": -3.0, "wick_pct": 4.0},
    "2": {"enabled": True, "threshold": -5.0, "wick_pct": 4.0},
    "3": {"enabled": True, "threshold": -7.0, "wick_pct": 4.0},
    "4": {"enabled": True, "threshold": -9.0, "wick_pct": 4.0},
    "5": {"enabled": True, "threshold": -11.0, "wick_pct": 4.0},
}


def _parse_settings(raw: dict | None) -> dict:
    if not raw or "days_config" not in raw:
        return {"days_config": {k: dict(v) for k, v in _DEFAULT_DAYS_CONFIG.items()}}

    days_config = {}
    raw_dc = raw["days_config"]
    for d in ("1", "2", "3", "4", "5"):
        default = _DEFAULT_DAYS_CONFIG[d]
        dc = raw_dc.get(d, raw_dc.get(int(d), {}))
        if dc:
            days_config[d] = {
                "enabled": bool(dc.get("enabled", True)),
                "threshold": float(dc.get("threshold", default["threshold"])),
                "wick_pct": float(dc.get("wick_pct", default["wick_pct"])),
            }
        else:
            days_config[d] = dict(default)

    return {"days_config": days_config}


# ── 1銘柄のスクリーニング (1〜5営業日を一括計算) ─
def screen_worker(ticker: str, settings: dict) -> list[dict]:
    """1銘柄につき1〜5営業日の累積下落を一度に計算する。
    データ取得は1回だけ。条件を満たした日数分の結果をリストで返す。

    yfinance の日足データは営業日のみ（土日祝は自動除外）なので
    iloc インデックスがそのまま営業日ベースのオフセットになる:
      iloc[-1] = 最新営業日（今日）
      iloc[-2] = 1営業日前
      iloc[-3] = 2営業日前
      iloc[-4] = 3営業日前
      iloc[-5] = 4営業日前
      iloc[-6] = 5営業日前
    """
    try:
        tk = yf.Ticker(ticker)

        try:
            mcap = tk.fast_info.get("marketCap", 0) or 0
        except Exception:
            mcap = 0
        if mcap < MIN_MARKET_CAP:
            return []

        end = datetime.now()
        start = end - timedelta(days=LOOKBACK_DAYS)
        df = tk.history(start=start, end=end, auto_adjust=True)
        if df.empty:
            return []
        df = df.dropna(subset=["Close"])
        # 最低7行必要: iloc[-1](今日) 〜 iloc[-6](5営業日前) + 余裕
        if len(df) < 7:
            return []

        days_config = settings["days_config"]
        # iloc[-1] = 最新営業日の終値
        current = float(df["Close"].iloc[-1])
        name = None  # 必要になるまで取得しない

        results = []
        for n_days in range(1, 6):
            cfg = days_config.get(str(n_days), {})
            if not cfg.get("enabled", False):
                continue

            threshold = cfg["threshold"]
            wick_tol = cfg["wick_pct"]

            # n営業日前の終値: iloc[-(n_days+1)]
            # 例: 1営業日前 → iloc[-2], 5営業日前 → iloc[-6]
            ref_close = float(df["Close"].iloc[-(n_days + 1)])
            cum_change = (current - ref_close) / ref_close * 100

            if cum_change > threshold:
                continue

            # 期間中の各営業日の上髭を計算
            # n_days=3 の場合: iloc[-3], iloc[-2], iloc[-1] の3営業日分
            max_wick = 0.0
            for i in range(n_days):
                d_idx = -(n_days - i)  # 古い営業日 → 新しい営業日の順
                o = float(df["Open"].iloc[d_idx])
                h = float(df["High"].iloc[d_idx])
                l = float(df["Low"].iloc[d_idx])
                c = float(df["Close"].iloc[d_idx])
                upper_wick = h - max(o, c)
                day_range = h - l
                wick = (upper_wick / day_range * 100) if day_range > 0 else 0.0
                if wick > max_wick:
                    max_wick = wick

            if max_wick > wick_tol:
                continue

            if name is None:
                name = get_ticker_name(ticker)

            results.append({
                "ticker": ticker,
                "name": name,
                "price": round(current, 1),
                "ref_close": round(ref_close, 1),
                "cum_change": round(cum_change, 2),
                "max_wick": round(max_wick, 1),
                "days": n_days,
            })

        return results

    except Exception:
        return []


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
        enabled = [d for d in range(1, 6)
                   if settings["days_config"].get(str(d), {}).get("enabled")]
        logging.info(
            "スクリーニング開始: %d 銘柄 (有効日数=%s)",
            len(tickers), enabled,
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

    # 重複排除: (ticker, days) の組み合わせで管理
    seen_results = set()
    for r in prev_results:
        seen_results.add((r["ticker"], r["days"]))

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(screen_worker, t, settings): t for t in remaining}

        count = 0
        for future in as_completed(futures):
            ticker = futures[future]
            result_list = future.result()
            _increment_processed(job_id)
            processed_list.append(ticker)
            for result in result_list:
                key = (result["ticker"], result["days"])
                if key in seen_results:
                    continue
                seen_results.add(key)
                _append_result(job_id, result)
                results_list.append(result)
            count += 1
            if count % 20 == 0:
                _flush_job(job_id)
                _save_progress(settings, tickers, processed_list, results_list)

    _delete_progress()

    job = _get_job(job_id)
    hits = job["hits"] if job else 0
    logging.info("スクリーニング完了: %d 銘柄処理 / %d 件検知", total, hits)
    _update_job(job_id, status="done", message="完了")


# ── チャート生成 ───────────────────────────────────
def build_chart_json(ticker: str, interval: str = "5m") -> str | None:
    tk = yf.Ticker(ticker)

    if interval == "1d":
        df = tk.history(period="6mo", interval="1d", auto_adjust=True)
        ma_windows = [25, 75]
        ma_colors = ["#f39c12", "#9b59b6"]
        time_fmt = "%m/%d"
    else:
        df = tk.history(period="1d", interval="5m", auto_adjust=True)
        if df.empty:
            df = tk.history(period="5d", interval="5m", auto_adjust=True)
            if df.empty:
                return None
            last_date = df.index[-1].date()
            df = df[df.index.date == last_date]
            if df.empty:
                return None
        ma_windows = [5, 25]
        ma_colors = ["#f39c12", "#9b59b6"]
        time_fmt = "%H:%M"

    df = df[["Open", "High", "Low", "Close", "Volume"]].dropna()
    if len(df) < 2:
        return None

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

    for w, c in zip(ma_windows, ma_colors):
        ma_len = min(w, len(df))
        ma = df["Close"].rolling(window=ma_len).mean()
        fig.add_trace(go.Scatter(
            x=df.index, y=ma,
            line=dict(color=c, width=1.5),
            name=f"MA{w}",
        ), row=1, col=1)

    fig.add_trace(go.Bar(
        x=df.index, y=df["Volume"],
        marker_color=colors,
        name="出来高",
        showlegend=False,
    ), row=2, col=1)

    fig.update_layout(
        template="plotly_dark",
        height=560,
        margin=dict(l=50, r=30, t=30, b=30),
        xaxis_rangeslider_visible=False,
        showlegend=True,
        legend=dict(orientation="h", y=1.02, x=0.5, xanchor="center"),
    )
    fig.update_xaxes(type="category", nticks=10, row=1, col=1)
    fig.update_xaxes(type="category", nticks=10, row=2, col=1)
    fig.update_yaxes(title_text="価格", row=1, col=1)
    fig.update_yaxes(title_text="出来高", row=2, col=1)

    tickvals = list(range(0, len(df), max(1, len(df) // 10)))
    ticktext = [df.index[i].strftime(time_fmt) for i in tickvals]
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
    interval = request.args.get("interval", "5m")
    if interval not in ("5m", "1d"):
        interval = "5m"
    chart_json = build_chart_json(ticker, interval)
    if chart_json is None:
        return jsonify({"ok": False, "error": "チャートデータを取得できませんでした"})
    return jsonify({"ok": True, "chart": json.loads(chart_json)})


if __name__ == "__main__":
    app.run(debug=True, port=5000)
