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
import os
import re
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
from bs4 import BeautifulSoup
from plotly.subplots import make_subplots
import yfinance as yf
from flask import Flask, jsonify, render_template, request

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ── 設定 ───────────────────────────────────────────
JPX_URL = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
MIN_MARKET_CAP = 7_000_000_000  # 70億円
MIN_VOLUME = 3_000  # 出来高フィルタ
MAX_WORKERS = 50
LOOKBACK_DAYS = 60
REASON_TIMEOUT = 60  # 理由調査のタイムアウト(秒)

# 理由調査用スレッドプール（スキャンと分離）
_reason_pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="reason")

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
    df = df.drop_duplicates(subset=["code"], keep="first")
    stocks = df[~df["market"].str.contains("ETF|REIT|PRO", na=False)].copy()
    after_etf = len(stocks)
    stocks = stocks[stocks["market"].str.contains("プライム", na=False)]
    stocks = stocks.drop_duplicates(subset=["code"], keep="first")
    after_prime = len(stocks)

    logging.info(
        "JPXフィルタ: 全%d行 → ETF等除外 %d → プライム市場 %d 銘柄",
        total_rows, after_etf, after_prime,
    )

    # 全銘柄の日本語名をキャッシュ（PTS等で他市場の銘柄名も必要なため）
    for _, row in df.iterrows():
        code = str(row["code"]).strip()
        if code:
            _jpx_names[f"{code}.T"] = str(row["name"]).strip()

    # スクリーニング対象はプライム市場のみ
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

        # 当日出来高が3000株以下なら除外
        today_volume = float(df["Volume"].iloc[-1]) if "Volume" in df.columns else 0
        if today_volume <= MIN_VOLUME:
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

            turnover = round(today_volume * current / 100_000_000, 1)
            spark = _spark_ohlc(df)
            results.append({
                "ticker": ticker,
                "name": name,
                "price": round(current, 1),
                "ref_close": round(ref_close, 1),
                "cum_change": round(cum_change, 2),
                "max_wick": round(max_wick, 1),
                "days": n_days,
                "turnover": turnover,
                "market_cap": mcap,
                "spark": spark,
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

    # 重複���除: 日数ごとに seen set を管理
    seen_by_day = {d: set() for d in range(1, 6)}
    for r in prev_results:
        seen_by_day[r["days"]].add(r["ticker"])

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(screen_worker, t, settings): t for t in remaining}

        count = 0
        for future in as_completed(futures):
            ticker = futures[future]
            result_list = future.result()
            _increment_processed(job_id)
            processed_list.append(ticker)
            for result in result_list:
                d = result["days"]
                sym = result["ticker"]
                if sym in seen_by_day[d]:
                    continue
                seen_by_day[d].add(sym)
                _append_result(job_id, result)
                results_list.append(result)
            count += 1
            if count % 20 == 0:
                _flush_job(job_id)
                _save_progress(settings, tickers, processed_list, results_list)

    _delete_progress()

    # 最終重複排除: (ticker, days) の組で一意にする
    with _jobs_lock:
        if job_id in _jobs_mem:
            dedup = {}
            for r in _jobs_mem[job_id]["results"]:
                key = (r["ticker"], r["days"])
                if key not in dedup:
                    dedup[key] = r
            _jobs_mem[job_id]["results"] = list(dedup.values())
            _jobs_mem[job_id]["hits"] = len(dedup)

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


# ── PTS銘柄取得 ───────────────────────────────────
PTS_URLS = {
    "up": "https://s.kabutan.jp/warnings/pts_night_price_increase/",
    "down": "https://s.kabutan.jp/warnings/pts_night_price_decrease/",
}
PTS_THRESHOLD = 3.0  # ±3%


def _spark_ohlc(df, n=20) -> list[list]:
    """直近n日分のOHLCをリストで返す: [[o,h,l,c], ...]"""
    try:
        tail = df.tail(n)
        cols = ["Open", "High", "Low", "Close"]
        if not all(c in tail.columns for c in cols):
            return []
        return tail[cols].round(1).values.tolist()
    except Exception:
        return []


def _parse_number(text: str) -> float:
    """カンマ付き数値文字列をfloatに変換"""
    return float(text.replace(",", "").strip())


def _scrape_kabutan_pts(url: str) -> list[dict]:
    """株探のPTSランキングページをスクレイピング

    テーブル構造 (2025年〜):
      th[0]: "銘柄名 コード 市場" (sticky cell)
      td[1]: 通常終値
      td[2]: PTS価格
      td[3]: "変動額 変動率%"
      td[4]: 出来高
      td[5-7]: PER, PBR, 利回り
    """
    results = []
    try:
        resp = http_requests.get(url, timeout=15, headers={
            "User-Agent": "Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36"
        })
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        for tr in soup.select("table tr"):
            cells = tr.find_all(["td", "th"])
            if len(cells) < 4:
                continue

            # cells[0] (th): "銘柄名 コード 市場" (例: "岡野バ 6492 東S")
            first_text = cells[0].get_text(strip=True)
            code_match = re.search(r"(\d{4}[A-Z]?)", first_text)
            if not code_match:
                continue
            code = code_match.group(1)

            # cells[1]: 通常終値, cells[2]: PTS価格
            try:
                close_price = _parse_number(cells[1].get_text(strip=True))
                pts_price = _parse_number(cells[2].get_text(strip=True))
            except (ValueError, IndexError):
                continue

            # cells[3]: "変動額 変動率%"
            change_text = cells[3].get_text(" ", strip=True)
            pct_match = re.search(r"([+-]?\d+\.?\d*)\s*%", change_text)
            if not pct_match:
                continue
            change_pct = float(pct_match.group(1))

            change = 0.0
            change_amt = re.search(r"([+-][\d,]+\.?\d*)", change_text)
            if change_amt:
                try:
                    change = _parse_number(change_amt.group(1))
                except ValueError:
                    pass

            results.append({
                "code": code,
                "name": "",  # fetch_pts_stocks で JPX名に置換
                "close": close_price,
                "pts_price": pts_price,
                "change": change,
                "change_pct": change_pct,
            })

    except Exception as e:
        logging.warning("PTS取得エラー (%s): %s", url, e)

    return results


def fetch_pts_stocks(threshold: float = PTS_THRESHOLD) -> list[dict]:
    """PTS上昇・下落ランキングを取得して±threshold%以上の銘柄を返す"""
    # JPX銘柄名がまだ読み込まれていなければ先にロード
    if not _jpx_names:
        try:
            fetch_jpx_tickers()
        except Exception:
            pass

    all_stocks = []
    seen = set()

    for direction, url in PTS_URLS.items():
        stocks = _scrape_kabutan_pts(url)
        for s in stocks:
            if abs(s["change_pct"]) >= threshold and s["code"] not in seen:
                seen.add(s["code"])
                # 当日出来高が3000株以下なら除外
                ticker = f"{s['code']}.T"
                try:
                    tk_obj = yf.Ticker(ticker)
                    hist = tk_obj.history(period="1mo", interval="1d", auto_adjust=True)
                    vol = float(hist["Volume"].iloc[-1]) if not hist.empty else 0
                except Exception:
                    tk_obj = None
                    hist = None
                    vol = 0
                if vol <= MIN_VOLUME:
                    continue
                # 時価総額
                try:
                    mcap = tk_obj.fast_info.get("marketCap", 0) or 0 if tk_obj else 0
                except Exception:
                    mcap = 0
                # スパークライン
                spark = _spark_ohlc(hist) if hist is not None and not hist.empty else []
                # JPXキャッシュから日本語銘柄名を取得
                jpx_name = _jpx_names.get(ticker, "")
                s["name"] = jpx_name if jpx_name and jpx_name != "-" else "-"
                s["turnover"] = round(vol * s["pts_price"] / 100_000_000, 1)
                s["market_cap"] = mcap
                s["spark"] = spark
                all_stocks.append(s)

    all_stocks.sort(key=lambda x: abs(x["change_pct"]), reverse=True)
    return all_stocks


# ── 当日大幅変動銘柄 ─────────────────────────────────
def _check_intraday(ticker: str, threshold: float) -> dict | None:
    """1銘柄の当日始値→現在値の変動率をチェック"""
    try:
        tk = yf.Ticker(ticker)

        try:
            mcap = tk.fast_info.get("marketCap", 0) or 0
        except Exception:
            mcap = 0
        if mcap < MIN_MARKET_CAP:
            return None

        df = tk.history(period="1d", interval="5m")
        if df.empty:
            df = tk.history(period="5d", interval="5m")
            if df.empty:
                return None
            last_date = df.index[-1].date()
            df = df[df.index.date == last_date]
            if df.empty:
                return None

        open_price = float(df["Open"].iloc[0])
        current = float(df["Close"].iloc[-1])
        volume = float(df["Volume"].sum())

        if volume <= MIN_VOLUME or open_price <= 0:
            return None

        change_pct = (current - open_price) / open_price * 100

        if abs(change_pct) < threshold:
            return None

        name = _jpx_names.get(ticker, "")
        if not name or name == "-":
            name = get_ticker_name(ticker)

        turnover = round(volume * current / 100_000_000, 1)
        try:
            dfd = tk.history(period="1mo", interval="1d", auto_adjust=True)
            spark = _spark_ohlc(dfd) if not dfd.empty else []
        except Exception:
            spark = []
        return {
            "code": ticker.replace(".T", ""),
            "name": name,
            "open": round(open_price, 1),
            "current": round(current, 1),
            "change_pct": round(change_pct, 2),
            "volume": int(volume),
            "turnover": turnover,
            "market_cap": mcap,
            "spark": spark,
        }
    except Exception:
        return None


def _run_intraday(job_id: str, threshold: float):
    """バックグラウンドで当日大幅変動銘柄をスキャン"""
    try:
        tickers = fetch_jpx_tickers()
    except Exception as e:
        _update_job(job_id, status="error", message=f"JPX一覧の取得に失敗: {e}")
        return

    total = len(tickers)
    logging.info("当日変動スキャン開始: %d 銘柄 (閾値=±%.1f%%)", total, threshold)
    _update_job(job_id, status="running", total=total, processed=0,
                message="当日変動スキャン中...")

    seen_lock = threading.Lock()
    seen_symbols = set()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_check_intraday, t, threshold): t for t in tickers}
        count = 0
        for future in as_completed(futures):
            result = future.result()
            _increment_processed(job_id)
            if result:
                with seen_lock:
                    if result["code"] not in seen_symbols:
                        seen_symbols.add(result["code"])
                        _append_result(job_id, result)
            count += 1
            if count % 20 == 0:
                _flush_job(job_id)

    # 最終重複排除 + 変動率の絶対値降順でソート
    with _jobs_lock:
        if job_id in _jobs_mem:
            dedup = {}
            for r in _jobs_mem[job_id]["results"]:
                if r["code"] not in dedup:
                    dedup[r["code"]] = r
            deduped = list(dedup.values())
            deduped.sort(key=lambda x: abs(x["change_pct"]), reverse=True)
            _jobs_mem[job_id]["results"] = deduped
            _jobs_mem[job_id]["hits"] = len(deduped)

    job = _get_job(job_id)
    hits = job["hits"] if job else 0
    logging.info("当日変動スキャン完了: %d 銘柄処理 / %d 件検知", total, hits)
    _update_job(job_id, status="done", message="完了")


# ── 変動理由調査 ─────────────────────────────────────
def investigate_reason(code: str, name: str, job_id: str | None = None) -> str:
    """2段階で銘柄の変動理由を調査: ①gpt-4o-search-previewで情報収集 → ②o3で分析"""
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY が設定されていません")

    import openai
    client = openai.OpenAI(api_key=api_key)

    today = datetime.now().strftime("%Y年%m月%d日")

    # ── ステップ① gpt-4o-search-preview で情報収集 ──
    if job_id:
        _update_job(job_id, step="step1")
    logging.info("理由調査 ステップ①開始: %s (%s)", code, name)

    search_prompt = (
        f"{name} 株価 {today} ニュース 決算 適時開示\n\n"
        f"上記キーワードに関連する最新の情報をできるだけ詳しく取得してください。"
        f"銘柄コードは {code} です。"
        f"検索結果のテキストをそのまま出力してください。"
    )

    try:
        search_resp = client.chat.completions.create(
            model="gpt-4o-search-preview",
            messages=[{"role": "user", "content": search_prompt}],
            timeout=REASON_TIMEOUT,
        )
    except Exception as e:
        logging.error("ステップ① API呼び出しエラー: %s — %s", type(e).__name__, e)
        raise

    search_result = search_resp.choices[0].message.content if search_resp.choices else ""
    logging.info("理由調査 ステップ①完了: %s (%d文字)", code, len(search_result))

    if not search_result.strip():
        search_result = "（検索結果なし）"

    # ── ステップ② o3 で深く分析 ──
    if job_id:
        _update_job(job_id, step="step2")
    logging.info("理由調査 ステップ②開始: %s (%s)", code, name)

    analysis_prompt = (
        f"以下は{name}（{code}）の本日{today}時点のニュース・開示情報です。\n\n"
        f"{search_result}\n\n"
        f"これを踏まえて以下を日本語で分析してください：\n\n"
        f"【変動の主な理由】\n"
        f"・本日出た材料か、何営業日前の材料かを明記すること\n"
        f"・各理由の先頭に【本日】【N営業日前】【N日前】【N週間前】【Nヶ月前】【1ヶ月以上前】【時期不明】のいずれかを必ず付けること\n"
        f"・明確な材料（決算・IR・ニュース等）がない場合は最初の行に「⚠️ 本日時点で明確な材料は確認できませんでした。」と書き、その後に考えられる背景要因を続けること\n\n"
        f"【背景にある構造的な要因】\n"
        f"業界動向や中長期的な要因を記載\n\n"
        f"【今後の注目材料】\n"
        f"今後のイベントや注目ポイントを記載"
    )

    try:
        analysis_resp = client.chat.completions.create(
            model="o3",
            messages=[{"role": "user", "content": analysis_prompt}],
            timeout=REASON_TIMEOUT * 2,
        )
    except Exception as e:
        logging.error("ステップ② API呼び出しエラー: %s — %s", type(e).__name__, e)
        raise

    result = analysis_resp.choices[0].message.content if analysis_resp.choices else "理由を特定できませんでした。"
    logging.info("理由調査 ステップ②完了: %s (%d文字)", code, len(result))
    return result


# ── ルーティング ───────────────────────────────────
@app.route("/")
def index():
    ga_id = os.environ.get("GOOGLE_ANALYTICS_ID", "")
    return render_template("index.html", ga_id=ga_id)


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


@app.route("/api/pts")
def api_pts():
    """PTS大幅変動銘柄を取得"""
    try:
        threshold = float(request.args.get("threshold", PTS_THRESHOLD))
        stocks = fetch_pts_stocks(threshold)
        return jsonify({"ok": True, "stocks": stocks})
    except Exception as e:
        logging.exception("PTS取得エラー")
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/intraday/start", methods=["POST"])
def api_intraday_start():
    """当日変動スキャンをバックグラウンドで開始"""
    try:
        raw = request.get_json(silent=True) or {}
        threshold = float(raw.get("threshold", 3.0))
        job_id = _new_job()
        t = threading.Thread(target=_run_intraday,
                             args=(job_id, threshold), daemon=True)
        t.start()
        return jsonify({"ok": True, "job_id": job_id})
    except Exception as e:
        logging.exception("当日変動スキャン開始エラー")
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/intraday/poll/<job_id>")
def api_intraday_poll(job_id):
    """当日変動スキャンの進捗と結果を返す"""
    try:
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
    except Exception as e:
        logging.exception("当日変動ポーリングエラー")
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/spark/<ticker>")
def api_spark(ticker):
    """注目銘柄用: 1銘柄のスパークラインOHLCを返す"""
    try:
        if not ticker.endswith(".T"):
            ticker = ticker + ".T"
        tk = yf.Ticker(ticker)
        hist = tk.history(period="1mo", interval="1d", auto_adjust=True)
        if hist is None or hist.empty:
            return jsonify({"ok": True, "spark": []})
        spark = _spark_ohlc(hist)
        try:
            mcap = tk.fast_info.get("marketCap", 0) or 0
        except Exception:
            mcap = 0
        return jsonify({"ok": True, "spark": spark, "market_cap": mcap})
    except Exception as e:
        logging.warning("Spark取得エラー (%s): %s", ticker, e)
        return jsonify({"ok": True, "spark": [], "market_cap": 0})


@app.route("/api/reason/start/<code>", methods=["POST"])
def api_reason_start(code):
    """理由調査をバックグラウンドで開始（専用スレッドプール使用）"""
    try:
        raw = request.get_json(silent=True) or {}
        name = raw.get("name", code)
        job_id = _new_job()

        def _run():
            try:
                reason = investigate_reason(code, name, job_id=job_id)
                _update_job(job_id, status="done", message=reason)
            except Exception as e:
                logging.exception("理由調査エラー: %s", code)
                _update_job(job_id, status="error",
                            message=f"調査中にエラーが発生しました: {e}")

        _reason_pool.submit(_run)
        return jsonify({"ok": True, "job_id": job_id})
    except Exception as e:
        logging.exception("理由調査開始エラー: %s", code)
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/reason/poll/<job_id>")
def api_reason_poll(job_id):
    """理由調査の進捗を返す"""
    try:
        job = _get_job(job_id)
        if job is None:
            return jsonify({"ok": False, "error": "ジョブが見つかりません"}), 404
        return jsonify({
            "ok": True,
            "status": job["status"],
            "message": job.get("message", ""),
            "step": job.get("step", ""),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


if __name__ == "__main__":
    app.run(debug=True, port=5000)
