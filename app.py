"""
日本株スクリーニング Web アプリ
Flask + yfinance + Plotly
JPX上場銘柄一覧から自動取得 → 2段階スクリーニング
"""

import io
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import plotly
import plotly.graph_objects as go
import requests as http_requests
from plotly.subplots import make_subplots
import yfinance as yf
from flask import Flask, Response, jsonify, render_template

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

# 一方的下落の設定
SELLOFF_VOLATILITY_MULT = 1.5
SELLOFF_BOUNCE_RATIO = 0.30
SELLOFF_TAIL_BARS = 5

# JPX 銘柄名キャッシュ (code -> name)
_jpx_names: dict[str, str] = {}


# ── JPX 銘柄一覧取得 ──────────────────────────────
def fetch_jpx_tickers() -> list[str]:
    """JPX上場銘柄Excelをダウンロードし、株式銘柄コード一覧を返す"""
    global _jpx_names
    r = http_requests.get(JPX_URL, timeout=30)
    r.raise_for_status()

    df = pd.read_excel(io.BytesIO(r.content), engine="xlrd")
    # 位置ベースでカラム割り当て (エンコーディング問題を回避)
    df.columns = [
        "date", "code", "name", "market",
        "sec33_code", "sec33", "sec17_code", "sec17",
        "scale_code", "scale",
    ]

    # ETF / REIT / PRO Market / 出資証券 を除外
    stocks = df[~df["market"].str.contains("ETF|REIT|PRO", na=False)].copy()

    # コードを文字列に統一し .T を付与
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
    """キャッシュ済みJPX名 → yfinance の順で名前を取得"""
    if ticker in _jpx_names and _jpx_names[ticker] != "-":
        return _jpx_names[ticker]
    try:
        info = yf.Ticker(ticker).info
        return info.get("longName") or info.get("shortName") or ticker
    except Exception:
        return ticker


# ── ステップ① 1銘柄の日足スクリーニング ────────────
def screen_worker(ticker: str) -> dict | None:
    """1銘柄を処理: 時価総額フィルタ → 日足データ取得 → 条件判定"""
    try:
        tk = yf.Ticker(ticker)

        # 時価総額チェック
        try:
            mcap = tk.fast_info.get("marketCap", 0)
        except Exception:
            mcap = 0
        if not mcap or mcap < MIN_MARKET_CAP:
            return None

        # 日足データ取得
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

        # 1) 前日比
        if abs(change_pct) >= DAILY_CHANGE_THRESHOLD:
            tag = "急騰" if change_pct > 0 else "急落"
            alerts.append({
                "text": f"{tag} {change_pct:+.1f}%",
                "type": "up" if change_pct > 0 else "down",
            })

        # 2) 年初来高値/安値圏
        if ytd_high > 0:
            d = (ytd_high - current) / ytd_high * 100
            if d <= NEAR_EXTREME_PCT:
                alerts.append({"text": f"高値圏 (高値比 -{d:.1f}%)", "type": "up"})
        if ytd_low > 0:
            d = (current - ytd_low) / ytd_low * 100
            if d <= NEAR_EXTREME_PCT:
                alerts.append({"text": f"安値圏 (安値比 +{d:.1f}%)", "type": "down"})

        # 3) MA乖離
        if abs(ma_dev) >= MA_DEVIATION_THRESHOLD:
            alerts.append({
                "text": f"MA{MA_PERIOD}乖離 {ma_dev:+.1f}%",
                "type": "up" if ma_dev > 0 else "down",
            })

        # 4) 統計的異常値
        if std_30d > 0 and abs(change_pct) >= std_30d * ANOMALY_SIGMA:
            alerts.append({
                "text": f"統計異常 ({change_pct:+.1f}% / 2σ={std_30d * ANOMALY_SIGMA:.1f}%)",
                "type": "up" if change_pct > 0 else "down",
            })

        if not alerts:
            return None

        mcap_b = mcap / 1e8  # 億円
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


# ── 一方的下落の検知 ──────────────────────────────
def detect_selloff(ticker: str) -> dict | None:
    """5分足データで一方的な下落を検知。"""
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

    # 一方的下落の区間ハイライト
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


@app.route("/api/screen")
def api_screen():
    """SSE ストリーム: JPX銘柄を並列スクリーニングし、進捗と結果を逐次配信"""

    def generate():
        # ステップ0: JPX銘柄一覧取得
        yield _sse({"type": "status", "message": "JPX銘柄一覧を取得中..."})
        try:
            tickers = fetch_jpx_tickers()
        except Exception as e:
            yield _sse({"type": "error", "message": f"JPX一覧の取得に失敗: {e}"})
            return

        total = len(tickers)
        yield _sse({"type": "start", "total": total})

        # ステップ①: 並列スクリーニング
        processed = 0
        hits = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(screen_worker, t): t for t in tickers}

            for future in as_completed(futures):
                processed += 1
                result = future.result()

                if result:
                    hits += 1
                    yield _sse({
                        "type": "hit",
                        "result": result,
                        "processed": processed,
                        "total": total,
                        "hits": hits,
                    })
                elif processed % 20 == 0 or processed == total:
                    yield _sse({
                        "type": "progress",
                        "processed": processed,
                        "total": total,
                        "hits": hits,
                    })

        yield _sse({"type": "done", "processed": total, "hits": hits})

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/api/chart/<ticker>")
def api_chart(ticker):
    chart_json = build_chart_json(ticker)
    if chart_json is None:
        return jsonify({"ok": False, "error": "チャートデータを取得できませんでした"})
    return jsonify({"ok": True, "chart": json.loads(chart_json)})


def _sse(data: dict) -> str:
    return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"


if __name__ == "__main__":
    app.run(debug=True, port=5000)
