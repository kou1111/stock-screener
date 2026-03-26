"""
日本株スクリーニング Web アプリ
Flask + yfinance + Plotly
"""

import json
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import plotly
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import yfinance as yf
from flask import Flask, jsonify, render_template, request

app = Flask(__name__)

# ── 設定 ───────────────────────────────────────────
STOCKS_FILE = Path(__file__).parent / "stocks.txt"
DEFAULT_TICKERS = "7203.T\n6758.T\n9984.T\n8306.T\n6861.T\n4063.T\n9433.T\n6501.T"

DAILY_CHANGE_THRESHOLD = 3.0
NEAR_EXTREME_PCT = 5.0
MA_PERIOD = 25
MA_DEVIATION_THRESHOLD = 5.0
ANOMALY_SIGMA = 2.0
LOOKBACK_DAYS = 60


# ── 銘柄リスト管理 ─────────────────────────────────
def load_tickers() -> list[str]:
    if not STOCKS_FILE.exists():
        STOCKS_FILE.write_text(DEFAULT_TICKERS, encoding="utf-8")
    lines = STOCKS_FILE.read_text(encoding="utf-8").splitlines()
    return [l.strip() for l in lines if l.strip() and not l.strip().startswith("#")]


def save_tickers(text: str) -> list[str]:
    STOCKS_FILE.write_text(text.strip(), encoding="utf-8")
    return load_tickers()


# ── データ取得 ─────────────────────────────────────
def fetch_data(tickers: list[str]) -> dict:
    end = datetime.now()
    start = end - timedelta(days=LOOKBACK_DAYS)
    data = {}
    raw = yf.download(
        tickers,
        start=start.strftime("%Y-%m-%d"),
        end=end.strftime("%Y-%m-%d"),
        progress=False,
        group_by="ticker",
        auto_adjust=True,
    )
    for ticker in tickers:
        try:
            df = raw.copy() if len(tickers) == 1 else raw[ticker].copy()
            df = df.dropna(subset=["Close"])
            if len(df) >= 2:
                data[ticker] = df
        except (KeyError, TypeError):
            continue
    return data


def get_ticker_name(ticker: str) -> str:
    try:
        info = yf.Ticker(ticker).info
        return info.get("longName") or info.get("shortName") or ticker
    except Exception:
        return ticker


# ── スクリーニング ─────────────────────────────────
def screen_one(ticker: str, df) -> dict | None:
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
        alerts.append({"text": f"{tag} {change_pct:+.1f}%", "type": "up" if change_pct > 0 else "down"})

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

    name = get_ticker_name(ticker)

    return {
        "ticker": ticker,
        "name": name,
        "price": current,
        "change_pct": round(change_pct, 2),
        "alerts": alerts,
    }


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

    # MA25
    ma_len = min(25, len(df))
    ma = df["Close"].rolling(window=ma_len).mean()

    # 出来高の色
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

    # ローソク足
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

    # MA25
    fig.add_trace(go.Scatter(
        x=df.index, y=ma,
        line=dict(color="#f39c12", width=1.5),
        name=f"MA{ma_len}",
    ), row=1, col=1)

    # 出来高
    fig.add_trace(go.Bar(
        x=df.index, y=df["Volume"],
        marker_color=colors,
        name="出来高",
        showlegend=False,
    ), row=2, col=1)

    fig.update_layout(
        title=dict(text=f"{ticker}  {name}  ({chart_date})", font=dict(size=16)),
        template="plotly_dark",
        height=560,
        margin=dict(l=50, r=30, t=60, b=30),
        xaxis_rangeslider_visible=False,
        showlegend=True,
        legend=dict(orientation="h", y=1.02, x=0.5, xanchor="center"),
    )
    fig.update_xaxes(type="category", nticks=10, row=1, col=1)
    fig.update_xaxes(type="category", nticks=10, row=2, col=1)
    fig.update_yaxes(title_text="価格", row=1, col=1)
    fig.update_yaxes(title_text="出来高", row=2, col=1)

    # X軸ラベルを時刻のみに
    tickvals = list(range(0, len(df), max(1, len(df) // 10)))
    ticktext = [df.index[i].strftime("%H:%M") for i in tickvals]
    fig.update_xaxes(tickvals=tickvals, ticktext=ticktext, row=2, col=1)
    fig.update_xaxes(tickvals=tickvals, ticktext=ticktext, row=1, col=1)

    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)


# ── ルーティング ───────────────────────────────────
@app.route("/")
def index():
    tickers_text = "\n".join(load_tickers())
    return render_template("index.html", tickers_text=tickers_text)


@app.route("/api/save_tickers", methods=["POST"])
def api_save_tickers():
    text = request.json.get("tickers", "")
    tickers = save_tickers(text)
    return jsonify({"ok": True, "count": len(tickers)})


@app.route("/api/screen", methods=["POST"])
def api_screen():
    tickers = load_tickers()
    if not tickers:
        return jsonify({"ok": False, "error": "銘柄が登録されていません"})

    data = fetch_data(tickers)
    results = []
    for ticker, df in data.items():
        r = screen_one(ticker, df)
        if r:
            results.append(r)

    results.sort(key=lambda r: abs(r["change_pct"]), reverse=True)
    return jsonify({"ok": True, "results": results, "total": len(tickers), "fetched": len(data)})


@app.route("/api/chart/<ticker>")
def api_chart(ticker):
    chart_json = build_chart_json(ticker)
    if chart_json is None:
        return jsonify({"ok": False, "error": "チャートデータを取得できませんでした"})
    return jsonify({"ok": True, "chart": json.loads(chart_json)})


if __name__ == "__main__":
    app.run(debug=True, port=5000)
