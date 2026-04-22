# Stock Screener - 日本株スクリーニング Web アプリ

Flask + yfinance + Plotly で構築した日本株スクリーニングツールです。

## 機能

- 監視銘柄リストの管理（ブラウザ上で編集・保存）
- ボタン一つでスクリーニング実行
- 条件に該当した銘柄を色分け表示
- 銘柄クリックで 5 分足ローソク足チャート（出来高 + MA25）を表示

## スクリーニング条件

| # | 条件 | しきい値 |
|---|------|----------|
| 1 | 前日比の騰落率 | ±3% 以上 |
| 2 | 年初来高値/安値圏 | 高値・安値から ±5% 以内 |
| 3 | 25 日移動平均乖離 | ±5% 以上 |
| 4 | 統計的異常値 | 過去 30 日の標準偏差 ×2 倍以上の変動 |

## ローカルで動かす

```bash
# 1. リポジトリをクローン
git clone https://github.com/<あなたのユーザー名>/stock-screener-web.git
cd stock-screener-web

# 2. 依存ライブラリをインストール
pip install -r requirements.txt

# 3. 起動
python app.py
```

ブラウザで http://localhost:5000 を開いてください。

## GitHub へのアップロード手順

```bash
# 1. stock-screener-web フォルダに移動
cd C:\stock-screener-web

# 2. Git リポジトリを初期化
git init
git add .
git commit -m "Initial commit"

# 3. GitHub でリポジトリを作成後、リモートを追加してプッシュ
git remote add origin https://github.com/<あなたのユーザー名>/stock-screener-web.git
git branch -M main
git push -u origin main
```

## Render.com へのデプロイ手順

### 1. Render アカウント作成
https://render.com にアクセスし、GitHub アカウントで登録します。

### 2. 新しい Web Service を作成
1. ダッシュボードで **「New +」** → **「Web Service」** をクリック
2. **「Build and deploy from a Git repository」** を選択
3. GitHub リポジトリ `stock-screener-web` を選択

### 3. 設定項目
| 項目 | 値 |
|------|----|
| Name | `stock-screener`（任意） |
| Region | `Oregon` または `Singapore` |
| Branch | `main` |
| Runtime | `Python 3` |
| Build Command | `pip install -r requirements.txt` |
| Start Command | `gunicorn app:app` |
| Instance Type | `Free` |

### 4. デプロイ
**「Create Web Service」** をクリックすると自動でビルド・デプロイが始まります。
完了すると `https://stock-screener-xxxx.onrender.com` のような URL が発行されます。

### 5. 環境変数の設定（PTS理由調査機能）

PTS大幅変動銘柄の「理由を調べる」機能には Anthropic API キーが必要です。

1. Render ダッシュボードでサービスを選択
2. **「Environment」** タブを開く
3. **「Add Environment Variable」** で以下を追加：

| Key | Value |
|-----|-------|
| `ANTHROPIC_API_KEY` | `sk-ant-...`（Anthropic コンソールで取得したAPIキー） |

APIキーは https://console.anthropic.com で取得できます。

ローカル環境では以下のように設定してください：

```bash
# Windows
set ANTHROPIC_API_KEY=sk-ant-...

# Mac/Linux
export ANTHROPIC_API_KEY=sk-ant-...
```

### 注意点
- Free プランではアクセスがないと 15 分でスリープします（次回アクセス時に約 30 秒の起動時間がかかります）
- `stocks.txt` はサーバーのファイルシステムに保存されるため、再デプロイ時にリセットされます。永続化が必要な場合はデータベースの利用を検討してください
