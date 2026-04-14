# aikeiba

競馬AIの当日予測結果を、静的サイトとして確認するMVPダッシュボードです。

## 実装済みMVP

- トップページ（買い/見送り一覧）
- レース詳細ページ（馬テーブル・ハイライト）
- ワイド候補ページ（EV/推奨度ソート、最低オッズ、高EVフィルタ）
- 見送りレースページ（見送り理由の可視化）
- JSON読み込みのみで動作（サーバーサイド処理なし）

## ローカル起動

ブラウザの `file://` では `fetch` 制限があるため、ローカルサーバーで開いてください。

```powershell
cd C:\Users\HND2205\Documents\git\aikeiba
python -m http.server 8000
```

ブラウザで `http://localhost:8000` を開くと表示されます。

## データ配置

`data/` 配下のJSONを、Pythonパイプラインの出力で毎日差し替える運用を想定しています。

- `data/races_today.json`
- `data/horse_predictions.json`
- `data/today_pipeline_bets.json`
- `data/race_summary.json`

## デプロイ

静的ファイルのみで構成されているため、そのまま GitHub Pages / Netlify / Cloudflare Pages に配置できます。
