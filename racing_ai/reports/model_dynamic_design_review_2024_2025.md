# Model Dynamic Design Review (2024 vs 2025)

generated_at: 2026-05-06

Scope:
- Production rule selection unchanged (`pair_selected_flag` semantics unchanged)
- `model_dynamic` is shadow-only
- This review focuses on why `model_dynamic_non_overlap` is not consistently better than rule in 2024/2025, and what to change **as shadow experiments**.

## 1) pair_model_score の分布が狭すぎる原因

### Observation

From `pair_model_score_distribution_review_2024_2025.csv`:

- 2024:
  - score_std ≈ 0.000397
  - score_p50 ≈ 0.045886, score_p90 ≈ 0.045886, score_max ≈ 0.046896
  - レース内 gap もほぼ 0（後述）
- 2025:
  - score_std ≈ 0.088276
  - score_p50 ≈ 0.0956, score_p90 ≈ 0.2081, score_max ≈ 0.7064

### Root cause (2024)

2024 は「モデルが弱い」のではなく、**推論入力特徴量の欠落が大きい**ためにスコアが潰れています。

2024-01-06 の expanded 候補（評価補助経路）で、pair_reranker meta の features を照合すると:

- meta features: 20
- 推論DFに存在しない features: 11
  - `field_size`, `distance`, `venue`, `surface` などレースメタ系
  - `*_rank_pct`, `*_z_in_race` などレース内相対特徴量
  - bucket系
- さらに `pair_ai_market_gap_min/max` が全NULL（=情報ゼロ）

この結果、評価補助経路では欠損特徴量を 0.0 埋めして推論しているため、
**モデル入力がほぼ同一**になり、pair_model_score がほぼ一定に収束します。

結論:
- 2024 でのスコア分布の狭さは **モデルの限界ではなく、評価補助経路の特徴量欠落**が主因。

## 2) model_dynamic の gap gate が厳しすぎる/意味を持たない可能性

### Observation

2024 の `pair_model_score_gap_to_next` は分布がほぼ 0:

- gap_p90 = 0
- gap_p99 ≈ 0.001

この状態で `min_gap=0.01` を要求すると、ほぼ全レースで gate を満たさず、選定が成立しません。

2025 では gap が機能している（gap_p90 ≈ 0.015）が、
`DYNAMIC_SKIP_GAP_SMALL` が race-level の主要スキップ要因になっており、
「gap gate がレースを買わない理由として強く働き過ぎる」可能性が高いです。

結論:
- gap gate は **2024では無意味（スコアが潰れているため）**
- 2025では **厳しすぎて候補数を削り過ぎている**疑いが強い

## 3) pair_reranker の目的変数がROIではなく的中寄りになっていないか

現状:
- pair_reranker の target は `actual_wide_hit`（的中）であり、ROIを直接最適化していない。
- 「的中確率の良い人気サイド」を上げやすく、**rule が既に強い領域をなぞりやすい**。

推測ではなく設計観点として:
- ROIを上げたい場合、目的関数が「的中」だけだと限界が出やすい。
- さらに市場情報（odds）を特徴量に入れると、的中寄りに強く寄ると “妙味” が減ることがある。

## 4) ROI寄り再学習案（市場確率/ワイドオッズ/payout proxy）

別紙: `pair_reranker_retraining_plan_roi_oriented.md`

## 5) candidate pool を広げた場合の効果（rule非重複候補の質）

別紙: `candidate_pool_expansion_experiment_plan.md`

## 結論（次に何を優先するか）

### 2023へ進むべきか？
現行のまま2023へ進むと、2024と同様に **評価補助経路の特徴量欠落が残る限り**、
pair_model_score が潰れて “gap/edge gate” が意味を失う可能性があります。

### 先に何を直すべきか（推奨）

1. **評価補助経路の特徴量整備（2024で欠けている11特徴量 + ai_gap_min/max）**
   - これにより 2024/2023 のスコア分布が復元できる可能性が高い
2. その上で、shadow実験として:
   - gap gate の再設計（「トップ差」ではなく「上位群の分離」など）
   - ROI寄り再学習（目的関数/重み付けの変更）
3. その後に 2023へ拡張（データ年次での頑健性確認）

