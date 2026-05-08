const DATA_FILES = {
  races: "./data/races_today.json",
  horses: "./data/horse_predictions.json",
  bets: "./data/today_pipeline_bets.json",
  summary: "./data/race_summary.json"
};

const COMPARISON_VIEW_PATH = "./data/comparison_view.json";

const state = {
  route: "top",
  venue: "all",
  buyFilter: "all",
  raceSort: "recommendation",
  wideSort: "expected_value",
  wideMinOdds: 0,
  wideHighEvOnly: false,
  selectedRaceId: null
};

const THEME_KEY = "aikeiba_theme";

const isNum = (value) => typeof value === "number" && Number.isFinite(value);
const formatPct = (value) => (isNum(value) ? `${Math.round(value * 100)}%` : "-");
const formatSigned = (value) => (isNum(value) ? `${value > 0 ? "+" : ""}${value.toFixed(2)}` : "-");
const formatFixed = (value, digits = 2) => (isNum(value) ? value.toFixed(digits) : "-");
const formatYesNo = (value) => (value ? "Yes" : "No");

function statusBadge(status) {
  if (status === "mismatch") return `<span class="pill status danger">mismatch</span>`;
  if (status === "ok_with_missing_calibration") return `<span class="pill status warning">missing calibration</span>`;
  return `<span class="pill status ok">ok</span>`;
}

const app = document.getElementById("app");
const filterBar = document.getElementById("globalFilters");
const todayLabel = document.getElementById("todayLabel");
const themeToggle = document.getElementById("themeToggle");
todayLabel.textContent = `本日: ${new Date().toLocaleDateString("ja-JP")}`;

let store = null;

function getTheme() {
  const saved = localStorage.getItem(THEME_KEY);
  return saved === "light" ? "light" : "dark";
}

function applyTheme(theme) {
  document.body.classList.toggle("light", theme === "light");
  if (themeToggle) themeToggle.textContent = theme === "light" ? "🌙" : "☀";
}

function initTheme() {
  applyTheme(getTheme());
  if (themeToggle) {
    themeToggle.onclick = () => {
      const nextTheme = getTheme() === "light" ? "dark" : "light";
      localStorage.setItem(THEME_KEY, nextTheme);
      applyTheme(nextTheme);
    };
  }
}

async function fetchJson(path) {
  const response = await fetch(path);
  if (!response.ok) {
    throw new Error(`${path} の読み込みに失敗しました`);
  }
  return response.json();
}

async function loadComparisonView() {
  try {
    const response = await fetch(COMPARISON_VIEW_PATH);
    if (!response.ok) return null;
    return await response.json();
  } catch (_error) {
    return null;
  }
}

async function loadData() {
  const [races, horses, bets, summary] = await Promise.all([
    fetchJson(DATA_FILES.races),
    fetchJson(DATA_FILES.horses),
    fetchJson(DATA_FILES.bets),
    fetchJson(DATA_FILES.summary)
  ]);
  const comparisonView = await loadComparisonView();
  return { races, horses, bets, summary, comparisonView };
}

function activeTab(route) {
  document.querySelectorAll(".tabs a").forEach((tab) => {
    tab.classList.toggle("active", tab.dataset.route === route);
  });
}

function parseRoute() {
  const hash = location.hash.replace("#", "");
  const [routeName, raceId] = hash.split("/");
  if (routeName === "race" && raceId) {
    state.route = "race";
    state.selectedRaceId = decodeURIComponent(raceId);
    return;
  }
  if (["top", "wide", "skip", "comparison"].includes(routeName)) {
    state.route = routeName;
    return;
  }
  state.route = "top";
}

function sortRows(rows, key, desc = true) {
  const sorted = [...rows].sort((left, right) => {
    const a = left?.[key];
    const b = right?.[key];
    if (a === b) return 0;
    if (a == null) return 1;
    if (b == null) return -1;
    return a > b ? 1 : -1;
  });
  return desc ? sorted.reverse() : sorted;
}

function renderFilters() {
  if (state.route === "comparison") {
    filterBar.innerHTML = "";
    return;
  }

  const venues = ["all", ...new Set(store.races.map((row) => row.venue))];
  let html = `
    <label>開催場
      <select id="venueSelect">
        ${venues.map((venue) => `<option value="${venue}" ${state.venue === venue ? "selected" : ""}>${venue === "all" ? "すべて" : venue}</option>`).join("")}
      </select>
    </label>
  `;

  if (state.route === "top") {
    html += `
      <label>買い/見送り
        <select id="buyFilter">
          <option value="all" ${state.buyFilter === "all" ? "selected" : ""}>すべて</option>
          <option value="buy" ${state.buyFilter === "buy" ? "selected" : ""}>買いのみ</option>
          <option value="skip" ${state.buyFilter === "skip" ? "selected" : ""}>見送りのみ</option>
        </select>
      </label>
      <label>並び順
        <select id="raceSort">
          <option value="recommendation" ${state.raceSort === "recommendation" ? "selected" : ""}>推奨度順</option>
          <option value="post_time" ${state.raceSort === "post_time" ? "selected" : ""}>発走時刻順</option>
        </select>
      </label>
    `;
  } else if (state.route === "wide") {
    html += `
      <label>並び順
        <select id="wideSort">
          <option value="expected_value" ${state.wideSort === "expected_value" ? "selected" : ""}>EV順</option>
          <option value="recommendation" ${state.wideSort === "recommendation" ? "selected" : ""}>推奨度順</option>
        </select>
      </label>
      <label>最低オッズ
        <input id="wideMinOdds" type="number" min="0" step="0.1" value="${state.wideMinOdds}" />
      </label>
      <label>
        <input id="wideHighEvOnly" type="checkbox" ${state.wideHighEvOnly ? "checked" : ""} />
        高EVのみ
      </label>
    `;
  }

  filterBar.innerHTML = html;
  bindFilterEvents();
}

function bindFilterEvents() {
  const venue = document.getElementById("venueSelect");
  if (venue) venue.onchange = (event) => { state.venue = event.target.value; render(); };

  const buyFilter = document.getElementById("buyFilter");
  if (buyFilter) buyFilter.onchange = (event) => { state.buyFilter = event.target.value; render(); };

  const raceSort = document.getElementById("raceSort");
  if (raceSort) raceSort.onchange = (event) => { state.raceSort = event.target.value; render(); };

  const wideSort = document.getElementById("wideSort");
  if (wideSort) wideSort.onchange = (event) => { state.wideSort = event.target.value; render(); };

  const wideMinOdds = document.getElementById("wideMinOdds");
  if (wideMinOdds) wideMinOdds.onchange = (event) => { state.wideMinOdds = Number(event.target.value || 0); render(); };

  const wideHighEvOnly = document.getElementById("wideHighEvOnly");
  if (wideHighEvOnly) wideHighEvOnly.onchange = (event) => { state.wideHighEvOnly = event.target.checked; render(); };
}

function filterByVenue(rows) {
  if (state.venue === "all") return rows;
  return rows.filter((row) => row.venue === state.venue);
}

function renderTopPage() {
  let rows = filterByVenue(store.races);
  if (state.buyFilter === "buy") rows = rows.filter((row) => row.buy_flag);
  if (state.buyFilter === "skip") rows = rows.filter((row) => !row.buy_flag);
  rows = state.raceSort === "post_time" ? sortRows(rows, "post_time", false) : sortRows(rows, "recommendation", true);

  const template = document.getElementById("raceCardTemplate");
  const grid = document.createElement("div");
  grid.className = "grid";

  rows.forEach((race) => {
    const node = template.content.cloneNode(true);
    node.querySelector("h3").textContent = `${race.venue} ${race.race_no}R`;
    const badge = node.querySelector(".badge");
    badge.textContent = race.buy_flag ? "買い" : "見送り";
    badge.classList.add(race.buy_flag ? "buy" : "skip");

    node.querySelector(".race-meta").innerHTML = `
      <div><dt>発走</dt><dd>${race.post_time}</dd></div>
      <div><dt>条件</dt><dd>${race.condition}</dd></div>
      <div><dt>推奨度</dt><dd class="${race.recommendation >= 75 ? "high" : "muted"}">${race.recommendation ?? "-"}</dd></div>
      <div><dt>候補ペア</dt><dd>${race.candidate_pairs ?? "-"}</dd></div>
      <div><dt>想定回収率</dt><dd class="${isNum(race.expected_roi) && race.expected_roi >= 1 ? "high" : "muted"}">${formatFixed(race.expected_roi, 2)}</dd></div>
      <div><dt>AI市場差</dt><dd>${formatSigned(race.ai_market_gap)}</dd></div>
    `;

    node.querySelector("button").onclick = () => {
      location.hash = `#race/${encodeURIComponent(race.race_id)}`;
    };
    grid.appendChild(node);
  });

  app.innerHTML = "";
  app.appendChild(grid);
}

function renderRacePage() {
  const race = store.races.find((row) => row.race_id === state.selectedRaceId) || store.races[0];
  if (!race) {
    app.innerHTML = `<section class="panel"><h2>レース詳細</h2><p class="muted">レースデータがありません。</p></section>`;
    return;
  }
  state.selectedRaceId = race.race_id;
  const horses = store.horses.filter((row) => row.race_id === race.race_id).sort((a, b) => (a.ai_rank ?? 999) - (b.ai_rank ?? 999));
  const summary = store.summary.find((row) => row.race_id === race.race_id);

  app.innerHTML = `
    <section class="panel">
      <h2>${race.venue} ${race.race_no}R 詳細 (${race.race_id})</h2>
      <p class="muted">${race.condition ?? "-"} / ${race.field_size ?? "-"}頭 / 馬場: ${race.track ?? "-"} / ${race.surface ?? "-"}</p>
      <p class="muted">density_top3: ${formatFixed(race.density_top3, 2)} / gap12: ${formatFixed(race.gap12, 2)} / 混戦度: ${formatFixed(race.chaos_index, 2)}</p>
      <p class="muted">見送り理由: ${summary?.reason ?? "-"}</p>
      <button id="backToTop" class="secondary">トップへ戻る</button>
    </section>
    <section class="panel">
      <h2>馬一覧</h2>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>枠</th><th>馬番</th><th>馬名</th><th>人気</th><th>AI</th><th>勝率</th><th>top3率</th>
              <th>ability</th><th>安定度</th><th>妙味</th><th>枠補正</th><th>役割</th><th>市場差</th>
            </tr>
          </thead>
          <tbody>
            ${horses.map((horse) => `
              <tr>
                <td>${horse.waku ?? "-"}</td>
                <td>${horse.horse_no ?? "-"}</td>
                <td>${horse.horse_name ?? "-"}</td>
                <td>${horse.pop_rank ?? "-"}</td>
                <td class="${horse.ai_rank <= 3 ? "high" : ""}">${horse.ai_rank ?? "-"}</td>
                <td>${formatPct(horse.win_rate)}</td>
                <td>${formatPct(horse.top3_rate)}</td>
                <td class="${isNum(horse.ability) && horse.ability >= 80 ? "high" : ""}">${formatFixed(horse.ability, 1)}</td>
                <td>${formatFixed(horse.stability, 2)}</td>
                <td class="${isNum(horse.value_score) && horse.value_score >= 0.65 ? "warning" : ""}">${formatFixed(horse.value_score, 2)}</td>
                <td>${formatFixed(horse.course_waku_final_multi, 2)}</td>
                <td>${horse.role ?? "-"}</td>
                <td class="${isNum(horse.market_gap) && horse.market_gap > 0 ? "high" : "muted"}">${formatSigned(horse.market_gap)}</td>
              </tr>
            `).join("")}
          </tbody>
        </table>
      </div>
    </section>
  `;

  document.getElementById("backToTop").onclick = () => { location.hash = "#top"; };
}

function renderWidePage() {
  let rows = filterByVenue(store.bets);
  rows = rows.filter((row) => !isNum(row.wide_odds_est) || row.wide_odds_est >= state.wideMinOdds);
  if (state.wideHighEvOnly) rows = rows.filter((row) => isNum(row.expected_value) && row.expected_value >= 1.2);
  rows = sortRows(rows, state.wideSort, true);

  app.innerHTML = `
    <section class="panel">
      <h2>推奨ワイド候補</h2>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>開催場</th><th>R</th><th>ペア</th><th>軸</th><th>相手</th><th>pair_score</th><th>EV</th>
              <th>top3率</th><th>妙味</th><th>想定オッズ</th><th>推奨度</th><th>stage</th>
            </tr>
          </thead>
          <tbody>
            ${rows.map((row) => `
              <tr>
                <td>${row.venue ?? "-"}</td>
                <td>${row.race_no ?? "-"}</td>
                <td>${row.pair ?? "-"}</td>
                <td>${row.axis_horse ?? "-"}</td>
                <td>${row.partner_horse ?? "-"}</td>
                <td>${formatFixed(row.pair_score, 2)}</td>
                <td class="${isNum(row.expected_value) && row.expected_value >= 1.2 ? "high" : ""}">${formatFixed(row.expected_value, 2)}</td>
                <td>${formatPct(row.top3_rate_pair)}</td>
                <td>${row.value_label ?? "-"}</td>
                <td>${formatFixed(row.wide_odds_est, 1)}</td>
                <td class="${row.recommendation >= 85 ? "high" : ""}">${row.recommendation ?? "-"}</td>
                <td>${row.selected_stage ?? "-"}</td>
              </tr>
            `).join("")}
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function renderSkipPage() {
  let rows = store.summary.map((row) => {
    const race = store.races.find((raceRow) => raceRow.race_id === row.race_id);
    return { ...row, venue: race?.venue ?? "-", race_no: race?.race_no ?? "-" };
  });
  rows = filterByVenue(rows);

  app.innerHTML = `
    <section class="panel">
      <h2>見送りレース</h2>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>開催場</th><th>R</th><th>race_id</th><th>見送り理由</th><th>人気集中</th><th>AI市場差小</th>
              <th>データ不足</th><th>density超過</th><th>gap12不足</th><th>オッズ不安定</th><th>horse_id欠損</th>
            </tr>
          </thead>
          <tbody>
            ${rows.map((row) => `
              <tr>
                <td>${row.venue}</td>
                <td>${row.race_no}</td>
                <td>${row.race_id}</td>
                <td>${row.reason ?? "-"}</td>
                <td>${row.popular_concentration ? "✓" : "-"}</td>
                <td>${row.small_ai_market_gap ? "✓" : "-"}</td>
                <td>${row.data_shortage ? "✓" : "-"}</td>
                <td>${row.density_top3_excess ? "✓" : "-"}</td>
                <td>${row.gap12_shortage ? "✓" : "-"}</td>
                <td>${row.odds_unstable ? "✓" : "-"}</td>
                <td>${row.horse_id_missing ? "✓" : "-"}</td>
              </tr>
            `).join("")}
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function renderComparisonPage() {
  const view = store.comparisonView;
  if (!view) {
    app.innerHTML = `
      <section class="panel">
        <h2>comparison</h2>
        <p class="muted">比較データ未生成です。compare-experiments 実行後、comparison_view.json を配置してください。</p>
      </section>
    `;
    return;
  }

  const bestMetricDefs = [
    ["best_logloss_after", "Best Logloss"],
    ["best_brier_after", "Best Brier"],
    ["best_ece_after", "Best ECE"],
    ["best_roi", "Best ROI"],
    ["best_hit_rate", "Best Hit Rate"],
    ["best_buy_races", "Best Buy Races"]
  ];
  const bestCards = bestMetricDefs.map(([key, label]) => {
    const item = view.best_summary?.[key];
    return `
      <article class="best-card">
        <h3>${label}</h3>
        <p><strong>experiment:</strong> ${item?.experiment_name ?? "-"}</p>
        <p><strong>model:</strong> ${item?.model_version ?? "-"}</p>
        <p><strong>value:</strong> ${formatFixed(item?.value, 4)}</p>
      </article>
    `;
  }).join("");

  const leaderboardRows = (view.leaderboard ?? []).map((row) => `
    <tr class="${row.comparison_status === "mismatch" ? "row-mismatch" : ""}">
      <td>${row.experiment_name ?? "-"}</td>
      <td>${row.model_version ?? "-"}</td>
      <td>${row.feature_snapshot_version ?? "-"}</td>
      <td>${statusBadge(row.comparison_status)}</td>
      <td>${formatYesNo(row.has_calibration)}</td>
      <td>${formatFixed(row.logloss_after, 4)}</td>
      <td>${formatFixed(row.brier_after, 4)}</td>
      <td>${formatFixed(row.ece_after, 4)}</td>
      <td>${formatFixed(row.roi, 4)}</td>
      <td>${formatFixed(row.hit_rate, 4)}</td>
      <td>${formatFixed(row.buy_races, 0)}</td>
      <td>${formatFixed(row.total_bets, 0)}</td>
      <td>${formatFixed(row.logloss_delta, 4)}</td>
      <td>${formatFixed(row.brier_delta, 4)}</td>
      <td>${formatFixed(row.ece_delta, 4)}</td>
      <td>${(row.missing_inputs ?? []).join(", ") || "-"}</td>
    </tr>
  `).join("");

  const rankingDefs = [
    ["by_logloss_after", "Logloss"],
    ["by_brier_after", "Brier"],
    ["by_ece_after", "ECE"],
    ["by_roi", "ROI"],
    ["by_hit_rate", "Hit Rate"]
  ];
  const rankingCards = rankingDefs.map(([key, title]) => {
    const items = view.ranking_views?.[key] ?? [];
    const topItems = items.slice(0, 5).map((item, index) => `<li>${index + 1}. ${item.experiment_name ?? "-"} (${item.model_version ?? "-"}) : ${formatFixed(item.value, 4)}</li>`).join("");
    return `
      <article class="ranking-card">
        <h3>${title}</h3>
        <ol>${topItems || "<li>-</li>"}</ol>
      </article>
    `;
  }).join("");

  const mismatchReasons = Object.entries(view.issues_summary?.mismatch_reasons_summary ?? {})
    .map(([reason, count]) => `<li>${reason}: ${count}</li>`)
    .join("");

  app.innerHTML = `
    <section class="panel">
      <h2>comparison header</h2>
      <div class="meta-grid">
        <div><dt>dataset_name</dt><dd>${view.dataset_name ?? "-"}</dd></div>
        <div><dt>status</dt><dd>${statusBadge(view.comparison_status)}</dd></div>
        <div><dt>experiment_count</dt><dd>${view.experiment_count ?? 0}</dd></div>
        <div><dt>valid_experiment_count</dt><dd>${view.valid_experiment_count ?? 0}</dd></div>
        <div><dt>mismatch_experiment_count</dt><dd>${view.mismatch_experiment_count ?? 0}</dd></div>
        <div><dt>missing_calibration_count</dt><dd>${view.missing_calibration_count ?? 0}</dd></div>
      </div>
    </section>

    <section class="panel">
      <h2>best summary</h2>
      <div class="best-grid">${bestCards}</div>
    </section>

    <section class="panel">
      <h2>leaderboard</h2>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>experiment_name</th><th>model_version</th><th>feature_snapshot</th><th>status</th><th>calibration</th>
              <th>logloss_after</th><th>brier_after</th><th>ece_after</th><th>roi</th><th>hit_rate</th><th>buy_races</th>
              <th>total_bets</th><th>logloss_delta</th><th>brier_delta</th><th>ece_delta</th><th>missing_inputs</th>
            </tr>
          </thead>
          <tbody>${leaderboardRows || `<tr><td colspan="16" class="muted">-</td></tr>`}</tbody>
        </table>
      </div>
    </section>

    <section class="panel">
      <h2>ranking views</h2>
      <div class="ranking-grid">${rankingCards}</div>
    </section>

    <section class="panel">
      <h2>issues summary</h2>
      <div class="issue-grid">
        <article><h3>mismatch_experiments</h3><p>${(view.issues_summary?.mismatch_experiments ?? []).join(", ") || "-"}</p></article>
        <article><h3>missing_calibration_experiments</h3><p>${(view.issues_summary?.missing_calibration_experiments ?? []).join(", ") || "-"}</p></article>
        <article><h3>missing_run_summary_experiments</h3><p>${(view.issues_summary?.missing_run_summary_experiments ?? []).join(", ") || "-"}</p></article>
        <article><h3>skipped_from_best_selection</h3><p>${(view.issues_summary?.skipped_from_best_selection ?? []).join(", ") || "-"}</p></article>
        <article><h3>mismatch_reasons_summary</h3><ul>${mismatchReasons || "<li>-</li>"}</ul></article>
      </div>
      <details>
        <summary>source_paths</summary>
        <ul class="muted">
          <li>comparison_report_json_path: ${view.source_paths?.comparison_report_json_path ?? "-"}</li>
          <li>comparison_report_csv_path: ${view.source_paths?.comparison_report_csv_path ?? "-"}</li>
          <li>dataset_manifest_path: ${view.source_paths?.dataset_manifest_path ?? "-"}</li>
        </ul>
      </details>
    </section>
  `;
}

function render() {
  parseRoute();
  activeTab(state.route);
  renderFilters();

  if (state.route === "top") renderTopPage();
  else if (state.route === "wide") renderWidePage();
  else if (state.route === "skip") renderSkipPage();
  else if (state.route === "comparison") renderComparisonPage();
  else renderRacePage();
}

window.addEventListener("hashchange", render);

initTheme();

loadData()
  .then((loaded) => {
    store = loaded;
    if (!location.hash) location.hash = "#top";
    render();
  })
  .catch((error) => {
    app.innerHTML = `<section class="panel"><h2>データ読み込みエラー</h2><p>${error.message}</p></section>`;
  });
