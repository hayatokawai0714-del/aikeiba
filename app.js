const DATA_FILES = {
  races: "./data/races_today.json",
  horses: "./data/horse_predictions.json",
  bets: "./data/today_pipeline_bets.json",
  summary: "./data/race_summary.json"
};

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

const formatPct = (v) => `${Math.round(v * 100)}%`;
const formatSigned = (v) => `${v > 0 ? "+" : ""}${v.toFixed(2)}`;

const app = document.getElementById("app");
const filterBar = document.getElementById("globalFilters");
const todayLabel = document.getElementById("todayLabel");
todayLabel.textContent = `更新日: ${new Date().toLocaleDateString("ja-JP")}`;

let store = null;

async function loadData() {
  const entries = await Promise.all(
    Object.entries(DATA_FILES).map(async ([key, path]) => {
      const res = await fetch(path);
      if (!res.ok) throw new Error(`${path} の読み込みに失敗`);
      return [key, await res.json()];
    })
  );
  return Object.fromEntries(entries);
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
  } else if (["top", "wide", "skip"].includes(routeName)) {
    state.route = routeName;
  } else {
    state.route = "top";
  }
}

function sortRows(rows, key, desc = true) {
  const sorted = [...rows].sort((a, b) => (a[key] > b[key] ? 1 : -1));
  return desc ? sorted.reverse() : sorted;
}

function renderFilters() {
  const venues = ["all", ...new Set(store.races.map((r) => r.venue))];
  let html = `
    <label>開催場
      <select id="venueSelect">
        ${venues.map((v) => `<option value="${v}" ${state.venue === v ? "selected" : ""}>${v === "all" ? "すべて" : v}</option>`).join("")}
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
  if (venue) venue.onchange = (e) => { state.venue = e.target.value; render(); };

  const buyFilter = document.getElementById("buyFilter");
  if (buyFilter) buyFilter.onchange = (e) => { state.buyFilter = e.target.value; render(); };

  const raceSort = document.getElementById("raceSort");
  if (raceSort) raceSort.onchange = (e) => { state.raceSort = e.target.value; render(); };

  const wideSort = document.getElementById("wideSort");
  if (wideSort) wideSort.onchange = (e) => { state.wideSort = e.target.value; render(); };

  const wideMinOdds = document.getElementById("wideMinOdds");
  if (wideMinOdds) wideMinOdds.onchange = (e) => { state.wideMinOdds = Number(e.target.value || 0); render(); };

  const wideHighEvOnly = document.getElementById("wideHighEvOnly");
  if (wideHighEvOnly) wideHighEvOnly.onchange = (e) => { state.wideHighEvOnly = e.target.checked; render(); };
}

function filterByVenue(rows) {
  if (state.venue === "all") return rows;
  return rows.filter((r) => r.venue === state.venue);
}

function renderTopPage() {
  let rows = filterByVenue(store.races);
  if (state.buyFilter === "buy") rows = rows.filter((r) => r.buy_flag);
  if (state.buyFilter === "skip") rows = rows.filter((r) => !r.buy_flag);

  rows = state.raceSort === "post_time" ? sortRows(rows, "post_time", false) : sortRows(rows, "recommendation", true);

  const tpl = document.getElementById("raceCardTemplate");
  const grid = document.createElement("div");
  grid.className = "grid";

  rows.forEach((race) => {
    const node = tpl.content.cloneNode(true);
    node.querySelector("h3").textContent = `${race.venue} ${race.race_no}R`;
    const badge = node.querySelector(".badge");
    badge.textContent = race.buy_flag ? "買い" : "見送り";
    badge.classList.add(race.buy_flag ? "buy" : "skip");

    node.querySelector(".race-meta").innerHTML = `
      <div><dt>発走</dt><dd>${race.post_time}</dd></div>
      <div><dt>条件</dt><dd>${race.condition}</dd></div>
      <div><dt>推奨度</dt><dd class="${race.recommendation >= 75 ? "high" : "muted"}">${race.recommendation}</dd></div>
      <div><dt>候補ペア</dt><dd>${race.candidate_pairs}</dd></div>
      <div><dt>想定回収率</dt><dd class="${race.expected_roi >= 1 ? "high" : "low"}">${race.expected_roi.toFixed(2)}</dd></div>
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
  const race = store.races.find((r) => r.race_id === state.selectedRaceId) || store.races[0];
  state.selectedRaceId = race.race_id;
  const horses = store.horses.filter((h) => h.race_id === race.race_id).sort((a, b) => a.ai_rank - b.ai_rank);
  const summary = store.summary.find((s) => s.race_id === race.race_id);

  const summaryHtml = summary
    ? `<p class="muted">見送り理由: ${summary.reason}</p>`
    : `<p class="muted">見送り理由: なし（買い候補）</p>`;

  app.innerHTML = `
    <section class="panel">
      <h2>${race.venue} ${race.race_no}R 詳細 (${race.race_id})</h2>
      <p class="muted">${race.condition} / ${race.field_size}頭 / 馬場: ${race.track} / ${race.surface}</p>
      <p class="muted">density_top3: ${race.density_top3.toFixed(2)} / gap12: ${race.gap12.toFixed(2)} / 混戦度: ${race.chaos_index.toFixed(2)}</p>
      ${summaryHtml}
      <button id="backToTop" class="secondary">トップに戻る</button>
    </section>

    <section class="panel">
      <h2>馬一覧</h2>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>枠</th><th>馬番</th><th>馬名</th><th>人気</th><th>AI順位</th><th>勝率</th><th>top3率</th>
              <th>ability</th><th>安定度</th><th>妙味</th><th>枠補正</th><th>役割</th><th>市場差</th>
            </tr>
          </thead>
          <tbody>
            ${horses.map((h) => `
              <tr>
                <td>${h.waku}</td>
                <td>${h.horse_no}</td>
                <td>${h.horse_name}</td>
                <td>${h.pop_rank}</td>
                <td class="${h.ai_rank <= 3 ? "high" : ""}">${h.ai_rank}</td>
                <td>${formatPct(h.win_rate)}</td>
                <td>${formatPct(h.top3_rate)}</td>
                <td class="${h.ability >= 80 ? "high" : ""}">${h.ability.toFixed(1)}</td>
                <td>${h.stability.toFixed(2)}</td>
                <td class="${h.value_score >= 0.65 ? "warning" : ""}">${h.value_score.toFixed(2)}</td>
                <td>${h.course_waku_final_multi.toFixed(2)}</td>
                <td>${h.role === "軸向き" ? `<span class="pill axis">${h.role}</span>` : `<span class="pill value">${h.role}</span>`}</td>
                <td class="${h.market_gap > 0 ? "high" : "muted"}">${formatSigned(h.market_gap)}</td>
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
  rows = rows.filter((r) => r.wide_odds_est >= state.wideMinOdds);
  if (state.wideHighEvOnly) rows = rows.filter((r) => r.expected_value >= 1.2);
  rows = sortRows(rows, state.wideSort, true);

  app.innerHTML = `
    <section class="panel">
      <h2>推奨ワイド候補</h2>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>開催</th><th>R</th><th>ペア</th><th>軸</th><th>相手</th><th>pair_score</th><th>EV</th>
              <th>top3率</th><th>妙味</th><th>想定オッズ</th><th>推奨度</th><th>stage</th>
            </tr>
          </thead>
          <tbody>
            ${rows.map((r) => `
              <tr>
                <td>${r.venue}</td>
                <td>${r.race_no}</td>
                <td>${r.pair}</td>
                <td>${r.axis_horse}</td>
                <td>${r.partner_horse}</td>
                <td>${r.pair_score}</td>
                <td class="${r.expected_value >= 1.2 ? "high" : ""}">${r.expected_value.toFixed(2)}</td>
                <td>${formatPct(r.top3_rate_pair)}</td>
                <td>${r.value_label}</td>
                <td>${r.wide_odds_est.toFixed(1)}</td>
                <td class="${r.recommendation >= 85 ? "high" : ""}">${r.recommendation}</td>
                <td>${r.selected_stage}</td>
              </tr>
            `).join("")}
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function renderSkipPage() {
  let rows = store.summary.map((s) => {
    const race = store.races.find((r) => r.race_id === s.race_id);
    return { ...s, venue: race?.venue ?? "-", race_no: race?.race_no ?? "-" };
  });
  rows = filterByVenue(rows);

  app.innerHTML = `
    <section class="panel">
      <h2>見送りレース</h2>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>開催</th><th>R</th><th>race_id</th><th>見送り理由</th><th>人気集中</th><th>AI市場差小</th>
              <th>データ不足</th><th>density超過</th><th>gap12不足</th><th>オッズ不安定</th><th>horse_id欠損</th>
            </tr>
          </thead>
          <tbody>
            ${rows.map((r) => `
              <tr>
                <td>${r.venue}</td>
                <td>${r.race_no}</td>
                <td>${r.race_id}</td>
                <td>${r.reason}</td>
                <td>${r.popular_concentration ? "○" : "-"}</td>
                <td>${r.small_ai_market_gap ? "○" : "-"}</td>
                <td>${r.data_shortage ? "○" : "-"}</td>
                <td>${r.density_top3_excess ? "○" : "-"}</td>
                <td>${r.gap12_shortage ? "○" : "-"}</td>
                <td>${r.odds_unstable ? "○" : "-"}</td>
                <td>${r.horse_id_missing ? "○" : "-"}</td>
              </tr>
            `).join("")}
          </tbody>
        </table>
      </div>
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
  else renderRacePage();
}

window.addEventListener("hashchange", render);

loadData()
  .then((loaded) => {
    store = loaded;
    if (!location.hash) location.hash = "#top";
    render();
  })
  .catch((error) => {
    app.innerHTML = `<section class="panel"><h2>データ読み込みエラー</h2><p>${error.message}</p></section>`;
  });
