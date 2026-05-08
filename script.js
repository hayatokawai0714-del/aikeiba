const DATA_PATH = "./data/races_today.json";
const BETS_PATH = "./data/today_pipeline_bets.json";
const RACES_CANDIDATE_PATHS = [
  "./data/races_today.json",
  "./racing_ai/data/exports/static/races_today.json",
  "./racing_ai/data/exports/races_today.json",
];
const BETS_CANDIDATE_PATHS = [
  "./data/today_pipeline_bets.json",
  "./racing_ai/data/exports/static/today_pipeline_bets.json",
  "./racing_ai/data/exports/today_pipeline_bets.json",
];

const VENUE_MAP = {
  "1": "札幌",
  "2": "函館",
  "3": "福島",
  "4": "新潟",
  "5": "東京",
  "6": "中山",
  "7": "中京",
  "8": "京都",
  "9": "阪神",
  "10": "小倉",
};

const todayLabel = document.getElementById("todayLabel");
const refreshBtn = document.getElementById("refreshBtn");
const todayDateInput = document.getElementById("todayDateInput");
const oddsCutoffInput = document.getElementById("oddsCutoffInput");
const apiStatus = document.getElementById("apiStatus");
const loadingText = document.getElementById("loadingText");
const lastRunLabel = document.getElementById("lastRunLabel");
const buyList = document.getElementById("buyList");
const raceList = document.getElementById("raceList");
const cardTemplate = document.getElementById("raceCardTemplate");

const formatMetric = (value, digits = 3) =>
  Number.isFinite(Number(value)) ? Number(value).toFixed(digits) : "-";

function two(value) {
  return String(value).padStart(2, "0");
}

function formatDateForInput(date) {
  return `${date.getFullYear()}-${two(date.getMonth() + 1)}-${two(date.getDate())}`;
}

function formatDateTimeForInput(date) {
  return `${formatDateForInput(date)}T${two(date.getHours())}:${two(date.getMinutes())}`;
}

function setLoading(isLoading, message = "", isError = false) {
  refreshBtn.disabled = isLoading;
  loadingText.textContent = message;
  loadingText.classList.toggle("error", isError);
}

function setApiStatus() {
  apiStatus.classList.remove("status-ok", "status-offline", "status-token");
  apiStatus.classList.add("status-ok");
  apiStatus.textContent = "API: 使用しない（ローカルJSON表示のみ）";
}

function toShortReason(reason) {
  if (!reason) return "-";
  if (reason === "race_filtered_out") return "対象外";
  if (reason.includes("top2")) return "上位2位";
  if (reason.includes("candidate_pool")) return "候補プール";
  if (reason.includes("non_candidate")) return "対象外";
  return reason;
}

function toPairText(race) {
  if (race.top_pair) return race.top_pair;
  if (race.best_pair) return race.best_pair;
  if (race._pairs_text) return race._pairs_text;
  return race.candidate_pairs > 0 ? `候補 ${race.candidate_pairs} 件` : "-";
}

function toPairHorseNames(race) {
  return race.top_pair_horse_names && race.top_pair_horse_names !== "-"
    ? race.top_pair_horse_names
    : "馬名未設定";
}

function toPairHorseIds(race) {
  return race.top_pair_horse_ids && race.top_pair_horse_ids !== "-"
    ? race.top_pair_horse_ids
    : "-";
}

function toNameStatus(race) {
  return race.top_pair_horse_name_status === "verified"
    ? "馬名: 検証済み"
    : "馬名: 未検証";
}

function pickAiGap(race) {
  if (Number.isFinite(Number(race.value_gap))) return Number(race.value_gap);
  if (Number.isFinite(Number(race.ability_gap))) return Number(race.ability_gap);
  if (Number.isFinite(Number(race.ai_market_gap))) return Number(race.ai_market_gap);
  return null;
}

function displayPostTime(postTime) {
  return postTime || "発走時刻 未設定";
}

function displayOddsCutoff(cutoff) {
  if (!cutoff || cutoff === "-") return "-";
  return String(cutoff).replace("T", " ");
}

function venueName(race) {
  if (race.venue_name) return race.venue_name;
  const code = String(race.venue ?? "");
  return VENUE_MAP[code] ?? (code || "-");
}

function raceLabel(race) {
  return `${venueName(race)} ${race.race_no ?? "-"}R`;
}

function renderBuyList(rows) {
  buyList.innerHTML = "";
  const buys = rows.filter((row) => Boolean(row.buy_flag));
  if (buys.length === 0) {
    buyList.innerHTML = "<li>買いレースはありません</li>";
    return;
  }
  buys.forEach((race) => {
    const item = document.createElement("li");
    const pairLines = Array.isArray(race._pair_list) && race._pair_list.length > 0
      ? race._pair_list.map((p) => `<div>・${p}</div>`).join("")
      : `<div>${toPairText(race)}</div>`;

    item.innerHTML = `${raceLabel(race)} / <span class="pair">候補 ${race.candidate_pairs ?? 0} 件</span><br>${pairLines}<br><small>${toPairHorseNames(race)}</small><br><small>horse_id: ${toPairHorseIds(race)}</small><br><small>${toNameStatus(race)} / ${race.race_id ?? "-"}</small>`;
    buyList.appendChild(item);
  });
}

function renderRaceCards(rows) {
  raceList.innerHTML = "";
  const sorted = [...rows].sort(
    (a, b) => Number(Boolean(b.buy_flag)) - Number(Boolean(a.buy_flag)),
  );

  sorted.forEach((race) => {
    const card = cardTemplate.content.cloneNode(true);
    const cardRoot = card.querySelector(".race-card");
    const isBuy = Boolean(race.buy_flag);
    cardRoot.classList.add(isBuy ? "buy-race" : "skip-race");

    card.querySelector(".race-time").textContent = displayPostTime(race.post_time);
    card.querySelector(".race-title").textContent = raceLabel(race);

    const pill = card.querySelector(".status-pill");
    pill.textContent = isBuy ? "買い" : "見送り";
    pill.classList.add(isBuy ? "buy" : "skip");

    card.querySelector(".pair-text").textContent = `推奨ペア: ${toPairText(race)}`;
    card.querySelector(".recommendation").textContent =
      `馬名: ${toPairHorseNames(race)} (${toNameStatus(race)}) / horse_id: ${toPairHorseIds(race)} / 推奨度: ${race.recommendation ?? "-"}`;
    card.querySelector(".mini-metrics").textContent =
      `AI差分 ${formatMetric(pickAiGap(race), 3)} / pair_score ${formatMetric(race.pair_score, 3)} / pair_value ${formatMetric(race.pair_value_score, 3)}`;
    card.querySelector(".odds-cutoff").textContent = displayOddsCutoff(race.odds_cutoff);
    card.querySelector(".reason-text").textContent = toShortReason(race.selection_reason);

    raceList.appendChild(card);
  });
}

async function loadRacesFromPath(path) {
  const response = await fetch(`${path}?t=${Date.now()}`, { cache: "no-store" });
  if (!response.ok) return [];
  const data = await response.json();
  return Array.isArray(data) ? data : [];
}

async function loadBetsFromPath(path) {
  const response = await fetch(`${path}?t=${Date.now()}`, { cache: "no-store" });
  if (!response.ok) return [];
  const data = await response.json();
  return Array.isArray(data) ? data : [];
}

function normalizeDateText(value) {
  if (!value) return "";
  return String(value).trim().replaceAll("/", "-");
}

function raceDateFromRaceId(raceId) {
  if (!raceId) return "";
  const m = String(raceId).match(/^(\d{4})(\d{2})(\d{2})-/);
  if (!m) return "";
  return `${m[1]}-${m[2]}-${m[3]}`;
}

function filterRacesByDate(races, dateText) {
  const target = normalizeDateText(dateText);
  if (!target) return races;
  return races.filter((r) => raceDateFromRaceId(r.race_id) === target);
}

function toRaceIdSet(rows) {
  return new Set((rows || []).map((x) => x?.race_id).filter(Boolean));
}

function scoreMatchRaceIds(races, bets) {
  const raceIds = toRaceIdSet(races);
  const betRaceIds = toRaceIdSet(bets);
  if (raceIds.size === 0 || betRaceIds.size === 0) return 0;
  let matched = 0;
  raceIds.forEach((id) => {
    if (betRaceIds.has(id)) matched += 1;
  });
  return matched;
}

async function loadBestRacesByDate(dateText) {
  let bestAll = [];
  let bestAllCount = -1;
  let bestMatched = [];
  let bestMatchedCount = -1;

  for (const path of RACES_CANDIDATE_PATHS) {
    try {
      const races = await loadRacesFromPath(path);
      const matched = filterRacesByDate(races, dateText);
      if (matched.length > bestMatchedCount) {
        bestMatched = matched;
        bestMatchedCount = matched.length;
      }
      if (races.length > bestAllCount) {
        bestAll = races;
        bestAllCount = races.length;
      }
    } catch (_) {
      // ignore and continue
    }
  }

  if (bestMatchedCount > 0) {
    return { races: bestMatched, hasExactDate: true };
  }
  return { races: bestAll, hasExactDate: false };
}

async function loadBestBets(races) {
  let best = [];
  let bestScore = -1;

  for (const path of BETS_CANDIDATE_PATHS) {
    try {
      const bets = await loadBetsFromPath(path);
      const score = scoreMatchRaceIds(races, bets);
      if (score > bestScore) {
        best = bets;
        bestScore = score;
      }
    } catch (_) {
      // ignore and continue
    }
  }

  if (bestScore < 0) {
    return loadBetsFromPath(BETS_PATH).catch(() => []);
  }
  return best;
}

function attachBetPairs(races, bets) {
  const byRace = new Map();
  bets.forEach((b) => {
    if (!b?.race_id || !b?.pair) return;
    if (!byRace.has(b.race_id)) byRace.set(b.race_id, []);
    byRace.get(b.race_id).push(String(b.pair));
  });

  races.forEach((race) => {
    const pairs = byRace.get(race.race_id) ?? [];
    race._pair_list = pairs.slice(0, 10);
    race._pairs_text = race._pair_list.join(", ");
  });
  return races;
}

async function refreshLocalView() {
  try {
    setLoading(true, "ローカルデータを読み込み中...");
    const selectedDate = todayDateInput?.value || "";
    const raceLoad = await loadBestRacesByDate(selectedDate);
    const raceListRaw = Array.isArray(raceLoad.races) ? raceLoad.races : [];
    const bets = await loadBestBets(raceListRaw);
    const list = attachBetPairs(raceListRaw, bets);
    renderBuyList(list);
    renderRaceCards(list);
    lastRunLabel.textContent = `最終更新: ${new Date().toLocaleString("ja-JP")}`;

    if (raceLoad.hasExactDate) {
      setLoading(false, "表示を更新しました。");
    } else {
      setLoading(false, `指定日 ${selectedDate || "-"} のデータが無いため、利用可能データを表示しています。`, true);
    }
  } catch (error) {
    setLoading(false, `更新に失敗しました: ${error.message}`, true);
    buyList.innerHTML = "<li>読み込み失敗</li>";
    raceList.innerHTML = `<p class="error">${error.message}</p>`;
  }
}

async function init() {
  const now = new Date();
  todayLabel.textContent = `対象日: ${now.toLocaleDateString("ja-JP")}`;
  if (todayDateInput) todayDateInput.value = formatDateForInput(now);
  if (oddsCutoffInput) oddsCutoffInput.value = formatDateTimeForInput(now);
  setApiStatus();

  refreshBtn.addEventListener("click", refreshLocalView);
  await refreshLocalView();
}

init();
