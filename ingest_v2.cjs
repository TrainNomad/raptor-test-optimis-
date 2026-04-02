/**
 * ingest_v2.js — Prétraitement stations.csv → RAPTOR Station-Based
 *
 * Ce script remplace build-stations-index.js + gtfs-ingest.js pour la partie
 * "globalisation des arrêts". Il fait le lien entre le référentiel Trainline
 * (stations.csv) et les stop_id GTFS de chaque opérateur, puis génère :
 *
 *   engine_data/
 *     stations.bin         — Float32[lat, lon] × N_stations (8 octets/gare)
 *     stop_to_station.json — { "SNCF:StopPoint:...": 1234, "TI:87654321": 1234, … }
 *     stations_meta.json   — { id, name, country, lat, lon, operators[] }[]
 *
 * Usage :
 *   node ingest_v2.js [operators.json] [engine_data_dir] [stations.csv]
 *
 * Pré-requis : avoir lancé gtfs-ingest.js pour que stops.json existe dans engine_data.
 */

'use strict';

const fs   = require('fs');
const path = require('path');

const OPS_FILE  = process.argv[2] || './operators.json';
const DATA_DIR  = process.argv[3] || './engine_data';
const CSV_FILE  = process.argv[4] || path.join(__dirname, 'stations.csv');
const STOPS_FILE = path.join(DATA_DIR, 'stops.json');

// ─── Validation des entrées ───────────────────────────────────────────────────

for (const [label, p] of [['stops.json', STOPS_FILE], ['stations.csv', CSV_FILE]]) {
  if (!fs.existsSync(p)) {
    console.error(`❌  ${label} introuvable : ${p}`);
    if (label === 'stops.json') console.error('   Lance d\'abord : node gtfs-ingest.js');
    process.exit(1);
  }
}

// ─── Helpers CSV ──────────────────────────────────────────────────────────────

/**
 * Parse une ligne CSV avec gestion des guillemets RFC 4180.
 * Retourne un tableau de strings (sans les guillemets).
 */
function parseCSVLine(line) {
  const result = [];
  let cur = '';
  let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') {
      // Guillemet doublé à l'intérieur d'un champ guillemété → guillemet littéral
      if (inQ && line[i + 1] === '"') { cur += '"'; i++; }
      else inQ = !inQ;
    } else if (c === ',' && !inQ) {
      result.push(cur);
      cur = '';
    } else {
      cur += c;
    }
  }
  result.push(cur);
  return result;
}

/**
 * Lit un CSV ligne par ligne via un stream (pour les gros fichiers).
 * onRow(row) reçoit un objet { colonne: valeur } et retourne true pour garder la ligne.
 * Retourne la liste des lignes gardées (ou un tableau vide si onRow retourne toujours false).
 */
function streamCSV(filePath, onRow, keepRows = false) {
  const readline = require('readline');
  return new Promise((resolve, reject) => {
    const rows = [];
    let headers = null;
    const rl = readline.createInterface({
      input: fs.createReadStream(filePath, { encoding: 'utf8', highWaterMark: 256 * 1024 }),
      crlfDelay: Infinity,
    });
    rl.on('line', (raw) => {
      const line = raw.replace(/^\uFEFF/, '').trim();
      if (!line) return;
      const cols = parseCSVLine(line);
      if (!headers) { headers = cols; return; }
      const row = {};
      headers.forEach((h, i) => { row[h] = (cols[i] || '').trim(); });
      const keep = onRow(row);
      if (keepRows && keep) rows.push(row);
    });
    rl.on('close', () => resolve(rows));
    rl.on('error', reject);
  });
}

// ─── Chargement de stops.json ─────────────────────────────────────────────────

console.log('\n🚉 ingest_v2.js — Prétraitement Station-Based RAPTOR');
console.log('═'.repeat(52));
console.time('Total');

console.log('\n① Chargement stops.json...');
const stops = JSON.parse(fs.readFileSync(STOPS_FILE, 'utf8'));
const totalStops = Object.keys(stops).length;
console.log(`   ${totalStops.toLocaleString()} stops chargés`);

// ─── Étape 1 : Index des stops par type d'identifiant ────────────────────────
//
// Pour chaque opérateur, on construit un dictionnaire :
//   clé → [stopId, ...]
// Où la clé est l'identifiant externe (UIC, code ATOC, renfe_id, etc.)
//
// Cela permet de relier stations.csv → stops.json sans parcours O(n²).

console.log('\n② Construction des index stops par opérateur...');

function extractOperator(sid) {
  const m = (sid || '').match(/^([A-Z0-9_]+):/);
  return m ? m[1] : 'UNKNOWN';
}

// UIC8 → stop_ids (SNCF, TI, TI_AV, SNCB, EU_SLEEPER)
const uic8ToStops = new Map();
// ATOC CRS → stop_ids (UK)
const atocToStops = new Map();
// Renfe code → stop_ids (RENFE, OUIGO_ES)
const renfeToStops = new Map();
// DB Hafas id → stop_ids (DB_FV)
const dbToStops = new Map();
// CP code → stop_ids (CP Portugal)
const cpToStops = new Map();
// Benerail → stop_ids (SNCB étendu)
const benerailToStops = new Map();

for (const [sid, stop] of Object.entries(stops)) {
  const op = stop.operator || extractOperator(sid);

  // ── SNCF : StopPoint:OCE...-87XXXXXXX ou StopArea:OC87XXXXXXX ──
  if (op === 'SNCF') {
    const m = sid.match(/-(\d{7,8})$/) || sid.match(/OC[A-Z](\d{7,8})$/);
    if (m) {
      const k = m[1];
      if (!uic8ToStops.has(k)) uic8ToStops.set(k, []);
      uic8ToStops.get(k).push(sid);
    }
  }

  // ── TI / TI_AV : TI:12345678 ou TI_AV:12345678 ──
  else if (op === 'TI' || op === 'TI_AV') {
    const m = sid.match(/^(?:TI|TI_AV):(\d+)$/);
    if (m) {
      if (!uic8ToStops.has(m[1])) uic8ToStops.set(m[1], []);
      uic8ToStops.get(m[1]).push(sid);
    }
  }

  // ── SNCB : SNCB:8814001 ──
  else if (op === 'SNCB') {
    const m = sid.match(/^SNCB:(\d+)$/);
    if (m) {
      if (!uic8ToStops.has(m[1])) uic8ToStops.set(m[1], []);
      uic8ToStops.get(m[1]).push(sid);
    }
  }

  // ── EU_SLEEPER : stop_id peut contenir le code UIC ──
  else if (op === 'EU_SLEEPER') {
    const m = sid.match(/(\d{7,8})$/);
    if (m) {
      if (!uic8ToStops.has(m[1])) uic8ToStops.set(m[1], []);
      uic8ToStops.get(m[1]).push(sid);
    }
  }

  // ── UK : stop_code CRS ──
  else if (op === 'UK' && stop.code) {
    const crs = stop.code.toUpperCase();
    if (!atocToStops.has(crs)) atocToStops.set(crs, []);
    atocToStops.get(crs).push(sid);
  }

  // ── RENFE / OUIGO_ES : RENFE:71801 ──
  else if (op === 'RENFE' || op === 'OUIGO_ES') {
    const m = sid.match(/^(?:RENFE|OUIGO_ES):(\d+)$/);
    if (m) {
      if (!renfeToStops.has(m[1])) renfeToStops.set(m[1], []);
      renfeToStops.get(m[1]).push(sid);
    }
  }

  // ── DB_FV : DB_FV:8000105 ──
  else if (op === 'DB_FV' || op === 'DB') {
    const m = sid.match(/^(?:DB_FV|DB):(\d+)$/);
    if (m) {
      if (!dbToStops.has(m[1])) dbToStops.set(m[1], []);
      dbToStops.get(m[1]).push(sid);
    }
  }

  // ── CP Portugal : CP:94_2006 → UIC 9402006 ──
  else if (op === 'CP') {
    const m = sid.match(/^CP:94_(\d+)$/);
    if (m) {
      const uicFull = '94' + String(parseInt(m[1])).padStart(5, '0');
      if (!cpToStops.has(uicFull)) cpToStops.set(uicFull, []);
      cpToStops.get(uicFull).push(sid);
    }
  }
}

console.log(`   UIC8       : ${uic8ToStops.size.toLocaleString()} codes`);
console.log(`   ATOC (CRS) : ${atocToStops.size.toLocaleString()} codes`);
console.log(`   Renfe      : ${renfeToStops.size.toLocaleString()} codes`);
console.log(`   DB_FV      : ${dbToStops.size.toLocaleString()} codes`);
console.log(`   CP         : ${cpToStops.size.toLocaleString()} codes`);

// ─── Étape 2 : Parcours de stations.csv pour construire GlobalStations ────────
//
// Chaque ligne du CSV = une gare potentielle.
// On ne garde que les gares utilisables :
//   - is_suggestable = 't' (visible dans l'UI), OU
//   - au moins un stop GTFS correspond (gare réelle dans le réseau)
//
// Les quais (is_main_station = 'f' et parent_station_id non vide) sont groupés
// sous leur gare parente. Les gares parentes (is_city = 't') servent de
// points de regroupement pour l'autocomplete.

console.log('\n③ Parcours de stations.csv → construction GlobalStations...');

// GlobalStations[stationIdx] = { id, name, country, lat, lon, operators: Set }
const globalStations = [];
// Map CSV id (string) → stationIdx (number)
const csvIdToStationIdx = new Map();
// Map stationIdx → [stopId, ...] — rempli pendant le parcours
const stationStopIds = [];

// On lit d'abord toutes les lignes du CSV une fois en mémoire.
// Le CSV fait ~15 MB — acceptable pour Node.js.
// Si votre CSV dépasse 50 MB, remplacez par streamCSV avec deux passes.

let csvRowCount = 0;
const allCsvRows = await (async () => {
  const rows = [];
  await streamCSV(CSV_FILE, (row) => {
    // Filtrage préliminaire : on ignore les entrées sans coordonnées GPS valides
    const lat = parseFloat(row.latitude);
    const lon = parseFloat(row.longitude);
    if (isNaN(lat) || isNaN(lon) || (lat === 0 && lon === 0)) return false;
    rows.push(row);
    csvRowCount++;
    return false; // on accumule manuellement ci-dessus
  });
  return rows;
})();

// Correction : streamCSV avec keepRows ne marche pas ici car on retourne false.
// Relançons avec une version simple.
// (La version ci-dessus n'accumule rien — on corrige.)

// Version corrigée : on relit proprement.
// (Node.js est single-thread donc cette approche synchrone est fine pour un script de build.)

console.log('   Lecture CSV en mémoire...');
const csvRows = [];
{
  // Lecture synchrone ligne par ligne (readline en mode async attendu via promesse)
  const readline = require('readline');
  await new Promise((resolve, reject) => {
    let headers = null;
    const rl = readline.createInterface({
      input: fs.createReadStream(CSV_FILE, { encoding: 'utf8', highWaterMark: 256 * 1024 }),
      crlfDelay: Infinity,
    });
    rl.on('line', (raw) => {
      const line = raw.replace(/^\uFEFF/, '').trim();
      if (!line) return;
      const cols = parseCSVLine(line);
      if (!headers) { headers = cols; return; }
      const row = {};
      headers.forEach((h, i) => { row[h] = (cols[i] || '').trim(); });

      const lat = parseFloat(row.latitude);
      const lon = parseFloat(row.longitude);
      if (isNaN(lat) || isNaN(lon) || (lat === 0 && lon === 0)) return;

      csvRows.push(row);
    });
    rl.on('close', resolve);
    rl.on('error', reject);
  });
}
console.log(`   ${csvRows.length.toLocaleString()} lignes CSV valides (avec coordonnées GPS)`);

// ─── Première passe : créer les entrées GlobalStations ────────────────────────
//
// On indexe d'abord les gares parentes (parent_station_id vide),
// puis on rattache les quais à leur parente.

// Identifie les stop_ids GTFS qui correspondent à cette ligne CSV
function resolveStopIds(row) {
  const found = new Set();

  // ── Via UIC8 (SNCF, TI, TI_AV, SNCB, EU_SLEEPER) ──
  // uic8_sncf est le plus fiable : format 87XXXXXX (8 chiffres)
  for (const uicCol of ['uic8_sncf', 'uic']) {
    const raw = row[uicCol];
    if (raw && raw.length >= 7) {
      const k8  = raw.padStart(8, '0');
      const k7  = raw.padStart(7, '0');
      const k   = raw;
      for (const key of [k8, k7, k]) {
        for (const sid of (uic8ToStops.get(key) || [])) found.add(sid);
      }
    }
  }

  // ── Via sncf_id (code TVS type "FRPAR") ──
  // Les stop_id SNCF finissent par -87XXXXXX, pas par le code TVS.
  // Le sncf_id sert uniquement pour l'autocomplete, pas pour le mapping GTFS.

  // ── Via trenitalia_id ──
  const tiId = row['trenitalia_id'];
  if (tiId) {
    for (const sid of (uic8ToStops.get(tiId) || [])) found.add(sid);
    // Trenitalia Italy : l'id est parfois différent du UIC — on essaie en direct
    const tiDirect = `TI:${tiId}`;
    if (stops[tiDirect]) found.add(tiDirect);
    const tiAvDirect = `TI_AV:${tiId}`;
    if (stops[tiAvDirect]) found.add(tiAvDirect);
  }

  // ── Via atoc_id (UK CRS, 3 lettres) ──
  const atocId = row['atoc_id'];
  if (atocId) {
    for (const sid of (atocToStops.get(atocId.toUpperCase()) || [])) found.add(sid);
  }

  // ── Via db_id (Hafas numérique) ──
  const dbId = row['db_id'];
  if (dbId) {
    for (const sid of (dbToStops.get(dbId) || [])) found.add(sid);
  }

  // ── Via renfe_id ──
  const renfeId = row['renfe_id'];
  if (renfeId) {
    for (const sid of (renfeToStops.get(renfeId) || [])) found.add(sid);
  }

  // ── Via CP Portugal (UIC 94XXXXX) ──
  // Le CSV Trainline n'a pas de colonne cp_id dédiée.
  // On essaie via uic : si le UIC commence par 94, c'est probablement CP.
  const uicRaw = row['uic'];
  if (uicRaw && uicRaw.startsWith('94')) {
    for (const sid of (cpToStops.get(uicRaw.padStart(7, '0')) || [])) found.add(sid);
  }

  return [...found];
}

// Première passe : gares principales (parent_station_id vide = sont leur propre racine)
let stationCount = 0;
const rowById = new Map(); // csv id → row (pour la deuxième passe)

for (const row of csvRows) {
  rowById.set(row.id, row);

  // On crée une station pour chaque ligne, quai ou non.
  // Les quais seront fusionnés avec leur parente à la deuxième passe.
  // Seules les gares suggestables OU ayant des stops GTFS intéressent l'UI.
  const stopIds = resolveStopIds(row);
  const isSuggestable = row.is_suggestable === 't';
  const isCity        = row.is_city === 't';

  // Ignorer les entrées sans stops GTFS ET non-suggestables → elles n'apportent rien
  if (!stopIds.length && !isSuggestable) continue;

  const idx = globalStations.length;
  globalStations.push({
    csv_id:         row.id,
    name:           row.name,
    slug:           row.slug,
    country:        row.country || 'FR',
    lat:            parseFloat(row.latitude),
    lon:            parseFloat(row.longitude),
    is_city:        isCity,
    is_main:        row.is_main_station === 't',
    is_suggestable: isSuggestable,
    parent_csv_id:  row.parent_station_id || null,
    operators:      new Set(),
  });
  stationStopIds.push(new Set(stopIds));
  csvIdToStationIdx.set(row.id, idx);
  stationCount++;
}

console.log(`   ${stationCount.toLocaleString()} stations créées (suggestables + GTFS)`);

// ─── stopToStation : stop_id GTFS → stationIdx ───────────────────────────────
//
// On construit ce dictionnaire en parcourant stationStopIds.
// Si un stop_id apparaît dans plusieurs stations (ex: StopArea + StopPoint SNCF),
// on privilégie la station is_main_station = true, sinon la première rencontrée.

const stopToStation = new Map(); // stopId → stationIdx

for (let idx = 0; idx < globalStations.length; idx++) {
  const st   = globalStations[idx];
  const sids = stationStopIds[idx];
  for (const sid of sids) {
    if (!stopToStation.has(sid)) {
      stopToStation.set(sid, idx);
    } else {
      // Conflit : on garde la station principale
      const existing = globalStations[stopToStation.get(sid)];
      if (st.is_main && !existing.is_main) stopToStation.set(sid, idx);
    }
    // Ajouter l'opérateur à la station
    const op = stops[sid]?.operator || extractOperator(sid);
    if (op) st.operators.add(op);
  }
}

// Stops sans station CSV → on leur crée une station synthétique basée sur le stop GTFS
let synthetic = 0;
for (const [sid, stop] of Object.entries(stops)) {
  if (stopToStation.has(sid)) continue;
  const lat = stop.lat || 0;
  const lon = stop.lon || 0;
  if (!lat && !lon) continue; // stop sans coordonnées → inutile
  const op  = stop.operator || extractOperator(sid);
  const idx = globalStations.length;
  globalStations.push({
    csv_id:         null,
    name:           stop.name || sid,
    slug:           null,
    country:        'XX', // inconnu
    lat, lon,
    is_city:        false,
    is_main:        false,
    is_suggestable: false,
    parent_csv_id:  null,
    operators:      new Set([op]),
  });
  stationStopIds.push(new Set([sid]));
  stopToStation.set(sid, idx);
  synthetic++;
}

console.log(`   ${stopToStation.size.toLocaleString()} stops mappés (dont ${synthetic.toLocaleString()} stations synthétiques)`);
console.log(`   ${globalStations.length.toLocaleString()} stations au total`);

// ─── Étape 3 : Génération des sorties ────────────────────────────────────────

console.log('\n④ Génération des sorties...');

if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

// ── 3a. stations.bin — Float32[lat, lon] × N stations ──
// Format : 4 octets lat (Float32LE) + 4 octets lon (Float32LE) = 8 octets/station
// L'index dans le buffer = stationIdx (utilisé par server.js pour le lookup GPS rapide)
{
  const N   = globalStations.length;
  const buf = Buffer.allocUnsafe(N * 8);
  for (let i = 0; i < N; i++) {
    buf.writeFloatLE(globalStations[i].lat, i * 8);
    buf.writeFloatLE(globalStations[i].lon, i * 8 + 4);
  }
  const outPath = path.join(DATA_DIR, 'stations.bin');
  fs.writeFileSync(outPath, buf);
  const kb = (fs.statSync(outPath).size / 1024).toFixed(1);
  console.log(`   ✓ stations.bin           ${kb} KB  (${N.toLocaleString()} × 8 octets)`);
}

// ── 3b. stop_to_station.json — { stopId: stationIdx } ──
{
  const obj = {};
  for (const [sid, idx] of stopToStation) obj[sid] = idx;
  const outPath = path.join(DATA_DIR, 'stop_to_station.json');
  fs.writeFileSync(outPath, JSON.stringify(obj));
  const kb = (fs.statSync(outPath).size / 1024).toFixed(1);
  console.log(`   ✓ stop_to_station.json   ${kb} KB  (${stopToStation.size.toLocaleString()} entrées)`);
}

// ── 3c. stations_meta.json — métadonnées lisibles ──
// Ce fichier remplace/enrichit stations.json pour server.js.
// Chaque entrée = une station avec ses stopIds, opérateurs, et infos CSV.
{
  const meta = globalStations.map((st, idx) => ({
    id:         idx,
    csv_id:     st.csv_id,
    name:       st.name,
    slug:       st.slug,
    country:    st.country,
    lat:        st.lat,
    lon:        st.lon,
    is_city:    st.is_city,
    is_main:    st.is_main,
    suggestable:st.is_suggestable,
    operators:  [...st.operators].sort(),
    stopIds:    [...stationStopIds[idx]],
    parent:     st.parent_csv_id ? csvIdToStationIdx.get(st.parent_csv_id) ?? null : null,
  }));
  const outPath = path.join(DATA_DIR, 'stations_meta.json');
  fs.writeFileSync(outPath, JSON.stringify(meta));
  const mb = (fs.statSync(outPath).size / 1024 / 1024).toFixed(2);
  console.log(`   ✓ stations_meta.json     ${mb} MB  (${meta.length.toLocaleString()} stations)`);
}

// ─── Résumé ───────────────────────────────────────────────────────────────────

const gtfsCoverage = [...stopToStation.keys()].filter(sid => stops[sid]).length;
const gtfsTotal    = Object.keys(stops).length;
const coveragePct  = ((gtfsCoverage / gtfsTotal) * 100).toFixed(1);

console.log('\n══ Résumé ════════════════════════════════════════════');
console.log(`   Stations globales   : ${globalStations.length.toLocaleString()}`);
console.log(`     dont CSV Trainline: ${(globalStations.length - synthetic).toLocaleString()}`);
console.log(`     dont synthétiques : ${synthetic.toLocaleString()}`);
console.log(`   Stops GTFS mappés   : ${gtfsCoverage.toLocaleString()} / ${gtfsTotal.toLocaleString()} (${coveragePct}%)`);
console.log(`   Opérateurs couverts : ${[...new Set(globalStations.flatMap(s => [...s.operators]))].sort().join(', ')}`);
console.timeEnd('Total');

console.log(`
╔══════════════════════════════════════════════════════╗
║  Prochaine étape : mettre à jour server.js           ║
║  Voir commentaire INTEGRATION ci-dessous.            ║
╚══════════════════════════════════════════════════════╝
`);

/*
 * ══ INTEGRATION dans server.js ════════════════════════════════════════════════
 *
 * 1. Charger stations.bin et stop_to_station.json au démarrage :
 *
 *    const stopToStation  = loadJSON('stop_to_station.json');   // { stopId → idx }
 *    const stationsBin    = fs.readFileSync(path.join(DATA_DIR, 'stations.bin'));
 *    const N_STATIONS     = stationsBin.length / 8;
 *
 *    function stationLat(idx) { return stationsBin.readFloatLE(idx * 8); }
 *    function stationLon(idx) { return stationsBin.readFloatLE(idx * 8 + 4); }
 *
 * 2. Dans RAPTOR, remplacer le tableau earliestArrivals (indexé par stop_id string)
 *    par un Int32Array indexé par station_id (entier) :
 *
 *    const earliestArrivals = new Int32Array(N_STATIONS).fill(Infinity);
 *    // Pour un stop_id "SNCF:StopPoint:...":
 *    const stIdx = stopToStation[sid] ?? -1;
 *    if (stIdx >= 0 && newTime < earliestArrivals[stIdx])
 *      earliestArrivals[stIdx] = newTime;
 *
 * 3. Transferts quai-à-quai devenus gratuits :
 *    Si deux stop_ids ont le même stationIdx → même gare physique →
 *    temps de transfert = 0 (ou 2 min fixe pour changement de quai).
 *    On peut remplacer le parcours du transferIndex pour ces paires.
 *
 *    function transferTimeBetween(sidA, sidB) {
 *      const stA = stopToStation[sidA] ?? -1;
 *      const stB = stopToStation[sidB] ?? -1;
 *      if (stA === stB && stA >= 0) return 2 * 60;       // même gare → 2 min
 *      const sameOp = extractOperator(sidA) === extractOperator(sidB);
 *      return sameOp ? MIN_TRANSFER_SAME : MIN_TRANSFER_CROSS;
 *    }
 *
 * 4. buildStopsIndex() peut lire stations_meta.json au lieu de stations.json :
 *    le format est compatible — même champs { name, country, lat, lon, stopIds, operators }.
 *    Remplacer le chemin du fichier dans buildStopNameMap() et buildStopsIndex().
 */
