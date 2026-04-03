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

const OPS_FILE   = process.argv[2] || './operators.json';
const DATA_DIR   = process.argv[3] || './engine_data';
const CSV_FILE   = process.argv[4] || path.join(__dirname, 'stations.csv');
const STOPS_FILE = path.join(DATA_DIR, 'stops.json');

// ─── Helpers CSV ──────────────────────────────────────────────────────────────

function parseCSVLine(line) {
  const result = [];
  let cur = '';
  let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') {
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

function streamCSV(filePath, onRow) {
  const readline = require('readline');
  return new Promise((resolve, reject) => {
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
      onRow(row);
    });
    rl.on('close', resolve);
    rl.on('error', reject);
  });
}

function extractOperator(sid) {
  const m = (sid || '').match(/^([A-Z0-9_]+):/);
  return m ? m[1] : 'UNKNOWN';
}

// ─── Main async ───────────────────────────────────────────────────────────────

async function main() {

  // ── Validation des entrées ──────────────────────────────────────────────────
  for (const [label, p] of [['stops.json', STOPS_FILE], ['stations.csv', CSV_FILE]]) {
    if (!fs.existsSync(p)) {
      console.error(`❌  ${label} introuvable : ${p}`);
      if (label === 'stops.json') console.error('   Lance d\'abord : node gtfs-ingest.js');
      process.exit(1);
    }
  }

  console.log('\n🚉 ingest_v2.js — Prétraitement Station-Based RAPTOR');
  console.log('═'.repeat(52));
  console.time('Total');

  // ── ① Chargement stops.json ─────────────────────────────────────────────────
  console.log('\n① Chargement stops.json...');
  const stops = JSON.parse(fs.readFileSync(STOPS_FILE, 'utf8'));
  const totalStops = Object.keys(stops).length;
  console.log(`   ${totalStops.toLocaleString()} stops chargés`);

  // ── ② Construction des index stops par opérateur ────────────────────────────
  console.log('\n② Construction des index stops par opérateur...');

  const uic8ToStops  = new Map();
  const atocToStops  = new Map();
  const renfeToStops = new Map();
  const dbToStops    = new Map();
  const cpToStops    = new Map();

  for (const [sid, stop] of Object.entries(stops)) {
    const op = stop.operator || extractOperator(sid);

    if (op === 'SNCF') {
      const m = sid.match(/-(\d{7,8})$/) || sid.match(/OC[A-Z](\d{7,8})$/);
      if (m) {
        if (!uic8ToStops.has(m[1])) uic8ToStops.set(m[1], []);
        uic8ToStops.get(m[1]).push(sid);
      }
    } else if (op === 'TI' || op === 'TI_AV') {
      const m = sid.match(/^(?:TI|TI_AV):(\d+)$/);
      if (m) {
        if (!uic8ToStops.has(m[1])) uic8ToStops.set(m[1], []);
        uic8ToStops.get(m[1]).push(sid);
      }
    } else if (op === 'SNCB') {
      const m = sid.match(/^SNCB:(\d+)$/);
      if (m) {
        if (!uic8ToStops.has(m[1])) uic8ToStops.set(m[1], []);
        uic8ToStops.get(m[1]).push(sid);
      }
    } else if (op === 'EU_SLEEPER') {
      const m = sid.match(/(\d{7,8})$/);
      if (m) {
        if (!uic8ToStops.has(m[1])) uic8ToStops.set(m[1], []);
        uic8ToStops.get(m[1]).push(sid);
      }
    } else if (op === 'UK' && stop.code) {
      const crs = stop.code.toUpperCase();
      if (!atocToStops.has(crs)) atocToStops.set(crs, []);
      atocToStops.get(crs).push(sid);
    } else if (op === 'RENFE' || op === 'OUIGO_ES') {
      const m = sid.match(/^(?:RENFE|OUIGO_ES):(\d+)$/);
      if (m) {
        if (!renfeToStops.has(m[1])) renfeToStops.set(m[1], []);
        renfeToStops.get(m[1]).push(sid);
      }
    } else if (op === 'DB_FV' || op === 'DB') {
      const m = sid.match(/^(?:DB_FV|DB):(\d+)$/);
      if (m) {
        if (!dbToStops.has(m[1])) dbToStops.set(m[1], []);
        dbToStops.get(m[1]).push(sid);
      }
    } else if (op === 'CP') {
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

  // ── ③ Parcours de stations.csv ──────────────────────────────────────────────
  console.log('\n③ Parcours de stations.csv → construction GlobalStations...');
  console.log('   Lecture CSV en mémoire...');

  const csvRows = [];
  await streamCSV(CSV_FILE, (row) => {
    const lat = parseFloat(row.latitude);
    const lon = parseFloat(row.longitude);
    if (isNaN(lat) || isNaN(lon) || (lat === 0 && lon === 0)) return;
    csvRows.push(row);
  });
  console.log(`   ${csvRows.length.toLocaleString()} lignes CSV valides (avec coordonnées GPS)`);

  // ── resolveStopIds ──────────────────────────────────────────────────────────
  function resolveStopIds(row) {
    const found = new Set();

    for (const uicCol of ['uic8_sncf', 'uic']) {
      const raw = row[uicCol];
      if (raw && raw.length >= 7) {
        for (const key of [raw.padStart(8, '0'), raw.padStart(7, '0'), raw]) {
          for (const sid of (uic8ToStops.get(key) || [])) found.add(sid);
        }
      }
    }

    const tiId = row['trenitalia_id'];
    if (tiId) {
      for (const sid of (uic8ToStops.get(tiId) || [])) found.add(sid);
      if (stops[`TI:${tiId}`])    found.add(`TI:${tiId}`);
      if (stops[`TI_AV:${tiId}`]) found.add(`TI_AV:${tiId}`);
    }

    const atocId = row['atoc_id'];
    if (atocId) {
      for (const sid of (atocToStops.get(atocId.toUpperCase()) || [])) found.add(sid);
    }

    const dbId = row['db_id'];
    if (dbId) {
      for (const sid of (dbToStops.get(dbId) || [])) found.add(sid);
    }

    const renfeId = row['renfe_id'];
    if (renfeId) {
      for (const sid of (renfeToStops.get(renfeId) || [])) found.add(sid);
    }

    const uicRaw = row['uic'];
    if (uicRaw && uicRaw.startsWith('94')) {
      for (const sid of (cpToStops.get(uicRaw.padStart(7, '0')) || [])) found.add(sid);
    }

    return [...found];
  }

  const globalStations    = [];
  const csvIdToStationIdx = new Map();
  const stationStopIds    = [];
  let stationCount = 0;
  const rowById = new Map();

  for (const row of csvRows) {
    rowById.set(row.id, row);

    const stopIds       = resolveStopIds(row);
    const isSuggestable = row.is_suggestable === 't';
    const isCity        = row.is_city === 't';

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

  // ── stopToStation ───────────────────────────────────────────────────────────
  const stopToStation = new Map();

  for (let idx = 0; idx < globalStations.length; idx++) {
    const st   = globalStations[idx];
    const sids = stationStopIds[idx];
    for (const sid of sids) {
      if (!stopToStation.has(sid)) {
        stopToStation.set(sid, idx);
      } else {
        const existing = globalStations[stopToStation.get(sid)];
        if (st.is_main && !existing.is_main) stopToStation.set(sid, idx);
      }
      const op = stops[sid]?.operator || extractOperator(sid);
      if (op) st.operators.add(op);
    }
  }

  // Stations synthétiques pour stops sans CSV
  let synthetic = 0;
  for (const [sid, stop] of Object.entries(stops)) {
    if (stopToStation.has(sid)) continue;
    const lat = stop.lat || 0;
    const lon = stop.lon || 0;
    if (!lat && !lon) continue;
    const op  = stop.operator || extractOperator(sid);
    const idx = globalStations.length;
    globalStations.push({
      csv_id: null, name: stop.name || sid, slug: null,
      country: 'XX', lat, lon,
      is_city: false, is_main: false, is_suggestable: false,
      parent_csv_id: null, operators: new Set([op]),
    });
    stationStopIds.push(new Set([sid]));
    stopToStation.set(sid, idx);
    synthetic++;
  }

  console.log(`   ${stopToStation.size.toLocaleString()} stops mappés (dont ${synthetic.toLocaleString()} stations synthétiques)`);
  console.log(`   ${globalStations.length.toLocaleString()} stations au total`);

  // ── ④ Génération des sorties ────────────────────────────────────────────────
  console.log('\n④ Génération des sorties...');

  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

  // stations.bin
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

  // stop_to_station.json
  {
    const obj = {};
    for (const [sid, idx] of stopToStation) obj[sid] = idx;
    const outPath = path.join(DATA_DIR, 'stop_to_station.json');
    fs.writeFileSync(outPath, JSON.stringify(obj));
    const kb = (fs.statSync(outPath).size / 1024).toFixed(1);
    console.log(`   ✓ stop_to_station.json   ${kb} KB  (${stopToStation.size.toLocaleString()} entrées)`);
  }

  // stations_meta.json
  {
    const meta = globalStations.map((st, idx) => ({
      id:          idx,
      csv_id:      st.csv_id,
      name:        st.name,
      slug:        st.slug,
      country:     st.country,
      lat:         st.lat,
      lon:         st.lon,
      is_city:     st.is_city,
      is_main:     st.is_main,
      suggestable: st.is_suggestable,
      operators:   [...st.operators].sort(),
      stopIds:     [...stationStopIds[idx]],
      parent:      st.parent_csv_id ? csvIdToStationIdx.get(st.parent_csv_id) ?? null : null,
    }));
    const outPath = path.join(DATA_DIR, 'stations_meta.json');
    fs.writeFileSync(outPath, JSON.stringify(meta));
    const mb = (fs.statSync(outPath).size / 1024 / 1024).toFixed(2);
    console.log(`   ✓ stations_meta.json     ${mb} MB  (${meta.length.toLocaleString()} stations)`);
  }

  // ── Résumé ──────────────────────────────────────────────────────────────────
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
}

main().catch(err => { console.error('❌ Erreur ingest_v2.js :', err); process.exit(1); });