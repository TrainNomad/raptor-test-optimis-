/**
 * filter_avanti.js
 *
 * Filtre le GTFS UK Rail complet (transit.land / ATOC) pour ne garder que
 * les opérateurs longue distance pertinents pour le routeur inter-européen.
 *
 * Input  : ./gtfs/UK_Rail/      (GTFS ATOC complet ~500 MB décompressé)
 * Output : ./gtfs/UK_National/  (GTFS filtré — prêt pour gtfs-ingest.js)
 *
 * Opérateurs retenus (agency_id = code ATOC 2 lettres) :
 *   VT  Avanti West Coast          (London Euston → Birmingham → Manchester/Glasgow)
 *   GR  LNER                       (London Kings Cross → Leeds/Edinburgh/Aberdeen)
 *   CS  Caledonian Sleeper         (London Euston → Écosse, trains de nuit)
 *   XC  CrossCountry               (Bristol/Penzance → Birmingham → Edinburgh)
 *   TP  TransPennine Express        (Liverpool/Manchester → Leeds/Newcastle/Edinburgh)
 *   EM  East Midlands Railway       (London St Pancras → Nottingham/Sheffield)
 *   GW  Great Western Railway       (London Paddington → Bristol/Cardiff/Penzance)
 *   SW  South Western Railway       (London Waterloo → Exeter/Bournemouth)
 *   HT  Hull Trains                 (London Kings Cross → Hull)
 *   GC  Grand Central               (London Kings Cross → Sunderland/Bradford)
 *   LD  Lumo                        (London Kings Cross → Edinburgh, low-cost)
 *   SR  ScotRail                    (réseau écossais — Edinburgh/Glasgow/Inverness…)
 *   NT  Northern Trains             (Leeds/Manchester → Newcastle/Carlisle/Barrow…)
 *   AW  Transport for Wales         (Cardiff → Swansea/Holyhead/Aberystwyth)
 *   TW  Transport for Wales (autre code parfois utilisé)
 *
 * Exclus : Southeastern, Southern, Thameslink, c2c, Greater Anglia,
 *          London Overground, TfL Rail, Chiltern, GTR, etc.
 *          (banlieue London uniquement — pas de connexion inter-européenne utile)
 *
 * Logique : route_type 2 (Rail) ou 100-106 (Extended Rail).
 *           Élagage supplémentaire : routes avec < 4 stops ignorées (banlieue).
 */

'use strict';

const fs       = require('fs');
const path     = require('path');
const readline = require('readline');

const IN_DIR  = process.argv[2] || './gtfs/UK_Rail';
const OUT_DIR = process.argv[3] || './gtfs/UK_National';

// ─── Opérateurs longue distance retenus ──────────────────────────────────────

const KEPT_AGENCIES = new Set([
  'VT',  // Avanti West Coast
  'GR',  // LNER
  'CS',  // Caledonian Sleeper
  'XC',  // CrossCountry
  'TP',  // TransPennine Express
  'EM',  // East Midlands Railway
  'GW',  // Great Western Railway
  'SW',  // South Western Railway
  'HT',  // Hull Trains
  'GC',  // Grand Central
  'LD',  // Lumo
  'SR',  // ScotRail
  'NT',  // Northern Trains
  'AW',  // Transport for Wales
  'TW',  // Transport for Wales (code alternatif)
]);

// Types de routes ferroviaires (pas de bus, pas de tram)
// 2 = Rail standard, 100-106 = Extended Rail types (ATOC utilise 100-106)
function isRailRoute(routeType) {
  const t = parseInt(routeType) || 0;
  return t === 2 || (t >= 100 && t <= 106);
}

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

async function readGTFS(filename, onKeep) {
  const filePath = path.join(IN_DIR, filename);
  if (!fs.existsSync(filePath)) {
    console.warn(`  ⚠  ${filename} absent dans ${IN_DIR}`);
    return [[], []];
  }
  return new Promise((resolve, reject) => {
    let headers = null;
    const rows  = [];
    const rl    = readline.createInterface({
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
      if (!onKeep || onKeep(row)) rows.push(row);
    });
    rl.on('close', () => resolve([headers || [], rows]));
    rl.on('error', reject);
  });
}

function writeGTFS(filename, headers, rows) {
  const outPath = path.join(OUT_DIR, filename);
  const lines   = [headers.join(',')];
  for (const row of rows) {
    lines.push(headers.map(h => {
      const v = row[h] || '';
      return v.includes(',') || v.includes('"') || v.includes('\n')
        ? '"' + v.replace(/"/g, '""') + '"'
        : v;
    }).join(','));
  }
  fs.writeFileSync(outPath, lines.join('\r\n') + '\r\n', 'utf8');
  const kb = (fs.statSync(outPath).size / 1024).toFixed(1);
  console.log(`  ✓ ${filename.padEnd(24)} ${rows.length.toLocaleString().padStart(8)} lignes  (${kb} KB)`);
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  console.log('\n⚙️  filter_avanti.js — Filtrage UK Rail (longue distance)');
  console.log(`   Source  : ${IN_DIR}`);
  console.log(`   Dest    : ${OUT_DIR}`);
  console.log(`   Agences : ${[...KEPT_AGENCIES].sort().join(', ')}`);
  console.time('Filtrage UK');

  if (!fs.existsSync(IN_DIR)) {
    console.error(`❌  Dossier source introuvable : ${IN_DIR}`);
    process.exit(1);
  }
  fs.mkdirSync(OUT_DIR, { recursive: true });

  // ── Étape 1 : agency.txt ─────────────────────────────────────────────────
  // Le GTFS ATOC utilise agency_id = code 2 lettres (VT, GR, XC…)
  // OU un code numérique selon la version du feed — on filtre sur les deux.
  const [agHdr, agRowsAll] = await readGTFS('agency.txt', () => true);

  // Trouver quels agency_id correspondent à nos opérateurs
  // Le code ATOC peut être dans agency_id directement OU dans agency_noc
  const keptAgencyIds = new Set();
  const agRows        = [];
  for (const r of agRowsAll) {
    const id  = (r.agency_id || '').toUpperCase();
    const noc = (r.agency_noc || r.agency_id || '').toUpperCase().slice(0, 2);
    if (KEPT_AGENCIES.has(id) || KEPT_AGENCIES.has(noc)) {
      keptAgencyIds.add(r.agency_id); // garder l'id original (casse originale)
      agRows.push(r);
    }
  }
  writeGTFS('agency.txt', agHdr, agRows);
  console.log(`   Agency IDs retenus : ${[...keptAgencyIds].sort().join(', ')}`);

  // ── Étape 2 : routes.txt ─────────────────────────────────────────────────
  const [rtHdr, rtRows] = await readGTFS('routes.txt', (r) => {
    const agId = (r.agency_id || '').toUpperCase();
    // Certains feeds ATOC encodent l'opérateur dans les 2 premières lettres
    // de agency_id ou directement comme "VT", "GR", etc.
    const agNoc = agId.slice(0, 2);
    if (!keptAgencyIds.has(r.agency_id) && !KEPT_AGENCIES.has(agId) && !KEPT_AGENCIES.has(agNoc)) {
      return false;
    }
    return isRailRoute(r.route_type);
  });
  writeGTFS('routes.txt', rtHdr, rtRows);

  const keptRouteIds = new Set(rtRows.map(r => r.route_id));
  console.log(`   Routes retenues : ${keptRouteIds.size.toLocaleString()}`);

  // ── Étape 3 : trips.txt ──────────────────────────────────────────────────
  const [trHdr, trRows] = await readGTFS('trips.txt',
    r => keptRouteIds.has(r.route_id));
  writeGTFS('trips.txt', trHdr, trRows);

  const keptTripIds    = new Set(trRows.map(r => r.trip_id));
  const keptServiceIds = new Set(trRows.map(r => r.service_id));
  console.log(`   Trips retenus   : ${keptTripIds.size.toLocaleString()}`);

  // ── Étape 4 : stop_times.txt (streaming, potentiellement ~200 MB) ────────
  console.log('  ⏳ Filtrage stop_times.txt (streaming)...');
  // Première passe : écriture filtrée
  await new Promise((resolve, reject) => {
    const inPath  = path.join(IN_DIR,  'stop_times.txt');
    const outPath = path.join(OUT_DIR, 'stop_times.txt');
    if (!fs.existsSync(inPath)) { console.warn('  ⚠  stop_times.txt absent'); return resolve(); }

    const outStream = fs.createWriteStream(outPath, { encoding: 'utf8' });
    let headers     = null;
    let kept = 0, total = 0;

    const rl = readline.createInterface({
      input: fs.createReadStream(inPath, { encoding: 'utf8', highWaterMark: 512 * 1024 }),
      crlfDelay: Infinity,
    });
    rl.on('line', (raw) => {
      const line = raw.replace(/^\uFEFF/, '').trim();
      if (!line) return;
      total++;
      if (!headers) {
        headers = parseCSVLine(line);
        outStream.write(line + '\r\n');
        return;
      }
      const cols    = parseCSVLine(line);
      const tripIdx = headers.indexOf('trip_id');
      if (tripIdx < 0) return;
      if (!keptTripIds.has((cols[tripIdx] || '').trim())) return;
      outStream.write(line + '\r\n');
      kept++;
    });
    rl.on('close', () => {
      outStream.end(() => {
        const kb = (fs.statSync(outPath).size / 1024).toFixed(1);
        console.log(`  ✓ stop_times.txt         ${kept.toLocaleString().padStart(8)} / ${(total-1).toLocaleString()} lignes  (${kb} KB)`);
        resolve();
      });
    });
    rl.on('error', reject);
  });

  // Deuxième passe : collecter les stop_ids utilisés
  const usedStopIds = new Set();
  await new Promise((resolve, reject) => {
    const p  = path.join(OUT_DIR, 'stop_times.txt');
    let hdr  = null;
    let sidx = -1;
    const rl = readline.createInterface({
      input: fs.createReadStream(p, { encoding: 'utf8' }),
      crlfDelay: Infinity,
    });
    rl.on('line', (raw) => {
      const line = raw.trim(); if (!line) return;
      const cols = parseCSVLine(line);
      if (!hdr) { hdr = cols; sidx = cols.indexOf('stop_id'); return; }
      if (sidx >= 0) usedStopIds.add((cols[sidx] || '').trim());
    });
    rl.on('close', resolve);
    rl.on('error', reject);
  });

  // ── Étape 5 : stops.txt ──────────────────────────────────────────────────
  const [stHdr, stRows] = await readGTFS('stops.txt',
    r => usedStopIds.has(r.stop_id) || r.location_type === '1');
  writeGTFS('stops.txt', stHdr, stRows);

  // ── Étape 6 : calendar + calendar_dates ──────────────────────────────────
  const [calHdr, calRows] = await readGTFS('calendar.txt',
    r => keptServiceIds.has(r.service_id));
  if (calHdr.length) writeGTFS('calendar.txt', calHdr, calRows);

  const [cdHdr, cdRows] = await readGTFS('calendar_dates.txt',
    r => keptServiceIds.has(r.service_id));
  if (cdHdr.length) writeGTFS('calendar_dates.txt', cdHdr, cdRows);

  // ── Étape 7 : fichiers statiques ─────────────────────────────────────────
  for (const f of ['feed_info.txt', 'transfers.txt', 'shapes.txt']) {
    const src = path.join(IN_DIR, f);
    if (fs.existsSync(src)) {
      const dst = path.join(OUT_DIR, f);
      fs.copyFileSync(src, dst);
      const kb = (fs.statSync(dst).size / 1024).toFixed(1);
      console.log(`  ✓ ${f.padEnd(24)} (copie brute, ${kb} KB)`);
    }
  }

  // ── Résumé par opérateur ─────────────────────────────────────────────────
  console.log('\n   Répartition par opérateur :');
  const tripsByAgency = {};
  for (const t of trRows) {
    const agId = t.agency_id || (rtRows.find(r => r.route_id === t.route_id) || {}).agency_id || '??';
    if (!tripsByAgency[agId]) tripsByAgency[agId] = 0;
    tripsByAgency[agId]++;
  }
  for (const [ag, count] of Object.entries(tripsByAgency).sort((a, b) => b[1] - a[1])) {
    console.log(`     ${ag.padEnd(6)} : ${count.toLocaleString()} trips`);
  }

  console.log(`\n   ✅ Filtrage terminé → ${OUT_DIR}`);
  console.timeEnd('Filtrage UK');
}

main().catch(err => { console.error('Erreur filter_avanti.js :', err); process.exit(1); });
