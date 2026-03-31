/**
 * filter_germany.js
 *
 * Filtre le GTFS DB Fernverkehr (source : gtfs.de/germany/fv_free)
 * pour ne garder que les trains longue distance : ICE, IC, EC, NJ (Nightjet).
 *
 * Input  : ./gtfs/db_fv/       (GTFS brut décompressé)
 * Output : ./gtfs/db_fv_filtered/  (GTFS filtré — prêt pour gtfs-ingest.js)
 *
 * Agences retenues (agency_id dans ce feed) :
 *   4  → DB Fernverkehr AG        (ICE, IC, EC, NJ)
 *   5  → DB Fernverkehr (codeshare)
 *   7  → SNCF codeshare DB        (TGV franco-allemand)
 *   10 → SBB (Suisse)             (EC, NJ transfrontaliers)
 *   11 → ÖBB (Autriche)           (NJ, EC)
 *
 * Agences exclues :
 *   1  HZZP, 2 DSB, 3 MÁV, 6 SÜWEX, 8 NS, 9 ZSSK, 12 PKP, 13 ČD
 *   → régionaux ou opérateurs dont on a le feed natif direct
 *
 * Opération : copie les fichiers fixes (agency.txt, calendar.txt…),
 * puis filtre routes.txt → trips.txt → stop_times.txt → stops.txt
 * en cascade pour ne garder que les données liées aux routes retenues.
 */

'use strict';

const fs       = require('fs');
const path     = require('path');
const readline = require('readline');

const IN_DIR  = process.argv[2] || './gtfs/db_fv';
const OUT_DIR = process.argv[3] || './gtfs/db_fv_filtered';

// ─── Agences Fernverkehr retenues ─────────────────────────────────────────────

const KEPT_AGENCIES = new Set(['4', '5', '7', '10', '11']);

// Types de routes acceptés (GTFS route_type étendu)
// 2   = Rail (standard UE)
// 100 = Railway Service (grande vitesse, type UIC)
// 101 = High Speed Rail Service (TGV/ICE)
// 102 = Long Distance Trains (IC/EC)
// 106 = Night Rail Service (NJ)
// 107 = Car Transport Rail Service (autoroute ferroviaire)
const KEPT_ROUTE_TYPES = new Set([2, 100, 101, 102, 106, 107]);

// Noms courts à exclure explicitement (trains régionaux/S-Bahn dans le feed FV)
const EXCLUDED_SHORT_NAMES = new Set(['S', 'RE', 'RB', 'IRE', 'R', 'STR', 'U', 'Bus']);

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

/**
 * Lit un fichier GTFS CSV et retourne [headers, rows[]].
 * Filtre onKeep(row) → boolean pour ne garder que certaines lignes.
 */
async function readGTFS(filename, onKeep) {
  const filePath = path.join(IN_DIR, filename);
  if (!fs.existsSync(filePath)) {
    console.warn(`  ⚠  ${filename} absent — ignoré`);
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

/**
 * Écrit un fichier GTFS CSV (headers + rows) dans OUT_DIR.
 */
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
  console.log(`  ✓ ${filename.padEnd(22)} ${rows.length.toLocaleString().padStart(8)} lignes  (${kb} KB)`);
}

/**
 * Copie un fichier brut sans filtrage.
 */
function copyGTFS(filename) {
  const src = path.join(IN_DIR, filename);
  const dst = path.join(OUT_DIR, filename);
  if (!fs.existsSync(src)) { console.warn(`  ⚠  ${filename} absent`); return; }
  fs.copyFileSync(src, dst);
  const kb = (fs.statSync(dst).size / 1024).toFixed(1);
  console.log(`  ✓ ${filename.padEnd(22)} (copie brute, ${kb} KB)`);
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  console.log('\n⚙️  filter_germany.js — Filtrage DB Fernverkehr (ICE·IC·EC·NJ)');
  console.log(`   Source  : ${IN_DIR}`);
  console.log(`   Dest    : ${OUT_DIR}`);
  console.time('Filtrage');

  if (!fs.existsSync(IN_DIR)) {
    console.error(`❌  Dossier source introuvable : ${IN_DIR}`);
    process.exit(1);
  }
  fs.mkdirSync(OUT_DIR, { recursive: true });

  // ── Étape 1 : agency.txt — garder uniquement les agences FV ──────────────
  const [agHdr, agRows] = await readGTFS('agency.txt',
    r => KEPT_AGENCIES.has(r.agency_id));
  writeGTFS('agency.txt', agHdr, agRows);

  const keptAgencyIds = new Set(agRows.map(r => r.agency_id));

  // ── Étape 2 : routes.txt — garder FV + filtre type ───────────────────────
  const [rtHdr, rtRows] = await readGTFS('routes.txt', (r) => {
    if (!keptAgencyIds.has(r.agency_id)) return false;
    const rtype = parseInt(r.route_type) || 0;
    if (!KEPT_ROUTE_TYPES.has(rtype) && rtype !== 2) {
      // Accepter aussi rtype 2 générique (certaines EC sont mal typées)
      if (rtype < 100 || rtype > 199) return false;
    }
    const shortName = (r.route_short_name || '').trim();
    if (EXCLUDED_SHORT_NAMES.has(shortName)) return false;
    return true;
  });
  writeGTFS('routes.txt', rtHdr, rtRows);

  const keptRouteIds = new Set(rtRows.map(r => r.route_id));
  console.log(`   Routes retenues : ${keptRouteIds.size.toLocaleString()}`);

  // ── Étape 3 : trips.txt — garder les trips des routes retenues ────────────
  const [trHdr, trRows] = await readGTFS('trips.txt',
    r => keptRouteIds.has(r.route_id));
  writeGTFS('trips.txt', trHdr, trRows);

  const keptTripIds    = new Set(trRows.map(r => r.trip_id));
  const keptServiceIds = new Set(trRows.map(r => r.service_id));
  console.log(`   Trips retenus   : ${keptTripIds.size.toLocaleString()}`);

  // ── Étape 4 : stop_times.txt — fichier potentiellement très gros (~GB) ────
  // On utilise un stream direct pour éviter de tout charger en mémoire.
  console.log('  ⏳ Filtrage stop_times.txt (streaming)...');
  await new Promise((resolve, reject) => {
    const inPath  = path.join(IN_DIR,  'stop_times.txt');
    const outPath = path.join(OUT_DIR, 'stop_times.txt');
    if (!fs.existsSync(inPath)) { console.warn('  ⚠  stop_times.txt absent'); return resolve(); }

    const outStream = fs.createWriteStream(outPath, { encoding: 'utf8' });
    let headers = null;
    let kept = 0;
    let total = 0;

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
      const tripId  = (cols[tripIdx] || '').trim();
      if (!keptTripIds.has(tripId)) return;
      outStream.write(line + '\r\n');
      kept++;
    });
    rl.on('close', () => {
      outStream.end(() => {
        const kb = (fs.statSync(outPath).size / 1024).toFixed(1);
        console.log(`  ✓ stop_times.txt       ${kept.toLocaleString().padStart(8)} / ${(total-1).toLocaleString()} lignes  (${kb} KB)`);
        resolve();
      });
    });
    rl.on('error', reject);
  });

  // Collecter les stop_ids utilisés
  const usedStopIds = new Set();
  await new Promise((resolve, reject) => {
    const p = path.join(OUT_DIR, 'stop_times.txt');
    let headers = null;
    const rl = readline.createInterface({
      input: fs.createReadStream(p, { encoding: 'utf8' }),
      crlfDelay: Infinity,
    });
    rl.on('line', (raw) => {
      const line = raw.trim();
      if (!line) return;
      const cols = parseCSVLine(line);
      if (!headers) { headers = cols; return; }
      const idx = headers.indexOf('stop_id');
      if (idx >= 0) usedStopIds.add((cols[idx] || '').trim());
    });
    rl.on('close', resolve);
    rl.on('error', reject);
  });

  // ── Étape 5 : stops.txt — uniquement les stops utilisés ──────────────────
  const [stHdr, stRows] = await readGTFS('stops.txt',
    r => usedStopIds.has(r.stop_id) || r.location_type === '1');
  writeGTFS('stops.txt', stHdr, stRows);

  // ── Étape 6 : calendar.txt et calendar_dates.txt (filtrés par service_id) ─
  const [calHdr, calRows] = await readGTFS('calendar.txt',
    r => keptServiceIds.has(r.service_id));
  writeGTFS('calendar.txt', calHdr, calRows);

  const [cdHdr, cdRows] = await readGTFS('calendar_dates.txt',
    r => keptServiceIds.has(r.service_id));
  writeGTFS('calendar_dates.txt', cdHdr, cdRows);

  // ── Étape 7 : fichiers fixes (pas besoin de filtrer) ──────────────────────
  for (const f of ['feed_info.txt', 'transfers.txt']) {
    if (fs.existsSync(path.join(IN_DIR, f))) copyGTFS(f);
  }

  console.log(`\n   ✅ Filtrage terminé → ${OUT_DIR}`);
  console.timeEnd('Filtrage');
}

main().catch(err => { console.error('Erreur filter_germany.js :', err); process.exit(1); });
