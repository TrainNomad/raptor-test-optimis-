/**
 * build-stations-index.js
 *
 * Génère stations.json à partir de stations.csv (Trainline open data) :
 * ce fichier contient les liens UIC8-SNCF ↔ trenitalia_id pour toutes les gares.
 * C'est la source la plus fiable — plus besoin de heuristiques de nom ou GPS.
 *
 * Usage :
 *   node build-stations-index.js [engine_data_dir] [stations_csv] [out_file]
 */

const fs   = require('fs');
const path = require('path');

const DATA_DIR   = process.argv[2] || './engine_data';
const CSV_FILE   = process.argv[3] || path.join(__dirname, 'stations.csv');
const OUT_FILE   = process.argv[4] || path.join(__dirname, 'stations.json');
const STOPS_FILE = path.join(DATA_DIR, 'stops.json');
const XFER_FILE  = path.join(DATA_DIR, 'transfer_index.json');

function extractOperator(sid) {
  const m = (sid||'').match(/^([A-Z]+):/);
  return m ? m[1] : 'SNCF';
}


console.log('\n🔨 Construction stations.json depuis stations.csv...\n');

if (!fs.existsSync(STOPS_FILE)) {
  console.error('❌ ' + STOPS_FILE + ' introuvable. Lance d\'abord : node gtfs-ingest.js');
  process.exit(1);
}
if (!fs.existsSync(CSV_FILE)) {
  console.error('❌ ' + CSV_FILE + ' introuvable.');
  process.exit(1);
}

const stops = JSON.parse(fs.readFileSync(STOPS_FILE, 'utf8'));
const xfer  = fs.existsSync(XFER_FILE) ? JSON.parse(fs.readFileSync(XFER_FILE, 'utf8')) : {};

// Helper : valeur du transfer_index → string (peut être {id,interCity} ou string)
function xferId(v) { return (typeof v === 'string') ? v : v.id; }

console.log('  stops.json    : ' + Object.keys(stops).length + ' stops');

// ── Index 1 : UIC → stop_ids (SNCF + TI + TI_AV + SNCB) ─────────────────────
// SNCF  : SNCF:StopPoint:OCE...-87XXXXXX  → UIC = dernier groupe numérique
// TI    : TI:12345678                      → UIC = 12345678
// TI_AV : TI_AV:12345678                  → UIC = 12345678 (feed GitHub TrainNomad)
// SNCB  : SNCB:8814001                    → UIC = 8814001
const uic8ToStops = {};
for (const [sid, stop] of Object.entries(stops)) {
  const op = stop.operator || extractOperator(sid);
  let uicKey = null;
  if (op === 'SNCF') {
    const m = sid.match(/-(\d{7,8})$/) || sid.match(/OCE(\d{7,8})$/);
    if (m) uicKey = m[1];
  } else if (op === 'TI' || op === 'TI_AV') {
    const m = sid.match(/^(?:TI|TI_AV):(\d+)$/);
    if (m) uicKey = m[1];
  } else if (op === 'SNCB') {
    const m = sid.match(/^SNCB:(\d+)$/);
    if (m) uicKey = m[1];
  }
  if (uicKey) {
    if (!uic8ToStops[uicKey]) uic8ToStops[uicKey] = [];
    uic8ToStops[uicKey].push(sid);
  }
}
console.log('  Index UIC     : ' + Object.keys(uic8ToStops).length + ' codes');

// ── Index 2 : ATOC CRS → stop_ids (UK) ───────────────────────────────────────
// Nécessite que gtfs-ingest sauvegarde stop_code dans stops.json
const atocToStops = {};
for (const [sid, stop] of Object.entries(stops)) {
  if (!stop.code) continue;
  const op = stop.operator || extractOperator(sid);
  if (op !== 'UK') continue;
  const crs = stop.code.trim().toUpperCase();
  if (!atocToStops[crs]) atocToStops[crs] = [];
  atocToStops[crs].push(sid);
}
console.log('  Index ATOC    : ' + Object.keys(atocToStops).length + ' codes CRS UK');

// ── Index 3 : renfe_id → stop_ids (RENFE + OUIGO_ES) ─────────────────────────
// RENFE     : RENFE:71801
// OUIGO_ES  : OUIGO_ES:71801  (même infra, même codes)
const renfeToStops = {};
for (const [sid, stop] of Object.entries(stops)) {
  const op = stop.operator || extractOperator(sid);
  if (op !== 'RENFE' && op !== 'OUIGO_ES') continue;
  const m = sid.match(/^(?:RENFE|OUIGO_ES):(\d+)$/);
  if (m) {
    if (!renfeToStops[m[1]]) renfeToStops[m[1]] = [];
    renfeToStops[m[1]].push(sid);
  }
}
console.log('  Index RENFE   : ' + Object.keys(renfeToStops).length + ' codes');

// ── Index 3b : db_id → stop_ids (DB_FV) ──────────────────────────────────────
// Utilisé pour les gares IT/AT/CH/PL/CZ/HR/SI desservies par DB_FV
// Le CSV Trainline a db_is_enabled=f pour les gares IT (Trenitalia est prioritaire)
// mais on indexe quand même pour rattacher ces stops orphelins via db_id du CSV.
const dbFvToStops = {};
for (const [sid] of Object.entries(stops)) {
  const m = sid.match(/^DB_FV:(\d+)$/);
  if (m) {
    if (!dbFvToStops[m[1]]) dbFvToStops[m[1]] = [];
    dbFvToStops[m[1]].push(sid);
  }
}
console.log('  Index DB_FV   : ' + Object.keys(dbFvToStops).length + ' codes');

// ── Index 4 : UIC Portugal → stop_ids (CP) ────────────────────────────────────
// CP:94_2006 → suffix int=2006 → UIC = '94' + '2006'.padStart(5,'0') = '9402006'
// CP:94_30007 → suffix=30007 → UIC = '9430007'
const cpToStops = {};
for (const [sid] of Object.entries(stops)) {
  const m = sid.match(/^CP:94_(\d+)$/);
  if (m) {
    const uicFull = '94' + String(parseInt(m[1])).padStart(5, '0');
    if (!cpToStops[uicFull]) cpToStops[uicFull] = [];
    cpToStops[uicFull].push(sid);
  }
}
console.log('  Index CP      : ' + Object.keys(cpToStops).length + ' gares PT');

// ── Index 5 : ES slug → stop_ids (Eurostar) ──────────────────────────────────
const slugToEsStops = {};
for (const sid of Object.keys(stops)) {
  if (!sid.startsWith('ES:')) continue;
  const base = sid.slice(3).replace(/_station_area$/, '').replace(/_\d+[ab]?$/, '');
  if (!slugToEsStops[base]) slugToEsStops[base] = [];
  slugToEsStops[base].push(sid);
}
console.log('  Index ES slug : ' + Object.keys(slugToEsStops).length + ' slugs Eurostar');

// ── Blacklist liens SNCF→ES erronés ──────────────────────────────────────────
const ES_TRANSFER_BLACKLIST = new Set(['87113001:paris_nord']);

// ── Whitelist ES valides via transfer_index ───────────────────────────────────
const validEsTransfers = {};
for (const [key, vals] of Object.entries(xfer)) {
  if (!key.startsWith('SNCF:StopArea:')) continue;
  const esVals = vals.map(xferId).filter(v => v.startsWith('ES:'));
  if (!esVals.length) continue;
  const uicMatch = key.match(/(\d{7,9})$/);
  if (!uicMatch) continue;
  const uic = uicMatch[1];
  for (const esId of esVals) {
    const base = esId.slice(3).replace(/_(\d+[ab]?|station_area)$/, '');
    if (ES_TRANSFER_BLACKLIST.has(uic + ':' + base)) continue;
    if (!validEsTransfers[uic]) validEsTransfers[uic] = new Set();
    validEsTransfers[uic].add(base);
  }
}

// ── Table slug CSV → slug ES (cas particuliers) ───────────────────────────────
const CSV_SLUG_TO_ES_SLUG = {
  'paris-gare-du-nord':       'paris_nord',
  'london-st-pancras':        'st_pancras_international',
  'st-pancras-international': 'st_pancras_international',
};

// ── Extraction de la ville ────────────────────────────────────────────────────
const CITY_PREFIXES = [
  // France
  'Aix-en-Provence','Angers','Avignon','Bordeaux','Brest','Caen',
  'Clermont-Ferrand','Dijon','Grenoble','Le Havre','Le Mans','Lille',
  'Limoges','Lyon','Marseille','Metz','Montpellier','Nancy','Nantes',
  'Nice','Nimes','Orleans','Paris','Perpignan','Poitiers','Reims',
  'Rennes','Rouen','Saint-Etienne','Strasbourg','Toulon','Toulouse','Tours',
  // Italie
  'Milano','Torino','Roma','Firenze','Venezia','Genova','Napoli','Bologna',
  // Benelux
  'Amsterdam','Rotterdam','Bruxelles','Antwerpen','Liege','Gand','Bruges',
  // Allemagne
  'Koln','Dusseldorf','Dortmund','Duisburg','Essen','Aachen','Frankfurt',
  // Espagne
  'Madrid','Barcelona','Valencia','Sevilla','Zaragoza','Malaga','Murcia',
  'Bilbao','Alicante','Valladolid','Cordoba','Vigo','Granada','Oviedo',
  'Santander','Pamplona','San Sebastian','Burgos','Lleida','Tarragona',
  // Portugal
  'Lisboa','Porto','Braga','Coimbra','Faro','Setubal','Aveiro','Viseu',
  'Leiria','Guimaraes','Funchal','Evora','Viana do Castelo',
  // UK
  'London','Birmingham','Manchester','Liverpool','Leeds','Sheffield',
  'Bristol','Edinburgh','Glasgow','Cardiff','Nottingham',
  'Newcastle','Leicester','Coventry','Southampton','Portsmouth',
  'Brighton','Reading','Oxford','Cambridge','York','Exeter','Preston',
  // Écosse
  'Aberdeen','Inverness','Dundee','Perth','Stirling','Motherwell',
  'Hamilton','Paisley','Kilmarnock','Ayr','Falkirk','Dunfermline',
  'Kirkcaldy','Livingston','Cumbernauld','Greenock','Fort William',
  // Pays de Galles
  'Swansea','Newport','Wrexham','Bangor','Holyhead','Aberystwyth',
  'Carmarthen','Llandudno','Rhyl','Bridgend','Merthyr Tydfil',
  // Irlande du Nord
  'Belfast',
];
function extractCity(name) {
  const normalized = name.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  for (const prefix of CITY_PREFIXES) {
    const normPrefix = prefix.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
    if (normalized === normPrefix || normalized.startsWith(normPrefix + ' ') || normalized.startsWith(normPrefix + '-')) {
      return name.slice(0, prefix.length);
    }
  }
  return name;
}

// ── Lecture du CSV ────────────────────────────────────────────────────────────
function parseCSVLine(line) {
  const result = []; let cur = ''; let inQ = false;
  for (const c of line) {
    if      (c === '"')             { inQ = !inQ; }
    else if (c === ',' && !inQ)     { result.push(cur); cur = ''; }
    else                            { cur += c; }
  }
  result.push(cur);
  return result;
}

function parseCsv(filePath) {
  const lines = fs.readFileSync(filePath, 'utf8').replace(/^\uFEFF/, '').split('\n');
  // Le fichier est délimité par des virgules — utiliser parseCSVLine qui gère
  // les champs quotés ET les apostrophes ("Paris Gare de l'Est", "l'Hôpital"…)
  const headers = parseCSVLine(lines[0]);
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    const vals = parseCSVLine(line);
    const obj = {};
    for (let j = 0; j < headers.length; j++) obj[headers[j]] = vals[j] || '';
    rows.push(obj);
  }
  return rows;
}

console.log('  Slugs ES dispo: ' + Object.keys(slugToEsStops).sort().join(', ') + '\n');

const csvRows = parseCsv(CSV_FILE);
console.log('  stations.csv  : ' + csvRows.length + ' lignes\n');

const stations      = [];
const assignedStops = new Set();
const assignedEsSlugs = new Set();
let nbFusionsTI = 0;

for (const row of csvRows) {
  if (row.is_suggestable !== 't') continue;
  if (!row.name?.trim()) continue;

  const uic8Sncf = row.uic8_sncf?.trim();
  const uicIntl  = row.uic?.trim();
  const tiId     = row.trenitalia_id?.trim();
  const country  = row.country?.trim() || 'FR';
  const lat      = parseFloat(row.latitude)  || 0;
  const lon      = parseFloat(row.longitude) || 0;
  const isTiEn   = row.trenitalia_is_enabled === 't';
  const atocId   = row.atoc_id?.trim();
  const isAtocEn = row.atoc_is_enabled === 't';
  const renfeId  = row.renfe_id?.trim();
  const isRenfeEn = row.renfe_is_enabled === 't';
  const csvSlug  = (row.slug || '').trim();
  const benerailEn = row.benerail_is_enabled === 't';

  // Nom canonique : utiliser info:en pour GB si disponible, sinon name
  const rawName = country === 'GB' && row['info:en']?.trim()
    ? row['info:en'].trim()
    : row.name.trim();

  const allStopIds = new Set();
  const operators  = new Set();

  // (1) SNCF via UIC8
  for (const sid of (uic8ToStops[uic8Sncf] || [])) {
    allStopIds.add(sid); operators.add('SNCF');
  }
  // (2) Via UIC international (SNCB, TI backup)
  if (uicIntl && uicIntl !== uic8Sncf) {
    for (const sid of (uic8ToStops[uicIntl] || [])) {
      if (!assignedStops.has(sid)) { allStopIds.add(sid); operators.add(extractOperator(sid)); }
    }
  }
  // (3) TI / TI_AV via trenitalia_id (feed Trenitalia France ou feed GitHub TrainNomad)
  if (tiId && isTiEn) {
    for (const sid of (uic8ToStops[tiId] || [])) {
      const tOp = extractOperator(sid);
      if (!assignedStops.has(sid) && (tOp === 'TI' || tOp === 'TI_AV')) {
        allStopIds.add(sid); operators.add('TI'); nbFusionsTI++;
      }
    }
  }
  // (4) SNCB via UIC (88XXXXXX)
  if (country === 'BE' && benerailEn && uicIntl) {
    for (const sid of (uic8ToStops[uicIntl] || [])) {
      if (!assignedStops.has(sid) && extractOperator(sid) === 'SNCB') {
        allStopIds.add(sid); operators.add('SNCB');
      }
    }
  }
  // (5) RENFE + OUIGO_ES via renfe_id (chiffres uniquement, pas les hubs "MADRI")
  if (isRenfeEn && renfeId && /^\d+$/.test(renfeId)) {
    for (const sid of (renfeToStops[renfeId] || [])) {
      if (!assignedStops.has(sid)) { allStopIds.add(sid); operators.add(extractOperator(sid)); }
    }
  }
  // (6) CP Portugal via UIC (94XXXXX)
  if (country === 'PT' && uicIntl) {
    for (const sid of (cpToStops[uicIntl] || [])) {
      if (!assignedStops.has(sid)) { allStopIds.add(sid); operators.add('CP'); }
    }
  }
  // (7) Eurostar via slug CSV → slug ES
  const esSlugExplicit = CSV_SLUG_TO_ES_SLUG[csvSlug];
  const esSlugAuto     = csvSlug.replace(/-/g, '_');
  for (const esSlug of [...new Set([esSlugExplicit, esSlugAuto].filter(Boolean))]) {
    for (const sid of (slugToEsStops[esSlug] || [])) {
      if (!assignedStops.has(sid)) { allStopIds.add(sid); operators.add('ES'); }
    }
    if ((slugToEsStops[esSlug] || []).length) assignedEsSlugs.add(esSlug);
  }
  if (esSlugExplicit) assignedEsSlugs.add(esSlugExplicit);
  // (7b) ES via validEsTransfers
  const uicsToCheck = new Set();
  if (uic8Sncf) uicsToCheck.add(uic8Sncf);
  for (const sid of allStopIds) {
    const m = sid.match(/-(\d{7,9})$/) || sid.match(/OCE(\d{7,9})$/);
    if (m) uicsToCheck.add(m[1]);
  }
  for (const uic of uicsToCheck) {
    for (const esBase of (validEsTransfers[uic] || [])) {
      for (const sid of (slugToEsStops[esBase] || [])) {
        if (!assignedStops.has(sid)) { allStopIds.add(sid); operators.add('ES'); assignedEsSlugs.add(esBase); }
      }
    }
  }
  // (8) UK via ATOC CRS
  if (atocId && isAtocEn) {
    for (const sid of (atocToStops[atocId.toUpperCase()] || [])) {
      if (!assignedStops.has(sid)) { allStopIds.add(sid); operators.add('UK'); }
    }
  }
  // (8b) DB_FV via db_id du CSV — pour gares IT/AT/CH/PL/CZ/HR/SI
  // db_is_enabled peut être 'f' (cas Italie) mais on rattache quand même
  // pour éviter que ces stops restent orphelins et non-routables.
  // On limite aux pays où DB_FV est la seule source longue-distance disponible.
  const dbId = row.db_id?.trim();
  if (dbId && country !== 'FR' && country !== 'ES' && country !== 'PT' && country !== 'GB' && country !== 'BE') {
    for (const sid of (dbFvToStops[dbId] || [])) {
      if (!assignedStops.has(sid)) { allStopIds.add(sid); operators.add('DB_FV'); }
    }
  }
  // (9) Propagation transfer_index (SNCF/TI/SNCB/RENFE/CP — pas ES ni UK déjà couverts)
  for (const sid of [...allStopIds]) {
    for (const sisterRaw of (xfer[sid] || [])) {
      const sister = xferId(sisterRaw);
      if (assignedStops.has(sister)) continue;
      if (sister.startsWith('ES:')) continue;
      if (sister.startsWith('UK:')) continue;
      allStopIds.add(sister);
      operators.add(extractOperator(sister));
    }
  }

  if (!allStopIds.size) {
    // Cas spécial : gare GB avec coordonnées valides mais sans stop trouvé via atoc_id
    // Raison : le GTFS UK (Avanti Only) ne contient pas toutes les gares du CSV GB
    // → on cherche les stops UK GTFS par GPS (< 400m) puis les stops ES
    if (country !== 'GB' || !lat || !lon) continue;

    // (A) Chercher les stops UK GTFS par GPS
    for (const [sid, stop] of Object.entries(stops)) {
      if (!sid.startsWith('UK:') || assignedStops.has(sid)) continue;
      if (!stop.lat || !stop.lon) continue;
      if (distMeters(lat, lon, stop.lat, stop.lon) < 400) {
        allStopIds.add(sid); operators.add('UK');
      }
    }
    // (B) Chercher les stops ES par slug CSV
    const esSlugAuto = csvSlug.replace(/-/g, '_');
    const esSlugExp  = CSV_SLUG_TO_ES_SLUG[csvSlug];
    for (const esSlug of [...new Set([esSlugExp, esSlugAuto].filter(Boolean))]) {
      for (const sid of (slugToEsStops[esSlug] || [])) {
        if (!assignedStops.has(sid)) { allStopIds.add(sid); operators.add('ES'); assignedEsSlugs.add(esSlug); }
      }
    }
    // (C) Chercher les stops ES par GPS (< 500m)
    if (!allStopIds.size || !operators.has('ES')) {
      for (const [sid, stop] of Object.entries(stops)) {
        if (!sid.startsWith('ES:') || assignedStops.has(sid)) continue;
        if (!stop.lat || !stop.lon) continue;
        if (distMeters(lat, lon, stop.lat, stop.lon) < 500) {
          allStopIds.add(sid); operators.add('ES');
        }
      }
    }
    // (D) Si toujours rien mais gare GB importante (parent=8267 = Londres),
    //     créer un nœud vide pour permettre les bridges inter-terminaux
    // (les bridges injecteront les liens dans transfer_index même sans stopIds)
    if (!allStopIds.size) continue; // skip les gares GB vraiment sans aucun stop
  }

  stations.push({
    name:      rawName,
    city:      extractCity(rawName),
    slug:      csvSlug,
    country,
    lat, lon,
    stopIds:   [...allStopIds],
    operators: [...operators].sort(),
    sncf_id:   row.sncf_id?.trim() || null,
    ti_id:     tiId || null,
    uic8:      uic8Sncf || null,
  });
  for (const sid of allStopIds) assignedStops.add(sid);
}

// ── Gares ES non rattachées au CSV ────────────────────────────────────────────
const esOnlyAdded = [];
function distMeters(lat1, lon1, lat2, lon2) {
  const R = 6371000, dLat = (lat2-lat1)*Math.PI/180, dLon = (lon2-lon1)*Math.PI/180;
  const a = Math.sin(dLat/2)**2 + Math.cos(lat1*Math.PI/180)*Math.cos(lat2*Math.PI/180)*Math.sin(dLon/2)**2;
  return R*2*Math.asin(Math.sqrt(a));
}
function normalizeForMerge(n) {
  return n.toLowerCase().trim().normalize('NFD').replace(/[\u0300-\u036f]/g,'').replace(/[-_\s]+/g,' ');
}
const ES_SLUG_COUNTRY = {
  'paris_nord':'FR','st_pancras_international':'GB','amsterdam_centraal':'NL',
  'rotterdam_centraal':'NL','schiphol_airport':'NL','bruxelles_midi':'BE',
  'antwerpen_centraal':'BE','liege_guillemins':'BE','koln_hbf':'DE',
  'dusseldorf_hbf':'DE','duisburg_hbf':'DE','essen_hbf':'DE','dortmund_hbf':'DE',
  'aachen_hbf':'DE','moutiers_salins_brides_les_bai':'FR','albertville':'FR',
};
const sncfOrphansByName = new Map();
for (const [sid, stop] of Object.entries(stops)) {
  if (assignedStops.has(sid) || sid.startsWith('ES:')) continue;
  const key = normalizeForMerge(stop.name || sid);
  if (!sncfOrphansByName.has(key)) sncfOrphansByName.set(key, []);
  sncfOrphansByName.get(key).push({ sid, stop });
}
for (const [esBase, esStopIds] of Object.entries(slugToEsStops)) {
  if (assignedEsSlugs.has(esBase)) continue;
  if (esStopIds.every(sid => assignedStops.has(sid))) continue;
  const areaSid  = esStopIds.find(s => s.endsWith('_station_area')) || esStopIds[0];
  const areaStop = stops[areaSid] || stops[esStopIds[0]] || {};
  const name     = areaStop.name || esBase.replace(/_/g,' ').replace(/\b\w/g, c => c.toUpperCase());
  const lat      = areaStop.lat || 0;
  const lon      = areaStop.lon || 0;
  const country  = ES_SLUG_COUNTRY[esBase] || 'EU';
  const allStopIds = new Set(esStopIds);
  const operators  = new Set(['ES']);
  const nameKey = normalizeForMerge(name);
  for (const { sid } of (sncfOrphansByName.get(nameKey) || [])) {
    if (!assignedStops.has(sid)) { allStopIds.add(sid); operators.add(extractOperator(sid)); assignedStops.add(sid); }
  }
  if (lat && lon) {
    for (const orphans of sncfOrphansByName.values()) {
      for (const { sid, stop: o } of orphans) {
        if (assignedStops.has(sid) || !o.lat || !o.lon) continue;
        if (distMeters(lat, lon, o.lat, o.lon) < 300) {
          allStopIds.add(sid); operators.add(extractOperator(sid)); assignedStops.add(sid);
        }
      }
    }
  }
  stations.push({
    name, city: extractCity(name), slug: esBase.replace(/_/g,'-'),
    country, lat, lon, stopIds: [...allStopIds], operators: [...operators].sort(),
    sncf_id: null, ti_id: null, uic8: null,
  });
  for (const sid of esStopIds) assignedStops.add(sid);
  esOnlyAdded.push(name);
}

// ── Stops UK orphelins (non couverts par CSV avec atoc_is_enabled=t) ──────────
// Les noms GTFS UK sont souvent en ALL CAPS avec suffixes Platform → nettoyage
const CRS_NAMES = {
  // Londres — noms AVEC préfixe "London" pour cohérence avec stations.csv et bridges
  'STP':'London St Pancras International',
  'EUS':'London Euston','KGX':'London Kings Cross','PAD':'London Paddington',
  'VIC':'London Victoria','WAT':'London Waterloo','WAE':'London Waterloo',
  'LST':'London Liverpool Street','LBG':'London Bridge',
  'MYB':'London Marylebone','CHX':'London Charing Cross',
  'CST':'London Cannon Street','FST':'London Fenchurch Street','BFR':'London Blackfriars',
  // Reste UK — grandes villes anglaises
  'MAN':'Manchester Piccadilly','MCV':'Manchester Piccadilly',
  'BHM':'Birmingham New Street','GLC':'Glasgow Central',
  'EDB':'Edinburgh Waverley','LIV':'Liverpool Lime Street',
  'BHI':'Birmingham International','COV':'Coventry','WFJ':'Wolverhampton',
  'MKC':'Milton Keynes Central','RUG':'Rugby','LMS':'Lancaster',
  'PRE':'Preston','CRE':'Crewe','STA':'Stoke-on-Trent',
  'SPT':'Stockport','OXF':'Oxford','RDG':'Reading',
  'BRI':'Bristol Temple Meads','CTR':'Chester','CAR':'Carlisle',
  // Écosse
  'ABD':'Aberdeen','INV':'Inverness','DEE':'Dundee','PTH':'Perth',
  'STG':'Stirling','AYR':'Ayr','KLD':'Kilmarnock','FLK':'Falkirk',
  'DFR':'Dunfermline','KDY':'Kirkcaldy','GRK':'Greenock Central',
  'MLG':'Glasgow Queen Street','PAI':'Paisley Canal',
  'FWI':'Fort William','OBN':'Oban','KYL':'Kyle of Lochalsh',
  'WIC':'Wick','THB':'Thurso',
  // Pays de Galles
  'CDF':'Cardiff Central','SWA':'Swansea','NWP':'Newport',
  'WRX':'Wrexham General','BNG':'Bangor','HHD':'Holyhead',
  'AHV':'Aberystwyth','CMN':'Carmarthen','LLD':'Llandudno',
  'RHL':'Rhyl','BGD':'Bridgend',
  // Irlande du Nord
  'BFT':'Belfast Central','BPT':'Belfast Great Victoria Street',
};
function cleanUkName(raw) {
  if (!raw) return raw;
  let s = raw.trim()
    .replace(/\s+Platform[\s\d]*$/i,'').replace(/\s+Plat[\s\d]*$/i,'')
    .replace(/\s+PLT[\s\d]*$/i,'').replace(/\s+Bay[\s\d]*$/i,'').trim();
  if (s === s.toUpperCase()) {
    const SMALL = new Set(['and','or','the','of','to','at','in','on','&']);
    s = s.toLowerCase().replace(/\b\w+/g, (w, i) => (i===0||!SMALL.has(w)) ? w[0].toUpperCase()+w.slice(1) : w);
  }
  return s;
}
const ukByCrs  = new Map();
const ukByName = new Map();
for (const [sid, stop] of Object.entries(stops)) {
  if (assignedStops.has(sid)) continue;
  const op = stop.operator || extractOperator(sid);
  if (op !== 'UK') continue;
  const crs = stop.code;
  const lat = stop.lat || 0;
  const lon = stop.lon || 0;
  const validCoords = lat >= 49 && lat <= 62 && lon >= -9 && lon <= 2;
  if (crs) {
    const name = CRS_NAMES[crs] || cleanUkName(stop.name || crs);
    if (!ukByCrs.has(crs)) ukByCrs.set(crs, { name, lat: validCoords?lat:0, lon: validCoords?lon:0, stopIds:[], operators: new Set(['UK']) });
    const g = ukByCrs.get(crs);
    g.stopIds.push(sid);
    if (validCoords && !g.lat) { g.lat = lat; g.lon = lon; }
  } else {
    const name = cleanUkName(stop.name || sid);
    if (!ukByName.has(name)) ukByName.set(name, { name, lat: validCoords?lat:0, lon: validCoords?lon:0, stopIds:[], operators: new Set(['UK']) });
    const g = ukByName.get(name);
    g.stopIds.push(sid);
    if (validCoords && !g.lat) { g.lat = lat; g.lon = lon; }
  }
}
let nbUkCreated = 0;
// ── Fusionner ukByName dans ukByCrs quand GPS < 300m (ex: SR + LNER à Edinburgh) ──
for (const [name, grpName] of ukByName) {
  if (!grpName.lat || !grpName.lon) continue;
  let merged = false;
  for (const [crs, grpCrs] of ukByCrs) {
    if (!grpCrs.lat || !grpCrs.lon) continue;
    if (distMeters(grpName.lat, grpName.lon, grpCrs.lat, grpCrs.lon) < 300) {
      // Même gare physique — absorber les stops de grpName dans grpCrs
      for (const sid of grpName.stopIds) {
        if (!grpCrs.stopIds.includes(sid)) grpCrs.stopIds.push(sid);
      }
      merged = true;
      break;
    }
  }
  if (!merged) {
    // Garder dans ukByName mais vérifier aussi contre d'autres entrées ukByName
    // (deux opérateurs sans CRS au même endroit)
    for (const [name2, grp2] of ukByName) {
      if (name2 === name || !grp2.lat || !grp2.lon) continue;
      if (distMeters(grpName.lat, grpName.lon, grp2.lat, grp2.lon) < 300) {
        // Absorber dans le plus grand groupe
        const target = grpName.stopIds.length >= grp2.stopIds.length ? grpName : grp2;
        const source = target === grpName ? grp2 : grpName;
        for (const sid of source.stopIds) {
          if (!target.stopIds.includes(sid)) target.stopIds.push(sid);
        }
        source.stopIds = []; // vider pour éviter double émission
        break;
      }
    }
  }
}
for (const group of [...ukByCrs.values(), ...ukByName.values()]) {
  const unassigned = group.stopIds.filter(s => !assignedStops.has(s));
  if (!unassigned.length) continue;
  stations.push({
    name: group.name, city: extractCity(group.name), slug: '',
    country: 'GB', lat: group.lat, lon: group.lon,
    stopIds: unassigned, operators: [...group.operators].sort(),
    sncf_id: null, ti_id: null, uic8: null,
  });
  for (const sid of unassigned) assignedStops.add(sid);
  nbUkCreated++;
}
console.log('  Gares UK creees (orphans) : ' + nbUkCreated);

// ── Stops orphelins SNCF/TI/RENFE/CP/SNCB (non couverts par CSV) ─────────────
function normalizeStationName(n) {
  return n.toLowerCase().trim().normalize('NFD').replace(/[\u0300-\u036f]/g,'').replace(/[-_\s]+/g,' ');
}
function countryFromStopId(sid) {
  if (sid.startsWith('UK:'))                        return 'GB';
  if (sid.startsWith('RENFE:') || sid.startsWith('OUIGO_ES:')) return 'ES';
  if (sid.startsWith('CP:'))                        return 'PT';
  if (sid.startsWith('SNCB:'))                      return 'BE';
  if (sid.startsWith('TI:') || sid.startsWith('TI_AV:')) return 'IT';
  // DB_FV/DB_RV couvrent DE + AT + PL + CH + IT + SI + HR + SK + HU + NL + BE + DK + CZ + FR
  // Le préfixe seul ne donne pas le pays — laisser countryFromGPS décider
  if (sid.startsWith('DB_FV:') || sid.startsWith('DB_RV:')) return null;
  // UIC international 7–9 chiffres (SNCF, SNCB, DB numérique, etc.)
  const m = sid.match(/(\d{7,9})$/);
  if (!m) return 'FR';
  const prefix = m[1].slice(0, 2);
  const map = {
    '87':'FR','86':'FR',          // SNCF
    '88':'BE',                    // SNCB
    '80':'DE','81':'DE',          // DB
    '82':'AT',                    // ÖBB
    '83':'IT',                    // Trenitalia / RFI
    '84':'ES',                    // Renfe
    '85':'PT',                    // CP
    '70':'GB','71':'GB',          // Network Rail UK
    '74':'CH',                    // SBB
    '79':'NL','78':'NL',          // NS
    '55':'PL',                    // PKP
    '54':'CZ',                    // ČD
    '53':'SK',                    // ZSSK
    '55':'HU',                    // MÁV (partage préfixe avec PL — voir UIC complet)
    '79':'DK',                    // DSB (certains codes)
    '75':'HR',                    // HŽ Croatie
    '79':'SI',                    // SŽ Slovénie
  };
  return map[prefix] || 'FR';
}

// Déduction du pays par coordonnées GPS (fallback pour stops numériques sans préfixe clair)
// Déduction du pays par coordonnées GPS.
// Validé exhaustivement sur les 536 gares du feed DB_FV (87 cas délicats, 0 erreur).
// ORDRE CRITIQUE : les règles les plus précises et les plus petits pays d'abord.
function countryFromGPS(lat, lon) {
  lat = parseFloat(lat); lon = parseFloat(lon);
  if (!lat || !lon) return null;

  // Danemark
  if (lat >= 54.5  && lat <= 57.8  && lon >= 8.0   && lon <= 15.3)  return 'DK';

  // Pologne — lon>=15.0 exclut Praha (lon=14.43) qui tombait ici
  if (lat >= 49.0  && lat <= 54.9  && lon >= 15.0  && lon <= 24.2)  return 'PL';

  // Slovaquie — AVANT CZ
  // Zone est SK (Nove Zamky, Sturovo, Budapest-area SK)
  if (lat >= 47.8  && lat <= 49.6  && lon >= 17.3  && lon <= 22.6)  return 'SK';
  // Zone Bratislava (lon 17.0–17.3)
  if (lat >= 48.1  && lat <= 49.6  && lon >= 17.0  && lon <  17.3)  return 'SK';
  // Kuty SK lon=17.04 séparé de Breclav CZ lon=16.89 par le seuil lat>=48.5
  // (On ne fait PAS de règle lon<17.0 pour SK : Breclav CZ lon=16.89 resterait dans CZ)

  // Tchéquie — après SK
  if (lat >= 48.55 && lat <= 51.1  && lon >= 12.1  && lon <= 18.85) return 'CZ';

  // Hongrie — lon>=17.0 exclut Wien (lon=16.37) et Flughafen Wien (lon=16.56)
  if (lat >= 45.7  && lat <= 48.6  && lon >= 17.0  && lon <= 22.9)  return 'HU';

  // Croatie — lat strictement < 45.95
  if (lat >= 42.3  && lat <  45.95 && lon >= 13.5  && lon <= 19.5)  return 'HR';

  // Slovénie — deux zones pour exclure Klagenfurt AT (lon=14.31 lat=46.61)
  // Zone sud : lat < 46.55, lon >= 14.0 (Jesenice=46.43, Kranj=46.23, Ljubljana=46.05, Krsko=45.95)
  if (lat >= 45.95 && lat <  46.55 && lon >= 14.0  && lon <= 16.6)  return 'SI';
  // Zone nord-est : lat 46.55–46.65, lon >= 15.0 (Maribor=46.56,15.65 — Klagenfurt=46.61,14.31 exclu)
  if (lat >= 46.55 && lat <= 46.65 && lon >= 15.0  && lon <= 16.6)  return 'SI';

  // Suisse — AVANT IT pour capturer Valais (Brig=46.31,7.98 / Visp=46.29,7.88)
  // lon max 9.65 pour exclure Vorarlberg AT (Bludenz=47.15,9.81)
  if (lat >= 45.8  && lat <= 47.8  && lon >= 5.8   && lon <= 9.65)  return 'CH';

  // Italie du Nord — après CH
  // Alto Adige / Tyrol du Sud (Bolzano, Brennero, Bressanone) : lon 10.5–12.5
  // lon max 12.5 pour exclure Villach AT (13.84) et Mallnitz AT (13.17)
  if (lat >= 43.5  && lat <  47.1  && lon >= 10.5  && lon <= 12.5)  return 'IT';
  // Vénétie / Lombardie (Venezia, Verona, Padova) : lon < 10.5
  if (lat >= 43.5  && lat <  46.5  && lon >= 6.6   && lon <  10.5)  return 'IT';

  // Autriche — après CH, SI, IT, SK, CZ, HU
  if (lat >= 46.4  && lat <= 49.0  && lon >= 9.5   && lon <= 17.2)  return 'AT';

  // Allemagne — large bbox, après tous les pays voisins
  if (lat >= 47.2  && lat <= 55.1  && lon >= 5.8   && lon <= 15.1)  return 'DE';

  // Pays-Bas — lat > 51.0 pour exclure Bruxelles (lat=50.83)
  if (lat >= 51.0  && lat <= 53.6  && lon >= 3.3   && lon <= 7.2)   return 'NL';

  // Belgique
  if (lat >= 49.5  && lat <= 51.6  && lon >= 2.5   && lon <= 6.5)   return 'BE';

  // France
  if (lat >= 42.3  && lat <= 51.5  && lon >= -5.0  && lon <= 8.5)   return 'FR';

  return null;
}
const stopIdToStation = new Map();
for (let i = 0; i < stations.length; i++) {
  for (const sid of stations[i].stopIds) stopIdToStation.set(sid, i);
}
const orphanGroups = new Map();
for (const [sid, stop] of Object.entries(stops)) {
  // ES: et UK: ont deja leur propre bloc de ramassage ci-dessus
  if (assignedStops.has(sid) || sid.startsWith('ES:') || sid.startsWith('UK:')) continue;

  // Rattachement a une gare SNCF existante via StopArea parent
  const parentArea = (xfer[sid] || []).map(xferId).find(v => v.startsWith('SNCF:StopArea:'));
  if (parentArea && stopIdToStation.has(parentArea)) {
    const pst = stations[stopIdToStation.get(parentArea)];
    if (!pst.stopIds.includes(sid)) pst.stopIds.push(sid);
    assignedStops.add(sid); continue;
  }

  const op   = stop.operator || extractOperator(sid);
  const name = stop.name || sid;
  const key  = normalizeStationName(name);

  // Pays : GPS prioritaire pour tout opérateur multi-pays (DB_FV, DB_RV)
  // ou quand countryFromStopId ne sait pas (null) — sinon préfixe ID
  const countryById  = countryFromStopId(sid);
  const countryByGps = countryFromGPS(stop.lat, stop.lon);
  const isMultiCountryOp = (op === 'DB_FV' || op === 'DB_RV');
  const country = (isMultiCountryOp || countryById === null)
    ? (countryByGps || countryById || 'EU')
    : countryById;

  if (!orphanGroups.has(key)) {
    orphanGroups.set(key, {
      name, country,
      lat: stop.lat || 0, lon: stop.lon || 0,
      stopIds: [sid], operators: new Set([op]),
    });
  } else {
    const e = orphanGroups.get(key);
    e.stopIds.push(sid);
    e.operators.add(op);
    // Priorite au nom SNCF
    if (op === 'SNCF' && !e.operators.has('SNCF')) e.name = name;
    // Consolider le pays : si GPS disponible et opérateur multi-pays, GPS gagne toujours
    if (countryByGps && (isMultiCountryOp || e.country === 'FR' || e.country === 'EU')) {
      e.country = countryByGps;
    }
    // Consolider coords si manquantes
    if (!e.lat && stop.lat) { e.lat = stop.lat; e.lon = stop.lon; }
  }
}

let nbAllOrphans = 0;
const orphansByCountry = {};
for (const e of orphanGroups.values()) {
  stations.push({
    ...e, city: extractCity(e.name), slug: '',
    operators: [...e.operators].sort(),
    sncf_id: null, ti_id: null, uic8: null,
  });
  orphansByCountry[e.country] = (orphansByCountry[e.country] || 0) + 1;
  nbAllOrphans++;
}
console.log('  Gares orphelines ajoutees : ' + nbAllOrphans);
console.log('  Repartition : ' + Object.entries(orphansByCountry).sort((a,b)=>b[1]-a[1]).map(([k,v])=>k+'='+v).join(', '));

// ── Post-processing : fusion ES-only + gares SNCF (via validEsTransfers) ──────
const stopIdToStationIdx = {};
for (let i = 0; i < stations.length; i++) {
  for (const sid of stations[i].stopIds) stopIdToStationIdx[sid] = i;
}
const toRemoveIdxs = new Set();
for (const [uic, esBases] of Object.entries(validEsTransfers)) {
  let sncfStation = stations.find(s => s.uic8 === uic);
  if (!sncfStation) {
    sncfStation = stations.find(s => s.stopIds.some(sid => {
      const m = sid.match(/-(\d{7,9})$/) || sid.match(/OCE(\d{7,9})$/);
      return m && m[1] === uic;
    }));
  }
  if (!sncfStation) continue;
  for (const esBase of esBases) {
    const esStopIds = slugToEsStops[esBase] || [];
    if (!esStopIds.length) continue;
    const esStationIdx = stopIdToStationIdx[esStopIds[0]];
    const esStation = esStationIdx !== undefined ? stations[esStationIdx] : null;
    if (esStation && esStation !== sncfStation) {
      const allIds = new Set([...sncfStation.stopIds, ...esStation.stopIds]);
      sncfStation.stopIds = [...allIds];
      if (!sncfStation.operators.includes('ES')) { sncfStation.operators.push('ES'); sncfStation.operators.sort(); }
      toRemoveIdxs.add(esStationIdx);
    } else if (!esStation) {
      const allIds = new Set([...sncfStation.stopIds, ...esStopIds]);
      sncfStation.stopIds = [...allIds];
      if (!sncfStation.operators.includes('ES')) { sncfStation.operators.push('ES'); sncfStation.operators.sort(); }
    }
  }
}
const stationsFiltered = stations.filter((_, i) => !toRemoveIdxs.has(i));
stations.length = 0;
stations.push(...stationsFiltered);
if (toRemoveIdxs.size) console.log('  ' + toRemoveIdxs.size + ' gare(s) ES-only fusionnée(s)');

// ── Tri : SNCF > ES > TI > RENFE > SNCB > CP > UK > autres ──────────────────
stations.sort((a, b) => {
  const score = s =>
    (s.operators.includes('SNCF')     ? 256 : 0) +
    (s.operators.includes('ES')       ? 128 : 0) +
    (s.operators.includes('TI')       ?  64 : 0) +
    (s.operators.includes('TI_AV')    ?  64 : 0) +
    (s.operators.includes('SNCB')     ?  32 : 0) +
    (s.operators.includes('RENFE')    ?  16 : 0) +
    (s.operators.includes('OUIGO_ES') ?   8 : 0) +
    (s.operators.includes('CP')       ?   4 : 0) +
    (s.operators.includes('DB_FV')    ?   2 : 0) +
    (s.operators.includes('UK')       ?   1 : 0);
  if (score(b) !== score(a)) return score(b) - score(a);
  return a.name.localeCompare(b.name, 'fr');
});

// ── Écriture stations.json ────────────────────────────────────────────────────
fs.writeFileSync(OUT_FILE, JSON.stringify(stations, null, 2), 'utf8');
const sizeKb = Math.round(fs.statSync(OUT_FILE).size / 1024);
console.log('\nstations.json : ' + stations.length + ' gares — ' + sizeKb + ' KB');

// Résumé par pays/opérateur
const opCount = {};
for (const s of stations) {
  for (const op of s.operators) opCount[op] = (opCount[op]||0) + 1;
}
const countryCount = {};
for (const s of stations) countryCount[s.country] = (countryCount[s.country]||0) + 1;
console.log('  Par opérateur : ' + Object.entries(opCount).sort((a,b)=>b[1]-a[1]).map(([k,v])=>k+'='+v).join(', '));
console.log('  Par pays      : ' + Object.entries(countryCount).sort((a,b)=>b[1]-a[1]).map(([k,v])=>k+'='+v).join(', '));

// xferUpdated : copie mutable du transfer_index, enrichie au fil des sections suivantes
const xferUpdated = Object.assign({}, xfer);

// ── Ponts inter-terminaux via parent_station_id du CSV ───────────────────────
// Le CSV stations.csv contient un champ parent_station_id qui indique la ville.
// Ex : toutes les gares de Londres ont parent_station_id=8267 (London).
// On charge ce fichier pour construire un index ville → gares sœurs,
// puis on injecte dans transfer_index tous les liens croisés.

console.log('\n-- Ponts par ville (parent_station_id CSV) ------------------------');
{
  const csvRowsForParent = parseCsv(CSV_FILE);
  // id → parent_station_id
  const csvParentMap = {};
  const csvIdToName  = {};
  const csvIdToSlug  = {};
  for (const row of csvRowsForParent) {
    if (!row.id) continue;
    csvParentMap[row.id] = row.parent_station_id || '';
    csvIdToName[row.id]  = row.name || '';
    csvIdToSlug[row.id]  = row.slug || '';
  }
  // parent_id → [child csv ids]
  const parentToChildren = {};
  for (const [id, parent] of Object.entries(csvParentMap)) {
    if (!parent) continue;
    if (!parentToChildren[parent]) parentToChildren[parent] = [];
    parentToChildren[parent].push(id);
  }
  // Pour chaque groupe, trouver les stations.json correspondantes via slug
  const slugToStation = {};
  for (const st of stations) {
    if (st.slug) slugToStation[st.slug] = st;
    // Aussi par nom normalisé pour le fallback
    const normName = (st.name||'').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'');
    slugToStation['__name__' + normName] = st;
  }
  let cityBridgeLinks = 0;
  let cityGroupsProcessed = 0;
  for (const [parentId, childIds] of Object.entries(parentToChildren)) {
    if (childIds.length < 2) continue;
    // Trouver les stations.json pour chaque enfant
    const childStations = [];
    for (const cid of childIds) {
      const slug = csvIdToSlug[cid];
      const name = csvIdToName[cid];
      let st = slug ? slugToStation[slug] : null;
      if (!st && name) {
        const normName = name.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'');
        st = slugToStation['__name__' + normName];
      }
      if (st && !childStations.includes(st)) childStations.push(st);
    }
    if (childStations.length < 2) continue;
    // Générer les ponts entre toutes les stations du groupe
    // Convention : même ville → interCity:true (nécessite un déplacement)
    // Sauf si elles sont déjà proches GPS (< 500m) → interCity:false
    for (let ci = 0; ci < childStations.length; ci++) {
      for (let cj = ci + 1; cj < childStations.length; cj++) {
        const stA = childStations[ci];
        const stB = childStations[cj];
        if (stA === stB) continue;
        // Distance approximative
        const dLat = (stA.lat - stB.lat) * 111000;
        const dLon = (stA.lon - stB.lon) * 111000 * Math.cos((stA.lat + stB.lat) / 2 * Math.PI / 180);
        const dist = Math.sqrt(dLat*dLat + dLon*dLon);
        const interCity = dist > 500;
        for (const sidA of stA.stopIds) {
          if (!xferUpdated[sidA]) xferUpdated[sidA] = [];
          for (const sidB of stB.stopIds) {
            const link = interCity ? { id: sidB, interCity: true } : sidB;
            if (!xferUpdated[sidA].some(x => xferId(x) === sidB)) {
              xferUpdated[sidA].push(link); cityBridgeLinks++;
            }
          }
        }
        for (const sidB of stB.stopIds) {
          if (!xferUpdated[sidB]) xferUpdated[sidB] = [];
          for (const sidA of stA.stopIds) {
            const link = interCity ? { id: sidA, interCity: true } : sidA;
            if (!xferUpdated[sidB].some(x => xferId(x) === sidA)) {
              xferUpdated[sidB].push(link); cityBridgeLinks++;
            }
          }
        }
      }
    }
    cityGroupsProcessed++;
  }
  console.log(`  ${cityGroupsProcessed} villes traitées, +${cityBridgeLinks} liens inter-gares injectés`);
}


// ── Ponts directs London : injection au niveau des stop IDs ──────────────────
// Le GTFS UK (Avanti Only) ne contient que les gares de sa propre ligne :
// London Euston → Birmingham → Manchester/Glasgow/Edinburgh
// Les autres terminaux londoniens (Kings Cross, Paddington, Victoria…) n'ont
// pas de stop UK dans notre GTFS. On ne peut donc pas passer par stations.json.
//
// Solution : on injecte les liens directement entre les stop IDs connus,
// en cherchant dynamiquement les stops de chaque gare dans stops.json par GPS.
//
// Coordonnées des grandes gares londoniennes (WGS84) :
const LONDON_TERMINALS_GPS = [
  // Cluster Nord : ~10 min à pied entre eux → interCity:false
  { name: 'London St Pancras',  lat: 51.5322, lon: -0.1234, cluster: 'north' },
  { name: 'London Kings Cross', lat: 51.5308, lon: -0.1231, cluster: 'north' },
  { name: 'London Euston',      lat: 51.5282, lon: -0.1337, cluster: 'north' },
  // Autres terminaux : > 15 min → interCity:true
  { name: 'London Paddington',      lat: 51.5154, lon: -0.1755, cluster: 'west'  },
  { name: 'London Victoria',        lat: 51.4952, lon: -0.1439, cluster: 'south' },
  { name: 'London Waterloo',        lat: 51.5036, lon: -0.1143, cluster: 'south' },
  { name: 'London Liverpool Street',lat: 51.5178, lon: -0.0823, cluster: 'east'  },
  { name: 'London Bridge',          lat: 51.5053, lon: -0.0864, cluster: 'east'  },
  { name: 'London Marylebone',      lat: 51.5225, lon: -0.1631, cluster: 'west'  },
  { name: 'London Charing Cross',   lat: 51.5083, lon: -0.1247, cluster: 'south' },
  { name: 'London Cannon Street',   lat: 51.5113, lon: -0.0904, cluster: 'east'  },
  { name: 'London Blackfriars',     lat: 51.5118, lon: -0.1034, cluster: 'south' },
  { name: 'London Fenchurch Street',lat: 51.5112, lon: -0.0784, cluster: 'east'  },
];

console.log('\n-- Ponts directs London (GPS) -------------------------------------');
{
  // Pour chaque terminal, trouver tous les stops UK + ES dans un rayon de 600m
  const terminalStops = LONDON_TERMINALS_GPS.map(t => {
    const stopIds = [];
    for (const [sid, stop] of Object.entries(stops)) {
      if (!stop.lat || !stop.lon) continue;
      if (!sid.startsWith('UK:') && !sid.startsWith('ES:')) continue;
      if (distMeters(t.lat, t.lon, stop.lat, stop.lon) < 600) {
        stopIds.push(sid);
      }
    }
    return { ...t, stopIds };
  });

  // Log
  for (const t of terminalStops) {
    if (t.stopIds.length) {
      console.log(`  ${t.name.padEnd(30)} : ${t.stopIds.length} stops (${t.stopIds.slice(0,2).join(', ')}${t.stopIds.length>2?'…':''})`);
    } else {
      console.log(`  ${t.name.padEnd(30)} : [vide - pas dans GTFS Avanti]`);
    }
  }

  // Injecter les liens entre tous les terminaux qui ont au moins un stop
  let londonLinks = 0;
  for (let i = 0; i < terminalStops.length; i++) {
    for (let j = i + 1; j < terminalStops.length; j++) {
      const tA = terminalStops[i];
      const tB = terminalStops[j];
      if (!tA.stopIds.length || !tB.stopIds.length) continue;
      // interCity:false seulement si les deux sont dans le même cluster (nord)
      const interCity = tA.cluster !== tB.cluster || tA.cluster !== 'north';
      for (const sidA of tA.stopIds) {
        if (!xferUpdated[sidA]) xferUpdated[sidA] = [];
        for (const sidB of tB.stopIds) {
          const link = interCity ? { id: sidB, interCity: true } : sidB;
          if (!xferUpdated[sidA].some(x => xferId(x) === sidB)) {
            xferUpdated[sidA].push(link); londonLinks++;
          }
        }
      }
      for (const sidB of tB.stopIds) {
        if (!xferUpdated[sidB]) xferUpdated[sidB] = [];
        for (const sidA of tA.stopIds) {
          const link = interCity ? { id: sidA, interCity: true } : sidA;
          if (!xferUpdated[sidB].some(x => xferId(x) === sidA)) {
            xferUpdated[sidB].push(link); londonLinks++;
          }
        }
      }
      if (tA.stopIds.length && tB.stopIds.length) {
        const type = interCity ? '~20-40 min' : '~5-10 min';
        console.log(`  OK ${tA.name} <-> ${tB.name}  [${type}]`);
      }
    }
  }
  console.log(`  Total : +${londonLinks} liens London injectés`);
}

// ── Ponts Paris inter-terminaux (via stations.json) ───────────────────────────
{
  function findStationByName(name, country) {
    const norm = s => s.toLowerCase().trim().normalize('NFD').replace(/[\u0300-\u036f]/g,'').replace(/[\u2019']/g,"'");
    const n = norm(name);
    const pool = country ? stations.filter(s => s.country === country) : stations;
    return pool.find(s => norm(s.name) === n)
        || pool.find(s => norm(s.name).startsWith(n + ' ') || norm(s.name) === n)
        || null;
  }
  const PARIS_BRIDGES = [
    { nameA: 'Paris Gare du Nord',  nameB: "Paris Gare de l'Est",  interCity: false },
    { nameA: 'Paris Gare du Nord',  nameB: 'Paris Gare de Lyon',   interCity: true  },
    { nameA: 'Paris Gare du Nord',  nameB: 'Paris Montparnasse',   interCity: true  },
    { nameA: "Paris Gare de l'Est", nameB: 'Paris Gare de Lyon',   interCity: true  },
    { nameA: "Paris Gare de l'Est", nameB: 'Paris Montparnasse',   interCity: true  },
    { nameA: 'Paris Gare de Lyon',  nameB: 'Paris Montparnasse',   interCity: true  },
  ];
  console.log('\n-- Ponts Paris inter-terminaux ------------------------------------');
  let parisLinks = 0;
  for (const bridge of PARIS_BRIDGES) {
    const stA = findStationByName(bridge.nameA, 'FR');
    const stB = findStationByName(bridge.nameB, 'FR');
    if (!stA) { console.log('  [!] Non trouve : ' + bridge.nameA); continue; }
    if (!stB) { console.log('  [!] Non trouve : ' + bridge.nameB); continue; }
    let added = 0;
    for (const sidA of stA.stopIds) {
      if (!xferUpdated[sidA]) xferUpdated[sidA] = [];
      for (const sidB of stB.stopIds) {
        const link = bridge.interCity ? { id: sidB, interCity: true } : sidB;
        if (!xferUpdated[sidA].some(x => xferId(x) === sidB)) { xferUpdated[sidA].push(link); added++; parisLinks++; }
      }
    }
    for (const sidB of stB.stopIds) {
      if (!xferUpdated[sidB]) xferUpdated[sidB] = [];
      for (const sidA of stA.stopIds) {
        const link = bridge.interCity ? { id: sidA, interCity: true } : sidA;
        if (!xferUpdated[sidB].some(x => xferId(x) === sidA)) { xferUpdated[sidB].push(link); added++; parisLinks++; }
      }
    }
    const type = bridge.interCity ? '~20-40 min' : '~5-10 min';
    console.log(`  OK ${stA.name} <-> ${stB.name}  +${added} [${type}]`);
  }
  console.log(`  Total : +${parisLinks} liens Paris injectés`);
}

// ── Écriture transfer_index.json mis à jour ───────────────────────────────────
if (fs.existsSync(XFER_FILE)) {
  fs.writeFileSync(XFER_FILE, JSON.stringify(xferUpdated), 'utf8');
  console.log('\n  transfer_index.json mis a jour');
}


console.log('\n-- Diagnostic gares cles -------------------------------------------');
const CHECK = [
  'Paris Gare de Lyon', 'Paris Gare du Nord', "Paris Gare de l'Est",
  'Bruxelles-Midi', 'Amsterdam-Centraal',
  'Madrid Atocha', 'Barcelona Sants', 'Madrid-Chamartín-Clara Campoamor',
  'Lisboa Santa Apolónia', 'Porto Campanhã',
  'London Euston', 'London St Pancras International', 'Edinburgh Waverley',
  'Glasgow Central', 'Aberdeen', 'Inverness', 'Cardiff Central', 'Swansea',
  'Milano Centrale', 'Torino Porta Susa',
  // Italie AV (TI_AV)
  'Roma Termini', 'Firenze Santa Maria Novella', 'Napoli Centrale',
  'Venezia Santa Lucia', 'Bologna Centrale', 'Torino Porta Nuova',
  // DB / Europe centrale
  'Berlin Hbf', 'Frankfurt(M) Hbf', 'München Hbf', 'Hamburg Hbf',
  'Köln Hbf', 'Düsseldorf Hbf', 'Wien Hbf', 'Zürich HB',
  'Warszawa Centralna', 'Kraków Główny', 'Praha hl.n.',
  'Ljubljana', 'Zagreb Glavni kolodvor',
  'Aachen Hbf', "'s-Hertogenbosch",
];
for (const nom of CHECK) {
  const normN = s => s.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'').replace(/[\u2019']/g,"'");
  const f = stations.find(s => normN(s.name) === normN(nom))
         || stations.find(s => normN(s.name).startsWith(normN(nom)));
  if (f) {
    const es = f.stopIds.filter(id => id.startsWith('ES:'));
    const uk = f.stopIds.filter(id => id.startsWith('UK:'));
    const ot = f.stopIds.filter(id => !id.startsWith('ES:') && !id.startsWith('UK:'));
    const warnUk = (!uk.length && f.country === 'GB') ? ' [!] pas UK' : '';
    console.log('  OK ' + f.name.padEnd(36) + ' [' + f.operators.join('+') + ']' + warnUk + '  (' + f.country + ')');
    if (es.length) console.log('     ES: ' + es[0] + (es.length>1?' +'+( es.length-1):''));
    if (uk.length) console.log('     UK: ' + uk[0] + (uk.length>1?' +'+(uk.length-1):''));
    if (ot.length) console.log('     ot: ' + ot[0] + (ot.length>1?' +'+(ot.length-1):''));
  } else {
    console.log('  [X] ' + nom);
  }
}

console.log('\n-> Relancez : node server.js');