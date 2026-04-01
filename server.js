/**
 * Serveur RAPTOR — SNCF + Trenitalia France
 * Optimisations : buildStopToTrips une seule fois, RAPTOR multi-origines, lookup Map
 */

const http = require('http');
const fs   = require('fs');
const path = require('path');
const url  = require('url');

const DATA_DIR    = process.env.DATA_DIR || './engine_data';
const PORT        = process.env.PORT     || 3000;
const MAX_ROUNDS  = 5;
const MAX_RESULTS = 8;

const MIN_TRANSFER_SAME  = 3  * 60;  // 3 min  — même opérateur / même gare
const MIN_TRANSFER_CROSS = 10 * 60;  // 10 min — inter-opérateurs (SNCF ↔ TI)
const MIN_TRANSFER_CITY  = 45 * 60;  // 45 min — inter-gares même ville (métro)

// ─── Données en RAM ───────────────────────────────────────────────────────────
let stops, routesInfo, routesByStop, routeStops, routeTrips, calendarIndex, meta;
let transferIndex  = {};
let stopsIndex     = [];
let stopNameMap    = new Map();   // stopId → nom affiché, O(1)
let stopStationMap = new Map();   // stopId → station name groupée, O(1)
let stopCityKeyMap  = new Map();   // stopId → 'city:country' key, O(1)
let globalCoordsMap = new Map();   // stopId → {lat, lon} précompilé au démarrage
let tarifIndex      = {};
let cityIndex      = new Map();   // ville groupée → { city, country, stopIds, stations }

const COUNTRY_NAMES = {
  FR:'France', IT:'Italie', BE:'Belgique', DE:'Allemagne',
  NL:'Pays-Bas', GB:'Royaume-Uni', ES:'Espagne', PT:'Portugal',
  CH:'Suisse', AT:'Autriche', PL:'Pologne', CZ:'Tchéquie', SK:'Slovaquie',
};

// Index RAPTOR global — construit UNE SEULE FOIS au démarrage
let globalStopToTrips = null;

function loadJSON(filename) {
  const p = path.join(DATA_DIR, filename);
  if (!fs.existsSync(p)) throw new Error('Fichier manquant : ' + p);
  return JSON.parse(fs.readFileSync(p, 'utf8'));
}

// ─── État du moteur ───────────────────────────────────────────────────────────
let engineReady    = false;
let engineError    = null;
let engineLoadedAt = null;
let engineLoadMs   = null;

function initEngine() {
  console.log('\n🚂 Chargement moteur RAPTOR (SNCF + Trenitalia)...');
  const t = Date.now();

  stops         = loadJSON('stops.json');
  routesInfo    = loadJSON('routes_info.json');
  routesByStop  = loadJSON('routes_by_stop.json');
  routeStops    = loadJSON('route_stops.json');
  routeTrips    = loadJSON('route_trips.json');
  calendarIndex = loadJSON('calendar_index.json');
  meta          = loadJSON('meta.json');

  // Correspondances inter-quais + inter-opérateurs
  const tFile = path.join(DATA_DIR, 'transfer_index.json');
  if (fs.existsSync(tFile)) {
    transferIndex = loadJSON('transfer_index.json');
    console.log('  Correspondances : ' + Object.keys(transferIndex).length + ' arrêts');
  } else {
    // Fallback UIC pour les stops SNCF
    const uicMap = {};
    for (const sid of Object.keys(stops)) {
      const m = sid.match(/-(\d{8})$/);
      if (!m) continue;
      if (!uicMap[m[1]]) uicMap[m[1]] = [];
      uicMap[m[1]].push(sid);
    }
    for (const sids of Object.values(uicMap)) {
      for (const sid of sids) transferIndex[sid] = sids.filter(s => s !== sid);
    }
    console.log('  Correspondances (fallback UIC) : ' + Object.keys(uicMap).length + ' gares');
  }

  // Liaison inter-opérateurs SNCF ↔ TI par nom de gare normalisé
  (function linkSncfTI() {
    const norm = s => (s || '').toLowerCase()
      .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
      .replace(/[^a-z0-9]/g, ' ').replace(/\s+/g, ' ').trim();

    const nameToSncf = new Map();
    for (const [sid, s] of Object.entries(stops)) {
      if (sid.startsWith('TI:')) continue;
      const n = norm(s.name);
      if (!n) continue;
      if (!nameToSncf.has(n)) nameToSncf.set(n, []);
      nameToSncf.get(n).push(sid);
    }

    let crossCount = 0;
    for (const [sid, s] of Object.entries(stops)) {
      if (!sid.startsWith('TI:')) continue;
      const n = norm(s.name);
      const sncfSids = nameToSncf.get(n) || [];
      if (!sncfSids.length) continue;
      if (!transferIndex[sid]) transferIndex[sid] = [];
      for (const ss of sncfSids) {
        if (!transferIndex[sid].includes(ss)) { transferIndex[sid].push(ss); crossCount++; }
      }
      for (const ss of sncfSids) {
        if (!transferIndex[ss]) transferIndex[ss] = [];
        if (!transferIndex[ss].includes(sid)) transferIndex[ss].push(sid);
      }
    }
    console.log('  Correspondances inter-opérateurs SNCF\u2194TI : ' + crossCount + ' liaisons');
  })();

  // Index stop→trips (une seule fois)
  console.log('  Construction index stop→trips...');
  globalStopToTrips = buildStopToTrips(routeTrips);
  console.log('  Index : ' + Object.keys(globalStopToTrips).length + ' stops couverts');

  // Map stopId→nom en O(1)
  buildStopNameMap();
  buildStopsIndex();

  // Map coordonnées globale — construite une seule fois, réutilisée par /api/explore
  globalCoordsMap = new Map();
  for (const st of stopsIndex) {
    for (const sid of (st.stopIds||[])) {
      if (!globalCoordsMap.has(sid) && st.lat && st.lon) {
        globalCoordsMap.set(sid, { lat: st.lat, lon: st.lon, name: st.name });
      }
    }
  }
  console.log('  Coords map : ' + globalCoordsMap.size + ' stops géolocalisés');

  // Tarifs
  const tarifsFile = path.join(__dirname, 'tarifs-tgv-inoui-ouigo.json');
  if (fs.existsSync(tarifsFile)) {
    const raw = JSON.parse(fs.readFileSync(tarifsFile, 'utf8'));
    for (const row of raw) {
      const trans = normTransporteur(row.transporteur);
      const key   = row.gare_origine_code_uic+':'+row.gare_destination_code_uic+':'+trans+':'+row.classe+':'+row.profil_tarifaire;
      if (!tarifIndex[key]) tarifIndex[key] = { min: row.prix_minimum, max: row.prix_maximum };
      else {
        tarifIndex[key].min = Math.min(tarifIndex[key].min, row.prix_minimum);
        tarifIndex[key].max = Math.max(tarifIndex[key].max, row.prix_maximum);
      }
    }
    console.log('  Tarifs : ' + Object.keys(tarifIndex).length + ' entrées');
  }

  const totalTrips = Object.values(routeTrips).reduce((s, t) => s + t.length, 0);
  engineLoadMs   = Date.now() - t;
  engineLoadedAt = new Date().toISOString();
  engineReady    = true;
  console.log('✅ Prêt en ' + engineLoadMs + 'ms — ' + totalTrips.toLocaleString() + ' trips chargés\n');
}

// ─── Noms des gares ───────────────────────────────────────────────────────────

function buildStopNameMap() {
  for (const [sid, s] of Object.entries(stops)) stopNameMap.set(sid, s.name || sid);

  const garesFile = path.join(__dirname, 'gares-de-voyageurs.json');
  if (fs.existsSync(garesFile)) {
    const garesRaw = JSON.parse(fs.readFileSync(garesFile, 'utf8'));
    for (const gare of garesRaw) {
      if (!gare.codes_uic || !gare.nom) continue;
      const uics = gare.codes_uic.split(';').map(u => u.trim());
      for (const [sid] of Object.entries(stops)) {
        if (uics.some(uic => sid.endsWith('-'+uic))) stopNameMap.set(sid, gare.nom);
      }
    }
  }

  const stFile = path.join(__dirname, 'stations.json');
  if (fs.existsSync(stFile)) {
    const stations = JSON.parse(fs.readFileSync(stFile, 'utf8'));
    for (const s of stations) {
      for (const sid of (s.stopIds || [])) stopNameMap.set(sid, s.name);
    }
  }
}

function cleanStopName(stopId) {
  return stopNameMap.get(stopId) || (stops[stopId]?.name) || stopId;
}

// ─── Utilitaires transferIndex ────────────────────────────────────────────────
// Le transferIndex peut contenir des strings (liens normaux) ou des objets
// { id, interCity: true } (liens inter-gares même ville, ajoutés par gtfs-ingest).

function transferEntries(stopId) {
  return (transferIndex[stopId] || []).map(e =>
    typeof e === 'string' ? { id: e, interCity: false } : e
  );
}

function transferTime(fromId, toEntry) {
  if (toEntry.interCity) return MIN_TRANSFER_CITY;
  const sameOp = extractOperator(fromId) === extractOperator(toEntry.id);
  return sameOp ? MIN_TRANSFER_SAME : MIN_TRANSFER_CROSS;
}

// ─── Autocomplete ─────────────────────────────────────────────────────────────

function buildStopsIndex() {
  cityIndex = new Map();

  const stFile = path.join(__dirname, 'stations.json');
  if (fs.existsSync(stFile)) {
    const raw = JSON.parse(fs.readFileSync(stFile, 'utf8'));
    for (const s of raw) {
      const city    = s.city    || s.name;
      const country = s.country || 'FR';
      stopsIndex.push({ name:s.name, city, country, stopIds:s.stopIds||[], operators:s.operators||[], lat:s.lat||0, lon:s.lon||0 });
      const _ck = city.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'') + ':' + country;
      for (const _sid of (s.stopIds||[])) {
        stopStationMap.set(_sid, s.name);
        stopCityKeyMap.set(_sid, _ck);
      }

      const key = city.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'') + ':' + country;
      if (!cityIndex.has(key)) {
        cityIndex.set(key, {
          city, country, countryName: COUNTRY_NAMES[country] || country,
          stopIds: new Set(s.stopIds||[]), ops: new Set(s.operators||[]),
          stations: [], lat: s.lat||0, lon: s.lon||0,
        });
      }
      const ce = cityIndex.get(key);
      for (const sid of (s.stopIds||[])) ce.stopIds.add(sid);
      for (const op  of (s.operators||[])) ce.ops.add(op);
      ce.stations.push({ name: s.name, stopIds: s.stopIds||[] });
    }
    for (const [key, ce] of cityIndex) {
      if (ce.stations.length < 2) cityIndex.delete(key);
    }
    console.log('  Autocomplete (stations.json) : ' + stopsIndex.length + ' gares');
    console.log('  Villes multi-gares           : ' + cityIndex.size);
    return;
  }
  const garesFile = path.join(__dirname, 'gares-de-voyageurs.json');
  if (fs.existsSync(garesFile)) {
    const garesRaw = JSON.parse(fs.readFileSync(garesFile, 'utf8'));
    for (const gare of garesRaw) {
      if (!gare.codes_uic || !gare.nom) continue;
      const uics = gare.codes_uic.split(';').map(u => u.trim());
      const sids = Object.keys(stops).filter(sid => uics.some(uic => sid.endsWith('-'+uic)));
      const extra = new Set(sids);
      for (const sid of sids) for (const s of (transferIndex[sid]||[])) extra.add(s);
      if (!extra.size) continue;
      const ops = [...new Set([...extra].map(sid => sid.split(':')[0]))];
      stopsIndex.push({ name:gare.nom, city:gare.nom, country:'FR', stopIds:[...extra], operators:ops,
        lat:gare.position_geographique?.lat||0, lon:gare.position_geographique?.lon||0 });
    }
    const assigned = new Set(stopsIndex.flatMap(s => s.stopIds));
    const tiGroups = new Map();
    for (const [sid, s] of Object.entries(stops)) {
      if (assigned.has(sid) || !sid.startsWith('TI:')) continue;
      const key = (s.name||'').toLowerCase();
      if (!tiGroups.has(key)) tiGroups.set(key, { name:s.name, stopIds:[sid], lat:s.lat||0, lon:s.lon||0 });
      else tiGroups.get(key).stopIds.push(sid);
    }
    for (const e of tiGroups.values()) stopsIndex.push({ ...e, operators:['TI'], country:'IT' });
    console.log('  Autocomplete (fallback) : ' + stopsIndex.length + ' gares');
  }
}

function searchStops(query, limit=10) {
  const q = query.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'');
  const res = [];
  for (const e of stopsIndex) {
    const nom = e.name.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'');
    if (nom.includes(q)) { res.push({ type:'station', ...e }); if (res.length >= limit) break; }
  }
  // Trier par ville pour que le groupement côté client fonctionne correctement
  res.sort((a, b) => {
    const cityA = (a.city || a.name).toLowerCase();
    const cityB = (b.city || b.name).toLowerCase();
    if (cityA !== cityB) return cityA.localeCompare(cityB, 'fr');
    return (a.name || '').localeCompare(b.name || '', 'fr');
  });
  return res;
}

function searchCities(query) {
  const q = query.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'');
  const res = [];
  for (const [, ce] of cityIndex) {
    const cityNorm = ce.city.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'');
    if (!cityNorm.startsWith(q) && !cityNorm.includes(q)) continue;
    res.push({
      type: 'city', name: ce.city, country: ce.country, countryName: ce.countryName,
      stopIds: [...ce.stopIds], operators: [...ce.ops].sort(),
      stations: ce.stations, lat: ce.lat, lon: ce.lon,
    });
  }
  res.sort((a, b) => {
    const aN = a.name.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'');
    const bN = b.name.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'');
    return (aN.startsWith(q) ? 0 : 1) - (bN.startsWith(q) ? 0 : 1) || a.name.localeCompare(b.name,'fr');
  });
  return res;
}

// ─── Utilitaires ─────────────────────────────────────────────────────────────

function secondsToHHMM(s) {
  if (s == null || s === Infinity) return '--:--';
  const totalMin = Math.floor(s / 60);
  return String(Math.floor(totalMin / 60) % 24).padStart(2,'0') + ':' + String(totalMin % 60).padStart(2,'0');
}
function timeToSeconds(t) { const [h,m] = t.split(':').map(Number); return h*3600+m*60; }
function extractOperator(sid) { const m=(sid||'').match(/^([A-Z]+):/); return m?m[1]:'SNCF'; }

function resolveStopIds(ids, mode = 'origin') {
  const out      = new Set(ids);
  const inputSet = new Set(ids);

  for (const id of ids) {
    const areaMatch = id.match(/StopArea:OCE(\d{8})$/);
    if (areaMatch) {
      const uic = areaMatch[1];
      for (const sid of Object.keys(stops)) {
        if (sid.endsWith('-' + uic)) out.add(sid);
      }
    }
    for (const entry of transferEntries(id)) {
      const sister = entry.id;
      if (mode === 'dest') {
        if (inputSet.has(sister)) continue;          // déjà dans la liste groupée
        if (entry.interCity) continue;               // pas d'expansion inter-gares en dest
        const sameOp = extractOperator(id) === extractOperator(sister);
        if (!sameOp) continue;
      }
      out.add(sister);
    }
  }
  return [...out];
}

// ─── Calendrier ───────────────────────────────────────────────────────────────

function getActiveServices(dateISO) {
  if (!dateISO) return null;
  const s = calendarIndex[dateISO];
  return s ? new Set(s) : null;
}

const dateCache = new Map();

function getFilteredData(dateISO) {
  if (!dateISO) return { stopToTrips: globalStopToTrips };
  if (dateCache.has(dateISO)) return dateCache.get(dateISO);

  const active = getActiveServices(dateISO);
  if (!active) return { stopToTrips: globalStopToTrips };

  const filteredTrips = {};
  for (const [rid, trips] of Object.entries(routeTrips)) {
    const valid = trips.filter(t => active.has(t.service_id));
    if (valid.length) filteredTrips[rid] = valid;
  }
  const result = { stopToTrips: buildStopToTrips(filteredTrips) };
  if (dateCache.size >= 2) dateCache.delete(dateCache.keys().next().value);
  dateCache.set(dateISO, result);
  return result;
}

// ─── Détection type de train ──────────────────────────────────────────────────

function detectTrainTypeTI(tripId, routeId) {
  const route   = routesInfo[routeId] || {};
  const combined = ((route.long||'') + ' ' + (route.short||'')).toUpperCase();
  if (combined.includes('FRECCIAROSSA'))  return 'FRECCIAROSSA';
  if (combined.includes('EURONIGHT') || combined.includes('NOTTE')) return 'EURONIGHT';
  if (combined.includes('INTERCITY') || combined.includes('INTERCITES')) return 'IC_IT';
  if (combined.includes('REGIONALE')) return 'REGIONALE_IT';

  const raw = (tripId||'').replace(/^TI:/i,'').toUpperCase();
  if (raw.includes('FRECCIAROSSA') || /^FR\d/.test(raw) || /^9\d{3}/.test(raw)) return 'FRECCIAROSSA';
  if (raw.includes('EURONIGHT') || /^(EN|ICN)\d/.test(raw)) return 'EURONIGHT';
  if (/^IC\d/.test(raw)) return 'IC_IT';
  if (/^RV?\d/.test(raw)) return 'REGIONALE_IT';

  return 'FRECCIAROSSA';
}

function tiRouteName(trainType) {
  return { FRECCIAROSSA:'Frecciarossa', EURONIGHT:'Euronight',
           IC_IT:'Intercity', REGIONALE_IT:'Regionale' }[trainType] || 'Frecciarossa';
}

function tiAdjust(seconds, dateISO) {
  if (seconds == null) return seconds;
  if (dateISO) {
    const month = new Date(dateISO + 'T12:00:00Z').getUTCMonth() + 1;
    return seconds + (month >= 4 && month <= 9 ? 7200 : 3600);
  }
  return seconds + 3600;
}

function detectTrainType(fromStopId, tripId, stored, op, routeId) {
  if (stored) return stored;
  const operator = op || extractOperator(fromStopId);

  if (operator === 'TI') return detectTrainTypeTI(tripId, routeId);

  const tid  = (tripId || '').toUpperCase();
  const m    = (fromStopId || '').match(/StopPoint:OCE(.+)-\d{8}$/);
  const quai = m ? m[1].trim() : '';
  if (quai==='OUIGO' || tid.includes('OUIGO')) {
    const n   = (tripId || '').match(/^OCESN([47]\d{3})/);
    const num = n ? parseInt(n[1]) : null;
    if (num !== null) return Math.floor(num/1000)===7 ? 'OUIGO' : 'OUIGO_CLASSIQUE';
    return 'OUIGO';
  }
  if (quai==='TGV INOUI'         || tid.includes('INOUI'))      return 'INOUI';
  if (quai==='INTERCITES de nuit')                               return 'IC_NUIT';
  if (quai==='INTERCITES'        || tid.includes('INTERCITES'))  return 'IC';
  if (quai==='Lyria'             || tid.includes('LYRIA'))       return 'LYRIA';
  if (quai==='ICE')                                              return 'ICE';
  if (quai==='TramTrain')                                        return 'TRAMTRAIN';
  if (quai==='Car TER')                                          return 'CAR';
  if (quai==='Train TER')                                        return 'TER';
  if (quai==='Navette')                                          return 'NAVETTE';
  return 'TRAIN';
}

// ─── RAPTOR ───────────────────────────────────────────────────────────────────

function buildStopToTrips(tripsData) {
  const index = {};
  for (const [routeId, trips] of Object.entries(tripsData)) {
    for (const trip of trips) {
      for (let i = 0; i < trip.stop_times.length; i++) {
        const sid = trip.stop_times[i].stop_id;
        if (!index[sid]) index[sid] = [];
        index[sid].push({ routeId, trip, idx: i });
      }
    }
  }
  return index;
}

function scanTrip(trip, fromIdx, tauBest, tau_cur, parent, routeId, dateISO, originSet) {
  let boarded   = false;
  let boardStop = null;
  let boardDep  = null;

  const isTI      = trip.operator === 'TI';
  const tripOp    = trip.operator || 'SNCF';

  for (let i = fromIdx; i < trip.stop_times.length; i++) {
    const st  = trip.stop_times[i];
    const sid = st.stop_id;

    if (!boarded) {
      const tau = tauBest[sid];
      if (tau !== undefined) {
        const rawDep = st.dep_time ?? st.arr_time;
        const dep = (isTI && rawDep != null) ? tiAdjust(rawDep, dateISO) : rawDep;
        if (dep != null) {
          // Temps minimum avant d'embarquer :
          // - 0 si c'est une origine de départ (pas une correspondance)
          // - MIN_TRANSFER_SAME si même opérateur
          // - MIN_TRANSFER_CROSS si inter-opérateurs
          const isOrigin   = originSet ? originSet.has(sid) : false;
          const prevOp     = parent[sid]?.operator || extractOperator(sid);
          const sameOp     = prevOp === tripOp;
          const minWait    = isOrigin ? 0
                           : sameOp  ? MIN_TRANSFER_SAME
                           :           MIN_TRANSFER_CROSS;

          if (dep >= tau + minWait) {
            boarded   = true;
            boardStop = sid;
            boardDep  = dep;
          }
        }
      }
      continue;
    }

    const rawArr = st.arr_time ?? st.dep_time;
    const arr = (isTI && rawArr != null) ? tiAdjust(rawArr, dateISO) : rawArr;
    if (arr == null) continue;

    if (arr < (tauBest[sid] ?? Infinity)) {
      tauBest[sid]  = arr;
      tau_cur[sid]  = arr;
      parent[sid]   = {
        from_stop:  boardStop,
        trip_id:    trip.trip_id,
        route_id:   routeId,
        dep_time:   boardDep,
        arr_time:   arr,
        train_type: trip.train_type || null,
        operator:   tripOp,
      };
    }
  }
}

function raptorCore(originIds, destIds, startTime, stopToTripsData, dateISO, extraOrigins = null) {
  const tau_best  = {};
  const parent    = {};
  const originSet = new Set();
  let   marked    = new Set();

  for (const oid of originIds) {
    if ((tau_best[oid] ?? Infinity) > startTime) {
      tau_best[oid] = startTime;
      marked.add(oid);
    }
    originSet.add(oid);

    for (const entry of transferEntries(oid)) {
      const sister = entry.id;
      const t = startTime + transferTime(oid, entry);
      if (t < (tau_best[sister] ?? Infinity)) {
        tau_best[sister] = t;
        marked.add(sister);
        parent[sister] = { from_stop:oid, trip_id:null, route_id:null,
                           dep_time:startTime, arr_time:t, is_transfer:true };
      }
      // Les sisters interCity (autre gare de la même ville) ne sont PAS des origines
      // par défaut (un trajet qui repart d'une gare interCity compte une correspondance).
      // Exception : si extraOrigins contient cette gare (sélection ville explicite),
      // elle est une vraie origine — l'utilisateur veut partir de n'importe quelle gare.
      if (!entry.interCity || (extraOrigins && extraOrigins.has(sister))) originSet.add(sister);
    }
  }

  const results   = [];
  const destSet   = destIds ? new Set(destIds) : null;
  const collected = new Set();

  for (let round = 1; round <= MAX_ROUNDS; round++) {
    const tau_prev_round = { ...tau_best };
    const tau_cur        = {};
    const newMarked      = new Set();

    for (const stop of marked) {
      for (const { routeId, trip, idx } of (stopToTripsData[stop] || [])) {
        scanTrip(trip, idx, tau_best, tau_cur, parent, routeId, dateISO, originSet);
      }
    }

    for (const [sid, arr] of Object.entries(tau_cur)) {
      if (arr < (tau_prev_round[sid] ?? Infinity)) newMarked.add(sid);

      for (const entry of transferEntries(sid)) {
        const sister = entry.id;
        const t = arr + transferTime(sid, entry);
        if (t < (tau_best[sister] ?? Infinity)) {
          tau_best[sister] = t;
          tau_cur[sister]  = t;
          parent[sister]   = { from_stop:sid, trip_id:null, route_id:null,
                                dep_time:arr, arr_time:t, is_transfer:true };
          newMarked.add(sister);
        }
      }
    }
    marked = newMarked;

    if (destSet) {
      for (const did of destSet) {
        if (tau_cur[did] !== undefined && tau_cur[did] < (tau_prev_round[did] ?? Infinity)) {
          const j = reconstructJourney(parent, originSet, did, dateISO);
          if (j) {
            const key = j.legs.map(l => l.trip_id).join('|');
            if (!results.some(r => r.legs.map(l => l.trip_id).join('|') === key)) {
              results.push(j);
            }
          }
        }
      }
    } else {
      for (const sid of Object.keys(tau_cur)) {
        if (originSet.has(sid) || collected.has(sid)) continue;
        if (tau_cur[sid] < (tau_prev_round[sid] ?? Infinity)) {
          const j = reconstructJourney(parent, originSet, sid, dateISO);
          if (j) {
            collected.add(sid);
            results.push(j);
          }
        }
      }
    }

    if (marked.size === 0) break;
  }

  return results;
}

// ─── RAPTOR allégé pour /api/explore ─────────────────────────────────────────
// Version mémoire-minimale : pas de parent[], pas de legs[], pas de reconstruct.
// Retourne uniquement { stop_id, dep_time, arr_time, duration, transfers }.
// Sur Render free (512 MB) : ~10× moins de mémoire que raptorCore en mode explore.

function raptorExplore(originIds, startTime, stopToTripsData, dateISO, extraOrigins = null) {
  // tau[sid] = earliest arrival in seconds
  const tau      = {};
  // dep[sid] = departure time from origin (pour calculer la durée totale)
  const dep      = {};
  // xfr[sid] = nombre de correspondances (trains, pas transfers quai)
  const xfr      = {};
  const originSet = new Set();
  let   marked    = new Set();

  for (const oid of originIds) {
    tau[oid] = startTime;
    dep[oid] = startTime;
    xfr[oid] = 0;
    marked.add(oid);
    originSet.add(oid);

    for (const entry of transferEntries(oid)) {
      const sister = entry.id;
      const t = startTime + transferTime(oid, entry);
      if (t < (tau[sister] ?? Infinity)) {
        tau[sister] = t;
        dep[sister] = startTime;
        xfr[sister] = 0;
        marked.add(sister);
      }
      if (!entry.interCity || (extraOrigins && extraOrigins.has(sister))) originSet.add(sister);
    }
  }

  const isTI = (sid) => (sid||'').startsWith('TI:');

  for (let round = 1; round <= MAX_ROUNDS; round++) {
    const tau_cur  = {};
    const newMarked = new Set();

    // ── Scan des trips ──
    for (const stop of marked) {
      for (const { routeId, trip, idx } of (stopToTripsData[stop] || [])) {
        let boarded = false, boardDep = null, boardTau = null;
        const tripIsTI = trip.operator === 'TI';

        for (let i = idx; i < trip.stop_times.length; i++) {
          const st  = trip.stop_times[i];
          const sid = st.stop_id;

          if (!boarded) {
            const t = tau[sid];
            if (t !== undefined) {
              let rawDep = st.dep_time ?? st.arr_time;
              if (rawDep == null) continue;
              if (tripIsTI) rawDep = tiAdjust(rawDep, dateISO);
              // Temps minimum de correspondance avant embarquement
          const isOrig  = originSet.has(sid);
          const prevOp2 = (sid||'').startsWith('TI:') ? 'TI' : 'SNCF';
          const tripOp2 = tripIsTI ? 'TI' : 'SNCF';
          const minW    = isOrig ? 0
                        : prevOp2 === tripOp2 ? MIN_TRANSFER_SAME
                        :                       MIN_TRANSFER_CROSS;
          if (rawDep >= t + minW) {
                boarded  = true;
                boardDep = rawDep;
                boardTau = t;
              }
            }
            continue;
          }

          let rawArr = st.arr_time ?? st.dep_time;
          if (rawArr == null) continue;
          if (tripIsTI) rawArr = tiAdjust(rawArr, dateISO);

          if (rawArr < (tau[sid] ?? Infinity)) {
            tau[sid]     = rawArr;
            tau_cur[sid] = rawArr;
            // Le dep_time de l'origine du voyage entier
            dep[sid] = dep[stop] ?? startTime;
            // Transferts = round - 1 (chaque round = 1 train supplémentaire)
            xfr[sid] = round - 1;
          }
        }
      }
    }

    // ── Propagation des transferts ──
    for (const [sid, arr] of Object.entries(tau_cur)) {
      if (arr < (tau[sid] ?? Infinity)) newMarked.add(sid);  // déjà mis à jour ci-dessus

      for (const entry of transferEntries(sid)) {
        const sister = entry.id;
        const t = arr + transferTime(sid, entry);
        if (t < (tau[sister] ?? Infinity)) {
          tau[sister]     = t;
          tau_cur[sister] = t;
          dep[sister]     = dep[sid] ?? startTime;
          xfr[sister]     = xfr[sid] ?? 0;
          newMarked.add(sister);
        }
      }
    }

    marked = newMarked;
    if (marked.size === 0) break;
  }

  // ── Résultats minimalistes (pas de legs) ──
  const out = [];
  for (const sid of Object.keys(tau)) {
    if (originSet.has(sid)) continue;
    const arrTime = tau[sid];
    const depTime = dep[sid] ?? startTime;
    out.push({
      stop_id:   sid,
      dep_time:  depTime,
      arr_time:  arrTime,
      dep_str:   secondsToHHMM(depTime),
      arr_str:   secondsToHHMM(arrTime),
      duration:  Math.round((arrTime - depTime) / 60),
      transfers: xfr[sid] ?? 0,
    });
  }
  return out;
}
function resolveStopName(stopId) {
  return stopStationMap.get(stopId) || cleanStopName(stopId);
}

// Retourne la clé ville d'un stopId — O(1)
function cityKeyOfStop(stopId) {
  return stopCityKeyMap.get(stopId) || stopId;
}

function searchJourneys(originIds, destIds, startTime, stopToTripsData, limit, dateISO, allowedTypes = null, extraOrigins = null) {
  const seen    = new Set();
  const results = [];
  let t       = startTime;
  const maxT  = startTime + 14 * 3600;
  let noNewCount = 0;

  while (results.length < limit && t <= maxT) {
    const batch = raptorCore(originIds, destIds, t, stopToTripsData, dateISO, extraOrigins);

    let maxDepThis = -1;
    for (const j of batch) {
      if (j.dep_time < startTime) continue;
      if (allowedTypes && !j.train_types.some(tt => allowedTypes.has(tt))) continue;
      const key = j.legs.map(l => l.trip_id).join('|');
      if (!seen.has(key)) {
        seen.add(key);
        results.push(j);
        if (j.dep_time > maxDepThis) maxDepThis = j.dep_time;
      }
    }

    if (maxDepThis >= 0) {
      t = maxDepThis + 1;
      noNewCount = 0;
    } else {
      t += 1800;
      noNewCount++;
      if (noNewCount >= 4 && results.length > 0) break;
    }
  }

  results.sort((a, b) =>
    a.transfers - b.transfers ||
    a.duration  - b.duration  ||
    a.dep_time  - b.dep_time
  );

  // Dédupliquer par ville d'arrivée + heure de départ :
  // pour un même départ ET une même ville d'arrivée, ne garder que le plus rapide.
  // Cela évite d'avoir "arriver Montparnasse" ET "arriver Saint-Lazare" pour le même train.
  const dedupedByArrCity = new Map(); // "depTime:cityKey" → journey
  for (const j of results) {
    const lastStop  = j.legs[j.legs.length - 1].to_id;
    const cityKey   = cityKeyOfStop(lastStop);
    const dedupeKey = j.dep_time + ':' + cityKey;
    const existing  = dedupedByArrCity.get(dedupeKey);
    if (!existing || j.duration < existing.duration) {
      dedupedByArrCity.set(dedupeKey, j);
    }
  }

  return [...dedupedByArrCity.values()]
    .sort((a, b) => a.transfers - b.transfers || a.duration - b.duration || a.dep_time - b.dep_time)
    .slice(0, limit);
}

// ─── Reconstruction du journey ────────────────────────────────────────────────

function reconstructJourney(parent, originSet, destId, dateISO) {
  const legs    = [];
  let   current = destId;
  const visited = new Set();

  while (!originSet.has(current)) {
    if (visited.has(current)) return null;
    visited.add(current);

    const p = parent[current];
    if (!p) return null;

    if (p.is_transfer) {
      // Transfert inter-gares (interCity) en fin de trajet = inutile, on remonte
      // jusqu'au vrai arrêt de train
      current = p.from_stop;
      continue;
    }

    const op      = p.operator || extractOperator(p.from_stop);
    const isTI    = op === 'TI';
    const route   = routesInfo[p.route_id] || {};

    const depTime = p.dep_time;
    const arrTime = p.arr_time;

    const trainType = detectTrainType(p.from_stop, p.trip_id, p.train_type, op, p.route_id);

    const routeName = isTI
      ? tiRouteName(trainType)
      : (route.short || route.long || p.route_id);

    legs.unshift({
      from_id:    p.from_stop,
      to_id:      current,
      from_name:  resolveStopName(p.from_stop),
      to_name:    resolveStopName(current),
      dep_time:   depTime,
      arr_time:   arrTime,
      dep_str:    secondsToHHMM(depTime),
      arr_str:    secondsToHHMM(arrTime),
      trip_id:    p.trip_id,
      route_id:   p.route_id,
      route_name: routeName,
      operator:   op,
      train_type: trainType,
      duration:   Math.round((arrTime - depTime) / 60),
    });
    current = p.from_stop;
  }

  if (!legs.length) return null;
  const dep = legs[0].dep_time;
  const arr = legs[legs.length - 1].arr_time;
  return {
    dep_time:    dep,
    arr_time:    arr,
    dep_str:     secondsToHHMM(dep),
    arr_str:     secondsToHHMM(arr),
    duration:    Math.round((arr - dep) / 60),
    transfers:   legs.length - 1,
    train_types: [...new Set(legs.map(l => l.train_type).filter(Boolean))],
    legs,
  };
}

// ─── Tarifs ───────────────────────────────────────────────────────────────────

function normTransporteur(t) {
  const u = (t||'').toUpperCase();
  if (u.includes('CLASSIQUE')) return 'OUIGO_CLASSIQUE';
  if (u.includes('OUIGO'))     return 'OUIGO';
  if (u.includes('INOUI'))     return 'INOUI';
  return u;
}
function uicFromStopId(sid) {
  const m = (sid||'').match(/(\d{8})(?:[^0-9]|$)/); return m ? m[1] : null;
}
const TRAIN_TYPE_TO_TRANS = {
  'INOUI':['INOUI'], 'OUIGO':['OUIGO'], 'OUIGO_CLASSIQUE':['OUIGO_CLASSIQUE'],
};
function getTarifLeg(leg, profil='Tarif Normal', classe='2') {
  const uO = uicFromStopId(leg.from_id), uD = uicFromStopId(leg.to_id);
  if (!uO || !uD) return null;
  for (const tr of (TRAIN_TYPE_TO_TRANS[leg.train_type] || [])) {
    const k1 = uO+':'+uD+':'+tr+':'+classe+':'+profil;
    if (tarifIndex[k1]) return { ...tarifIndex[k1], transporteur:tr };
    const k2 = uD+':'+uO+':'+tr+':'+classe+':'+profil;
    if (tarifIndex[k2]) return { ...tarifIndex[k2], transporteur:tr };
  }
  return null;
}
function getTarifJourney(journey, profil='Tarif Normal', classe='2') {
  let min=0, max=0, hasTer=false, allFound=true;
  for (const leg of journey.legs) {
    const t = getTarifLeg(leg, profil, classe);
    if (!t) {
      if (!['OUIGO','OUIGO_CLASSIQUE','INOUI'].includes(leg.train_type)) hasTer = true;
      else allFound = false;
    } else { min += t.min; max += t.max; }
  }
  return { totalMin:min, totalMax:max, hasTer, allFound };
}

// ─── HTTP ─────────────────────────────────────────────────────────────────────

function cors(res) {
  res.setHeader('Access-Control-Allow-Origin','*');
  res.setHeader('Access-Control-Allow-Methods','GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers','Content-Type');
}
function jsonResp(res, data, status=200) {
  cors(res);
  res.writeHead(status, {'Content-Type':'application/json'});
  res.end(JSON.stringify(data));
}
function serveFile(res, fp) {
  if (!fs.existsSync(fp)) { res.writeHead(404); res.end('Not found'); return; }
  const mime = {'.html':'text/html','.js':'application/javascript','.css':'text/css','.svg':'image/svg+xml'};
  cors(res);
  res.writeHead(200, {'Content-Type': mime[path.extname(fp)] || 'text/plain'});
  fs.createReadStream(fp).pipe(res);
}
function getBody(req) {
  return new Promise(r => {
    let b = '';
    req.on('data', c => b += c);
    req.on('end', () => { try { r(JSON.parse(b)); } catch { r({}); } });
  });
}

const PROFILS = ['Tarif Normal','Tarif Avantage','Tarif Elève - Etudiant - Apprenti','Tarif Réglementé'];

const server = http.createServer(async (req, res) => {
  const parsed = url.parse(req.url, true);
  const p = parsed.pathname, q = parsed.query;

  if (req.method === 'OPTIONS') { cors(res); res.writeHead(204); res.end(); return; }

  // Répondre immédiatement aux pings keep-alive, même si l'engine charge encore
  if (p === '/eveille') {
    return jsonResp(res, {
      ok:        true,
      ready:     engineReady,
      uptime_s:  Math.floor(process.uptime()),
      loaded_at: engineLoadedAt,
      load_ms:   engineLoadMs,
      message:   engineReady ? '✅ Moteur opérationnel' : '⏳ Chargement en cours…',
    });
  }

  // Bloquer les routes API tant que l'engine n'est pas prêt
  if (p.startsWith('/api/') && !engineReady) {
    return jsonResp(res, {
      error:   'Serveur en cours de démarrage, réessayez dans quelques secondes.',
      ready:   false,
      load_ms: engineLoadMs,
    }, 503);
  }

  if (p === '/api/meta') {
    if (!engineReady) return jsonResp(res, { warming: true }, 503);
    return jsonResp(res, { ...meta, operators: meta.operators || ['SNCF','TI'] });
  }

  if (p === '/api/stops') {
    const qs = (q.q||'').trim();
    return jsonResp(res, qs ? searchStops(qs, 10) : []);
  }

  if (p === '/api/cities') {
    const qs = (q.q || '').trim();
    if (!qs || qs.length < 2) return jsonResp(res, []);
    return jsonResp(res, searchCities(qs));
  }

  if (p === '/api/search') {
    const t0 = Date.now();
    const fromIds = (q.from||'').split(',').filter(Boolean);
    const toIds   = (q.to||'').split(',').filter(Boolean);
    if (!fromIds.length || !toIds.length) return jsonResp(res, {error:'from et to requis'}, 400);

    const timeStr = q.time || '08:00';
    const dateStr = q.date || '';
    const profil  = PROFILS.includes(q.carte) ? q.carte : 'Tarif Normal';
    const offset  = parseInt(q.offset||'0');
    const afterDep= parseInt(q.after_dep||'0');
    const limit   = Math.min(parseInt(q.limit||'8'), 32);
    const startSec = Math.max(timeToSeconds(timeStr) + offset, afterDep || 0);

    const allowedTypes = q.train_types
      ? new Set(q.train_types.split(',').map(s => s.trim()).filter(Boolean))
      : null;

    const { stopToTrips: stt } = getFilteredData(dateStr);
    const fromCity   = q.fromCity === '1';  // sélection ville côté client
    const toCity     = q.toCity   === '1';
    const uniqueFrom = resolveStopIds([...new Set(fromIds)], 'origin');
    const uniqueTo   = resolveStopIds([...new Set(toIds)], 'dest');
    // Si sélection ville : toutes les gares envoyées sont des origines réelles
    const extraOrigins = fromCity ? new Set(fromIds) : null;

    console.log('\n[SEARCH]', dateStr || 'sans date', timeStr);
    console.log('  from IDs reçus   :', fromIds);
    console.log('  from IDs résolus :', uniqueFrom);
    console.log('  to   IDs reçus   :', toIds);
    console.log('  to   IDs résolus :', uniqueTo);
    console.log('  from dans stopToTrips :', uniqueFrom.filter(id => stt[id]).length, '/', uniqueFrom.length);
    console.log('  to   dans stopToTrips :', uniqueTo.filter(id => stt[id]).length, '/', uniqueTo.length);

    const journeys = searchJourneys(uniqueFrom, uniqueTo, startSec, stt, limit, dateStr, allowedTypes, extraOrigins);
    console.log('  Résultats :', journeys.length, journeys.map(j => j.dep_str + '->' + j.arr_str + ' (' + j.transfers + ' corresp)'));

    const lastDep   = journeys.length ? Math.max(...journeys.map(j => j.dep_time||0)) : startSec;
    const nextOffset = lastDep - timeToSeconds(timeStr);

    return jsonResp(res, {
      journeys,
      computed_ms:      Date.now()-t0,
      next_offset:      nextOffset,
      last_dep_time:    lastDep,
      profil_tarifaire: profil,
    });
  }

  if (p === '/api/tarifs' && req.method === 'POST') {
    const body = await getBody(req);
    const profil = PROFILS.includes(body.profil) ? body.profil : 'Tarif Normal';
    const tarifs = (body.journeys||[]).map(j => getTarifJourney(j, profil, body.classe||'2'));
    return jsonResp(res, { tarifs, profil });
  }

  if (p === '/api/explore') {
    const t0      = Date.now();
    const fromIds = (q.from||'').split(',').filter(Boolean);
    const dateStr = q.date || '';

    if (!fromIds.length) return jsonResp(res, { error: 'from requis' }, 400);

    console.log('\n[EXPLORE]', dateStr || 'sans date', '| from:', fromIds.slice(0,3).join(','));

    const { stopToTrips: stt } = getFilteredData(dateStr);
    const fromCity     = q.fromCity === '1';
    const uniqueFrom   = resolveStopIds([...new Set(fromIds)], 'origin');
    const originSet    = new Set(uniqueFrom);
    const extraOrigins = fromCity ? new Set(fromIds) : null;

    // Slot unique 07h00 — couvre la majorité des départs utiles.
    // Render free = 512 MB : on évite les 3 passes raptorCore + reconstruct complet.
    const startSec = timeToSeconds(q.time || '07:00');
    const reached  = raptorExplore(uniqueFrom, startSec, stt, dateStr, extraOrigins);

    // ── Étape 1 : meilleur trajet par stop_id ──────────────────────────────
    const bestByStop = {};
    for (const r of reached) {
      const sid = r.stop_id;
      if (originSet.has(sid)) continue;
      if (!bestByStop[sid] || r.duration < bestByStop[sid].duration) {
        bestByStop[sid] = r;
      }
    }

    // ── Étape 2 : consolider par coordonnées GPS arrondies ───────────────
    // Plusieurs stop_ids peuvent pointer sur la même gare physique (quais, opérateurs).
    // On arrondit lat/lon à 3 décimales (~100 m) et on ne garde que le trajet le plus court.
    // Cela réduit drastiquement les doublons (ex: 6× St Pancras → 1 point).
    const bestByGeo = {};
    for (const [sid, r] of Object.entries(bestByStop)) {
      const coords = globalCoordsMap.get(sid);
      const lat = coords?.lat || null;
      const lon = coords?.lon || null;
      if (!lat || !lon) continue;

      // Clé géographique arrondie à ~100 m
      const geoKey = lat.toFixed(3) + ':' + lon.toFixed(3);

      if (!bestByGeo[geoKey] || r.duration < bestByGeo[geoKey].duration) {
        bestByGeo[geoKey] = { ...r, lat, lon, sid,
          // Préférer le nom de station groupée (stopStationMap) plutôt que le stop brut
          name: stopStationMap.get(sid) || cleanStopName(sid)
        };
      }
    }

    // ── Étape 3 : construire la réponse ──────────────────────────────────
    const journeys = Object.values(bestByGeo).map(r => ({
      dep_time:  r.dep_time,
      arr_time:  r.arr_time,
      dep_str:   r.dep_str,
      arr_str:   r.arr_str,
      duration:  r.duration,
      transfers: r.transfers,
      dest_lat:  r.lat,
      dest_lon:  r.lon,
      legs: [{ to_id: r.sid, to_name: r.name }],
    }));

    console.log(`  → ${journeys.length} destinations | ${Date.now()-t0}ms`);
    return jsonResp(res, { journeys, computed_ms: Date.now()-t0 });
  }

  if (p === '/api/debug/trips') {
    const routeId = q.route;
    const stopId  = q.stop;
    const dateISO = q.date || '';

    if (routeId) {
      const tripsForRoute = routeTrips[routeId] || [];
      const active = dateISO ? getActiveServices(dateISO) : null;
      const filtered = active ? tripsForRoute.filter(t => active.has(t.service_id)) : tripsForRoute;
      const out = filtered.map(t => ({
        trip_id:    t.trip_id,
        service_id: t.service_id,
        stop_times: t.stop_times.map(st => ({
          stop_id:   st.stop_id,
          stop_name: cleanStopName(st.stop_id),
          dep:       secondsToHHMM(st.dep_time),
          arr:       secondsToHHMM(st.arr_time),
          dep_raw:   st.dep_time,
        })),
      }));
      return jsonResp(res, { route: routeId, date: dateISO||'sans filtre', info: routesInfo[routeId], trips: out });
    }

    if (stopId) {
      const { stopToTrips } = getFilteredData(dateISO);
      const entries = stopToTrips[stopId] || [];
      const out = entries.map(({ routeId, trip, idx }) => {
        const st = trip.stop_times[idx];
        return {
          route_id:   routeId,
          route_name: (routesInfo[routeId]?.long || routesInfo[routeId]?.short || '').slice(0, 60),
          trip_id:    trip.trip_id,
          service_id: trip.service_id,
          dep:        secondsToHHMM(st.dep_time ?? st.arr_time),
          dep_raw:    st.dep_time ?? st.arr_time,
        };
      }).sort((a, b) => (a.dep_raw ?? 0) - (b.dep_raw ?? 0));
      return jsonResp(res, { stop: stopId, stop_name: cleanStopName(stopId), date: dateISO||'sans filtre', departures: out });
    }

    return jsonResp(res, { error: 'Param route= ou stop= requis. Ex: /api/debug/trips?stop=TI:10007&date=2026-03-01' }, 400);
  }

  const staticMap = {'/':'index.html','/index.html':'index.html','/trajets.html':'trajets.html'};
  if (staticMap[p]) return serveFile(res, path.join(__dirname, staticMap[p]));

  const assetPath = path.join(__dirname, p);
  if (fs.existsSync(assetPath) && fs.statSync(assetPath).isFile()) return serveFile(res, assetPath);

  res.writeHead(404); res.end('Not found');
});

// ─── Démarrage ────────────────────────────────────────────────────────────────
// Le serveur écoute IMMÉDIATEMENT (Render considère le process prêt dès que
// le port est ouvert). L'engine se charge en arrière-plan : /eveille répond
// pendant ce temps, les autres routes retournent 503 jusqu'à engineReady=true.

server.listen(PORT, () => {
  console.log('🌐 http://localhost:' + PORT + '  (moteur en cours de chargement…)');
  try {
    initEngine();
  } catch (err) {
    engineError = err.message;
    console.error('❌ Échec chargement moteur :', err);
  }
});