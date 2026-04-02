#!/bin/bash
set -e

# =============================================================================
#  update-gtfs.sh
#  Télécharge, dézippe et filtre tous les GTFS, puis lance l'ingestion.
#
#  Dépendances (même dossier) :
#    filter_avanti.js        — filtre les données UK Rail → UK National
#    filter_germany.js       — filtre Allemagne FV (ICE · IC · EC · NJ)
#    gtfs-ingest.js          — ingestion RAPTOR multi-opérateurs
#    build-stations-index.js — index des stations
#    operators.json          — liste des opérateurs
#
#  Sections non-bloquantes (set -e suspendu localement) :
#    PARTIE 2b — TI_AV : skippée si la GitHub Release n'existe pas encore
# =============================================================================

TRANSITLAND_API_KEY="${TRANSITLAND_API_KEY:-iSQvk8H4v8dTBm5rACmwsV6gLqks8laM}"

# ─────────────────────────────────────────────────────────────────────────────
#  PARTIE 1 — UK Rail (transit.land) + filtrage Avanti
# ─────────────────────────────────────────────────────────────────────────────
echo "📥 Téléchargement UK Rail (transit.land)..."

mkdir -p ./gtfs/UK_Rail
mkdir -p ./gtfs/Avanti_Only

curl -k -L \
  "https://transit.land/api/v2/rest/feeds/f-uk~rail/download_latest_feed_version?apikey=${TRANSITLAND_API_KEY}" \
  -o /tmp/gtfs_uk_full.zip

if ! unzip -o /tmp/gtfs_uk_full.zip -d ./gtfs/UK_Rail > /dev/null 2>&1; then
  echo "⚠️  UK Rail ignoré — ZIP invalide (clé API invalide ?)"
  mkdir -p ./gtfs/UK_National
else
  echo "⚙️  Filtrage Avanti West Coast (VT)..."
  node filter_avanti.js || echo "⚠️  filter_avanti.js ignoré — données UK Rail absentes"
fi

# ─────────────────────────────────────────────────────────────────────────────
#  PARTIE 2 — Autres opérateurs (SNCF, Eurostar, Renfe…) via Node
# ─────────────────────────────────────────────────────────────────────────────
echo "📥 Téléchargement des autres GTFS..."

node --input-type=commonjs << 'ENDNODE'
const https        = require('https');
const fs           = require('fs');
const { execSync } = require('child_process');

const ops         = require('./operators.json');
const NAP_API_KEY = process.env.NAP_API_KEY || '5c51e865-2f81-4215-a1f0-3b73985a31fa';

// ─── Téléchargement via URL directe (curl) ────────────────────────────────────
function downloadDirect(op) {
  const dir = op.gtfs_dir;
  fs.mkdirSync(dir, { recursive: true });
  const tmp = '/tmp/gtfs_' + op.id + '.zip';
  console.log('  -> ' + op.id + ' (direct) : ' + op.gtfs_url);
  execSync('curl -L -s -o ' + tmp + ' "' + op.gtfs_url + '"');
  execSync('unzip -o ' + tmp + ' -d ' + dir + ' > /dev/null');
  console.log('  OK ' + op.id + ' extrait dans ' + dir);
}

// ─── Téléchargement via NAP espagnol (clé API requise) ───────────────────────
function downloadNAP(op) {
  return new Promise((resolve, reject) => {
    const dir = op.gtfs_dir;
    fs.mkdirSync(dir, { recursive: true });
    const tmp  = '/tmp/gtfs_' + op.id + '.zip';
    console.log('  -> ' + op.id + ' (NAP id=' + op.gtfs_nap_id + ')');

    const file    = fs.createWriteStream(tmp);
    const options = {
      hostname: 'nap.transportes.gob.es',
      path:     '/api/Fichero/download/' + op.gtfs_nap_id,
      method:   'GET',
      headers:  { 'ApiKey': NAP_API_KEY, 'accept': 'application/octet-stream' },
    };

    function get(opts) {
      https.get(opts, function(res) {
        if (res.statusCode === 301 || res.statusCode === 302) {
          console.log('     -> Redirection : ' + res.headers.location);
          return get(res.headers.location);
        }
        if (res.statusCode !== 200) return reject(new Error('NAP HTTP ' + res.statusCode));
        res.pipe(file);
        file.on('finish', function() {
          file.close();
          try {
            execSync('unzip -o ' + tmp + ' -d ' + dir + ' > /dev/null');
            console.log('  OK ' + op.id + ' extrait dans ' + dir);
            resolve();
          } catch(e) { reject(e); }
        });
        file.on('error', reject);
      }).on('error', reject);
    }

    get(options);
  });
}

// ─── Boucle principale ────────────────────────────────────────────────────────
(async function() {
  // Ignorer UK (Partie 1), DB_FV (Partie 3), NL (Partie 5) et TI_AV (Partie 2b — unzip spécial)
  const filtered = ops.filter(op => op.id !== 'UK' && op.id !== 'AVANTI' && op.id !== 'DB_FV' && op.id !== 'DB_RV' && op.id !== 'NL' && op.id !== 'TI_AV');

  for (const op of filtered) {
    try {
      if (op.gtfs_url) {
        downloadDirect(op);
      } else if (op.gtfs_nap_id) {
        await downloadNAP(op);
      } else {
        console.log('  SKIP ' + op.id + ' : aucune source configurée.');
      }
    } catch(err) {
      console.error('  ERREUR ' + op.id + ' : ' + err.message);
      process.exit(1);
    }
  }
})();
ENDNODE

# ─────────────────────────────────────────────────────────────────────────────
#  PARTIE 2b — Trenitalia Italia AV+IC (raw GitHub)
#  URL lue depuis operators.json (champ gtfs_url de l'entrée id=TI_AV)
#  → extraction directe dans ./gtfs/trenitalia_it_api/
#  Non bloquant : skip propre si le fichier est inaccessible
# ─────────────────────────────────────────────────────────────────────────────
echo "📥 Téléchargement Trenitalia Italia AV+IC (GitHub raw)..."

mkdir -p ./gtfs/trenitalia_it_api

# Lire l'URL depuis operators.json (source unique de vérité)
TI_AV_URL=$(node --input-type=commonjs -e "const ops=require('./operators.json'); const op=ops.find(o=>o.id==='TI_AV'); console.log(op ? op.gtfs_url : '');")
TI_AV_ZIP="/tmp/gtfs_ti_av.zip"

echo "  URL : $TI_AV_URL"

if [ -z "$TI_AV_URL" ]; then
  echo "  ⚠️  TI_AV ignoré — pas de gtfs_url dans operators.json"
else
  set +e  # section non-bloquante — une erreur ici ne stoppe pas le deploy

  HTTP_CODE=$(curl -L -s -w "%{http_code}" -o "$TI_AV_ZIP" "$TI_AV_URL")

  if [ "$HTTP_CODE" != "200" ]; then
    echo "  ⚠️  TI_AV ignoré — fichier non disponible (HTTP $HTTP_CODE)"
    echo "      URL : $TI_AV_URL"
  elif ! unzip -t "$TI_AV_ZIP" > /dev/null 2>&1; then
    echo "  ⚠️  TI_AV ignoré — fichier téléchargé invalide (pas un ZIP valide)"
    echo "      HTTP_CODE=$HTTP_CODE — contenu reçu :"
    head -c 300 "$TI_AV_ZIP" | cat
  else
    unzip -o "$TI_AV_ZIP" -d ./gtfs/trenitalia_it_api > /dev/null
    echo "  ✅ TI_AV extrait dans ./gtfs/trenitalia_it_api/"
    echo "     Fichiers : $(ls ./gtfs/trenitalia_it_api/*.txt 2>/dev/null | wc -l) .txt"
    echo "     Contenu  : $(ls ./gtfs/trenitalia_it_api/*.txt 2>/dev/null | xargs -I{} basename {} | tr '\n' ' ')"
    if [ -f ./gtfs/trenitalia_it_api/stops.txt ]; then
      echo "     stops.txt : $(wc -l < ./gtfs/trenitalia_it_api/stops.txt) lignes"
      echo "     Exemple   : $(head -2 ./gtfs/trenitalia_it_api/stops.txt | tail -1 | cut -c1-80)"
    else
      echo "  ⚠️  stops.txt absent du ZIP — structure inattendue"
      echo "     Contenu ZIP : $(unzip -l "$TI_AV_ZIP" | head -15)"
    fi
  fi

  set -e  # reprendre le mode strict
fi

# ─────────────────────────────────────────────────────────────────────────────
#  PARTIE 3 — Allemagne Fernverkehr (ICE · IC · EC · NJ)
# ─────────────────────────────────────────────────────────────────────────────
echo "📥 Téléchargement Allemagne Fernverkehr (ICE · IC · EC · NJ)..."
mkdir -p ./gtfs/db_fv
curl -L -s \
  "https://download.gtfs.de/germany/fv_free/latest.zip" \
  -o /tmp/gtfs_db_fv.zip
unzip -o /tmp/gtfs_db_fv.zip -d ./gtfs/db_fv > /dev/null

echo "⚙️  Filtrage Allemagne FV (exclusion non-ferroviaire)..."
node filter_germany.js

# ─────────────────────────────────────────────────────────────────────────────
#  PARTIE 4 — Ingestion RAPTOR + index stations
# ─────────────────────────────────────────────────────────────────────────────
echo "⚙️  Ingestion GTFS -> engine_data..."
node gtfs-ingest.js

echo "🗺️  Construction index stations..."
node build-stations-index.js

echo "✅ Mise à jour terminée."