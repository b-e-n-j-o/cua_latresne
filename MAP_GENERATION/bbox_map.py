# bbox_map.py
# -*- coding: utf-8 -*-
"""
Génération d'une carte HTML autonome (Leaflet) autour d'une parcelle + BBOX
- WFS parcelle via intersections_parcelle.locate_parcel_feature
- Fenêtre = buffer métrique autour de la parcelle (calcul PostGIS)
- Comptage, valeurs distinctes, et échantillon de features clippées à la BBOX
- Rendu HTML autonome (aucune dépendance folium)

API publique:
    build_map_html_bbox(eng, insee, section, numero4, mapping_path, ... ) -> str (chemin HTML)

Dépendances: sqlalchemy, PostGIS, intersections_parcelle (pour WFS + mapping)
"""

from __future__ import annotations
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

# Configuration des chemins d'import
import path_setup  # Configure automatiquement le Python path

# Importe les utilitaires du cœur depuis INTERSECTIONS/
from INTERSECTIONS.intersections_parcelle import (
    load_layer_map as _load_layer_map,
    locate_parcel_feature as _locate_parcel_feature,
)

log = logging.getLogger("bbox_map")
if not log.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# -----------------------------------------------------------------------------
# Styles (CNIG simplifiés) — tu peux étendre ici sans toucher les appels
# -----------------------------------------------------------------------------
STYLES_CONFIG: Dict[str, Any] = {
    "public.b_zonage_plu": {
        "type": "cnig_plu",
        "field": "typezone",
        "zoom_threshold": 18,
        "small": {
            "U":   {"fill": "rgba(230,0,0,0.6)", "stroke": "#343434", "weight": 0.8},
            "AUc": {"fill": "rgba(255,101,101,0.6)", "stroke": "#343434", "weight": 0.8},
            "AUs": {"fill": "rgba(254,204,190,0.6)", "stroke": "#343434", "weight": 0.8},
            "A":   {"fill": "rgba(255,255,0,0.6)", "stroke": "#343434", "weight": 0.8},
            "N":   {"fill": "rgba(86,170,2,0.6)", "stroke": "#343434", "weight": 0.8}
        },
        "large": {
            "U":   {"stroke": "#B00006", "understroke": "#343434", "dash": "5 2", "weight": 7},
            "AUc": {"stroke": "#D40006", "understroke": "#343434", "dash": "5 2", "weight": 7},
            "AUs": {"stroke": "#E88766", "understroke": "#343434", "dash": "5 2", "weight": 7},
            "A":   {"stroke": "#FFF000", "understroke": "#343434", "dash": "5 2", "weight": 7},
            "N":   {"stroke": "#23A600", "understroke": "#343434", "dash": "5 2", "weight": 7}
        },
        "label_field": "libelle"
    },
    "public.b_assiette_de_servitude_d_utilite_publique": {
        "type": "cnig_sup",
        "zoom_threshold": 18,
        "label_field": None
    }
}

# -----------------------------------------------------------------------------
# HTML autonome (Leaflet + UI)
# -----------------------------------------------------------------------------
HTML_TEMPLATE_BBOX = """<!doctype html>
<html lang="fr">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Intersections BBOX — Parcelle</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<style>
:root { --panel-bg:#fff; --panel-shadow:0 8px 22px rgba(0,0,0,.15); --border:#e5e7eb; --muted:#6b7280; }
html, body { height:100%; margin:0; font-family: ui-sans-serif,system-ui,-apple-system,"Segoe UI",Roboto,Arial; }
#map { position:absolute; inset:0; z-index:0; }
.panel { position:absolute; top:12px; left:12px; width:420px; max-height: calc(100% - 24px); overflow:auto;
         background:var(--panel-bg); border:1px solid var(--border); border-radius:12px; box-shadow:var(--panel-shadow); padding:10px; z-index:10000; }
.row { display:flex; align-items:center; gap:8px; justify-content:space-between; }
.small { color:var(--muted); font-size:12px; }
.badge { background:#f3f4f6; border:1px solid var(--border); border-radius:999px; padding:0 8px; font-size:12px; }
.chips { display:flex; flex-wrap:wrap; gap:6px; margin-top:6px; }
.chip { background:#eef2ff; color:#1e40af; border-radius:999px; padding:2px 8px; font-size:12px; }
details.layer { border:1px solid var(--border); border-radius:10px; padding:8px; margin-top:8px; }
details.layer summary { cursor:pointer; font-weight:600; }
.legend-dot { width:10px; height:10px; border-radius:999px; border:1px solid rgba(0,0,0,.2); display:inline-block; }
.controls { display:flex; gap:8px; flex-wrap:wrap; margin:8px 0 4px; }
.controls input { padding:6px 8px; border:1px solid var(--border); border-radius:8px; font-size:13px; width:110px;}
.btn { background:#111827; color:#fff; border:none; border-radius:8px; padding:6px 10px; font-size:13px; cursor:pointer; }
.btn.secondary { background:#f3f4f6; color:#111; border:1px solid var(--border); }
.divider { height:1px; background:var(--border); margin:8px 0; }
.sticky { position:sticky; top:0; background:var(--panel-bg); z-index:5; padding-bottom:6px; }
</style>
</head>
<body>
<div id="map"></div>
<div class="panel">
  <div class="sticky">
    <div class="row"><div><strong>Intersections BBOX</strong></div><div class="small">HTML autonome</div></div>
    <div class="small">Parcelle: <strong id="parcelLbl"></strong> — Buffer: <span id="bufTxt"></span> m</div>
    <div class="small">BBOX: <span id="bboxTxt"></span></div>
    <div class="controls">
      <button id="selectAll" class="btn">Tout cocher</button>
      <button id="clearAll"  class="btn secondary">Tout décocher</button>
      <input id="search" placeholder="filtrer par couche..."/>
    </div>
    <div class="divider"></div>
    <div class="small">Couches intersectantes <span id="hitCount" class="badge">0</span></div>
  </div>
  <div id="layers"></div>
</div>

<script type="application/json" id="data-json">{DATA_JSON}</script>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
const DATA = JSON.parse(document.getElementById('data-json').textContent);

const map = L.map('map');
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{ maxZoom: 22, attribution: '&copy; OSM' }).addTo(map);

const panelEl = document.querySelector('.panel');
L.DomEvent.disableClickPropagation(panelEl);
L.DomEvent.disableScrollPropagation(panelEl);

document.getElementById('parcelLbl').textContent = DATA.parcel.label;
document.getElementById('bufTxt').textContent = DATA.buffer_m.toFixed(0);
document.getElementById('bboxTxt').textContent = DATA.bbox.join(', ');

const [minx,miny,maxx,maxy] = DATA.bbox;
const bboxPoly = L.polygon([[miny,minx],[miny,maxx],[maxy,maxx],[maxy,minx]], { color:'#111', weight:2, dashArray:'6,6', fillOpacity:0 }).addTo(map);

// parcelle
if (DATA.parcel && DATA.parcel.geojson){
  const parcelLayer = L.geoJSON(DATA.parcel.geojson, {
    style: { color:'#0b5', weight:3, fillOpacity:0.12 }
  }).addTo(map);
  parcelLayer.bindPopup(`<strong>Parcelle</strong><br>${DATA.parcel.label}`);
  map.fitBounds(L.featureGroup([bboxPoly, parcelLayer]).getBounds(), { padding:[16,16] });
} else {
  map.fitBounds(bboxPoly.getBounds(), { padding:[16,16] });
}

const layerObjs = new Map();
function hashColor(name){ let h=0; for(let c of name) h=(h*31 + c.charCodeAt(0))>>>0; const r=(h&255),g=((h>>8)&255),b=((h>>16)&255); return `rgb(${r},${g},${b})`; }

function popupHTML(title, props){
  const head = `<div style="font-weight:600;margin-bottom:4px;">${title}</div>`;
  if(!props || Object.keys(props).length===0) return head + '<i>Pas de propriétés</i>';
  return head + Object.entries(props).map(([k,v])=>`<div><span style="color:#6b7280">${k}</span>: ${v ?? '<i>null</i>'}</div>`).join('');
}

function makePLULayer(item){
  const sty = item.styles || {};
  const field = sty.field || 'typezone';
  const Z = sty.zoom_threshold || 18;
  const small = sty.small || {};
  const large = sty.large || {};

  function styleSmall(f){
    const v = (f.properties||{})[field];
    const cat = small[v] || {};
    return {
      color: cat.stroke || '#343434',
      weight: cat.weight ?? 0.8,
      fillColor: cat.fill || '#ccc',
      fillOpacity: 0.6
    };
  }
  function styleUnder(f){
    const v = (f.properties||{})[field];
    const cat = large[v] || {};
    return { color: cat.understroke || '#343434', weight: 2.5, fillOpacity: 0, dashArray: null };
  }
  function styleOver(f){
    const v = (f.properties||{})[field];
    const cat = large[v] || {};
    return { color: cat.stroke || '#000', weight: (cat.weight || 7), fillOpacity: 0, dashArray: cat.dash || '5 2' };
  }

  const base = L.geoJSON(item.geojson, {
    style: styleSmall,
    onEachFeature: (f,l)=>{
      if (sty.label_field && f.properties && f.properties[sty.label_field]){
        l.bindTooltip(String(f.properties[sty.label_field]), {permanent:false, direction:'center'});
      }
      l.bindPopup(popupHTML(`${item.schema}.${item.table}`, f.properties||{}));
    }
  });

  const under = L.geoJSON(item.geojson, { style: styleUnder });
  const over  = L.geoJSON(item.geojson, { style: styleOver });

  const group = L.featureGroup([base, under, over]);

  function refresh(){
    const z = map.getZoom();
    const smallScale = (z < Z);
    base.setStyle(smallScale ? styleSmall : (_)=>({opacity:0, fillOpacity:0}));
    under.setStyle(smallScale ? (_)=>({opacity:0}) : styleUnder);
    over.setStyle(smallScale ? (_)=>({opacity:0}) : styleOver);
  }
  map.on('zoomend', refresh);
  refresh();
  return group;
}

function supCode(props){
  const raw = String((props && props.idass) || '').toLowerCase();
  return raw.split('-')[0] || '';
}
function supLibelle(props){
  const code = supCode(props);
  const typ = (props && (props.typeass || props.type)) || '';
  return (typ ? `${typ} — ` : '') + (code ? code.toUpperCase() : 'SUP');
}
function supColor(code){
  const fam = (code.match(/^[a-z]+/)||[''])[0];
  const palette = {
    'a':'#8B5E3C','ac':'#E67E22','ar':'#8E44AD','as':'#1ABC9C',
    'el':'#2980B9','i':'#C0392B','int':'#7F8C8D','js':'#F1C40F',
    'pm':'#D35400','pt':'#2C3E50','t':'#27AE60'
  };
  return palette[fam] || '#111';
}
function makeSUPLayer(item){
  const Z = (item.styles && item.styles.zoom_threshold) || 18;
  function styleSmall(f){
    const code = supCode(f.properties||{});
    return { color: supColor(code), weight: 1.2, fillOpacity: 0 };
  }
  function styleUnder(f){ return { color:'#343434', weight:2.5, fillOpacity:0 }; }
  function styleOver(f){
    const code = supCode(f.properties||{});
    return { color: supColor(code), weight:6, fillOpacity:0, dashArray:'6 3' };
  }
  const base  = L.geoJSON(item.geojson, {
    style: styleSmall,
    onEachFeature: (f,l)=>{
      const lbl = supLibelle(f.properties||{});
      l.bindTooltip(lbl, {sticky:true, direction:'top'});
      l.bindPopup(popupHTML(item.name || `${item.schema}.${item.table}`, Object.assign({}, f.properties, {__libelle: lbl})));
    }
  });
  const under = L.geoJSON(item.geojson, { style: styleUnder });
  const over  = L.geoJSON(item.geojson, { style: styleOver });
  const group = L.featureGroup([base, under, over]);
  function refresh(){
    const smallScale = (map.getZoom() < Z);
    base.setStyle(smallScale ? styleSmall : (_)=>({opacity:0, fillOpacity:0}));
    under.setStyle(smallScale ? (_)=>({opacity:0}) : styleUnder);
    over.setStyle(smallScale ? (_)=>({opacity:0}) : styleOver);
  }
  map.on('zoomend', refresh); refresh();
  return group;
}

function addLayerItem(item){
  const wrap = document.getElementById('layers');
  const full = `${item.schema}.${item.table}`;
  const displayName = item.name || full;
  const color = hashColor(full);
  const id = 'chk_' + full.replace(/[^a-z0-9_\\.]/gi,'_');
  const chips = [];
  if (item.values){
    for(const [col,vals] of Object.entries(item.values)){
      vals.slice(0, 12).forEach(v=> chips.push(`<span class="chip" title="${col}">${v}</span>`));
    }
  }
  const det = document.createElement('details');
  det.className = 'layer';
  det.innerHTML = `
    <summary>
      <label style="display:flex; align-items:center; gap:8px;">
        <input type="checkbox" id="${id}">
        <span class="legend-dot" style="background:${color}"></span>
        <span title="${full}">${displayName}</span>
        <span class="badge" title="features">${item.count}</span>
        <span class="small" title="mode d'aperçu" style="margin-left:auto;">${item.preview_mode}</span>
      </label>
    </summary>
    ${chips.length ? `<div class="chips">${chips.join('')}</div>` : ''}
  `;
  wrap.appendChild(det);

  document.getElementById(id).addEventListener('change', (e)=>{
    if (e.target.checked){
      let layer;
      if (item.styles && item.styles.type === 'cnig_plu'){
        layer = makePLULayer(item);
      } else if (item.styles && item.styles.type === 'cnig_sup'){
        layer = makeSUPLayer(item);
      } else {
        const gj = item.geojson;
        layer = L.geoJSON(gj, {
          style: f => ({ color, weight:2, fillOpacity: (f.geometry.type.includes('Polygon')?0.12:0) }),
          pointToLayer: (f, latlng)=> L.circleMarker(latlng, { radius:5, color }),
          onEachFeature: (f, l)=> l.bindPopup(popupHTML(displayName, f.properties || {}))
        });
      }
      layer.addTo(map);
      layerObjs.set(full, layer);
    } else {
      const layer = layerObjs.get(full);
      if (layer){ map.removeLayer(layer); layerObjs.delete(full); }
    }
  });
}

function refreshList(){
  const q = (document.getElementById('search').value || '').toLowerCase();
  const wrap = document.getElementById('layers');
  wrap.innerHTML = '';
  const items = DATA.layers.filter(it => it.count>0 && (
    `${it.schema}.${it.table}`.toLowerCase().includes(q) || 
    (it.name && it.name.toLowerCase().includes(q))
  ));
  document.getElementById('hitCount').textContent = items.length;
  items.forEach(addLayerItem);
}

document.getElementById('search').addEventListener('input', refreshList);
document.getElementById('selectAll').addEventListener('click', ()=>{
  document.querySelectorAll('#layers input[type="checkbox"]').forEach(chk=>{ if(!chk.checked) chk.click(); });
});
document.getElementById('clearAll').addEventListener('click', ()=>{
  document.querySelectorAll('#layers input[type="checkbox"]').forEach(chk=>{ if(chk.checked) chk.click(); });
});

refreshList();
</script>
</body>
</html>"""

# -----------------------------------------------------------------------------
# SQL (SRID-aware) — reprojection côté table + clip BBOX
# -----------------------------------------------------------------------------
COUNT_SQL_BBOX = """
SET LOCAL statement_timeout = '90s';
WITH env AS ( SELECT ST_MakeEnvelope(:minx,:miny,:maxx,:maxy, 4326) AS env4326 ),
env_t AS ( SELECT ST_Transform(env.env4326, :tsrid) AS env_t FROM env ),
cand AS (
  SELECT t.{geom_q} AS g
  FROM {qname} t, env_t e
  WHERE t.{geom_q} IS NOT NULL
    AND t.{geom_q} && e.env_t
)
SELECT COUNT(*)::bigint
FROM cand c
JOIN env_t e ON ST_Intersects(ST_MakeValid(c.g), e.env_t);
"""

VALUES_SQL_BBOX = """
SET LOCAL statement_timeout = '90s';
WITH env AS ( SELECT ST_MakeEnvelope(:minx,:miny,:maxx,:maxy, 4326) AS env4326 ),
env_t AS ( SELECT ST_Transform(env.env4326, :tsrid) AS env_t FROM env ),
cand AS (
  SELECT t.{geom_q} AS g, ({col_sql}::text) AS v
  FROM {qname} t, env_t e
  WHERE t.{geom_q} IS NOT NULL
    AND {col_sql} IS NOT NULL
    AND t.{geom_q} && e.env_t
)
SELECT DISTINCT c.v
FROM cand c
JOIN env_t e ON ST_Intersects(ST_MakeValid(c.g), e.env_t)
LIMIT :lim;
"""

FEATURES_SQL_BBOX = """
SET LOCAL statement_timeout = '120s';
WITH env AS ( SELECT ST_MakeEnvelope(:minx,:miny,:maxx,:maxy, 4326) AS env4326 ),
env_t AS ( SELECT ST_Transform(env.env4326, :tsrid) AS env_t FROM env ),
cand AS (
  SELECT t.{geom_q} AS g, {props_sql} AS props
  FROM {qname} t, env_t e
  WHERE t.{geom_q} IS NOT NULL
    AND t.{geom_q} && e.env_t
),
valid AS ( SELECT ST_MakeValid(c.g) AS g, c.props FROM cand c ),
clip AS (
  SELECT ST_Intersection(v.g, e.env_t) AS g, v.props
  FROM valid v, env_t e
  WHERE ST_Intersects(v.g, e.env_t)
)
SELECT ST_AsGeoJSON(
         CASE WHEN :simp > 0
              THEN ST_SimplifyPreserveTopology(ST_Transform(c.g, 4326), :simp)
              ELSE ST_Transform(c.g, 4326)
          END
       ) AS gj,
       c.props
FROM clip c
LIMIT :maxf;
"""

# -----------------------------------------------------------------------------
# Helpers SQL / PostGIS
# -----------------------------------------------------------------------------
def qident(*parts: str) -> str:
    return ".".join(f'"{p}"' for p in parts)

def list_existing_columns(engine: Engine, schema: str, table: str) -> List[str]:
    sql = """
    SELECT column_name FROM information_schema.columns
    WHERE table_schema=:schema AND table_name=:table
    ORDER BY ordinal_position;
    """
    with engine.begin() as con:
        rows = con.execute(text(sql), {"schema": schema, "table": table}).all()
    return [r[0] for r in rows]

def _props_sql(keep_cols: List[str]) -> str:
    if not keep_cols:
        return "NULL::jsonb"
    pairs = ", ".join([f"'{c}', t.\"{c}\"" for c in keep_cols])
    return f"jsonb_build_object({pairs})"

def get_table_srid(eng: Engine, schema: str, table: str, geom_col: str) -> int:
    q = "SELECT Find_SRID(:s,:t,:g)"
    with eng.begin() as con:
        srid = con.execute(text(q), {"s": schema, "t": table, "g": geom_col}).scalar()
    if not srid or int(srid) <= 0:
        q2 = f'SELECT ST_SRID("{geom_col}") FROM "{schema}"."{table}" WHERE "{geom_col}" IS NOT NULL LIMIT 1'
        with eng.begin() as con:
            srid = con.execute(text(q2)).scalar()
    return int(srid or 4326)

def buffer_bbox_from_parcel_geojson(eng: Engine, parcel_geom_geojson: dict, buffer_m: float) -> List[float]:
    q = """
    WITH p AS ( SELECT ST_SetSRID(ST_GeomFromGeoJSON(:gj), 4326) AS g ),
    buf AS ( SELECT ST_Transform(ST_Buffer(ST_Transform(g, 2154), :bufm), 4326) AS g FROM p )
    SELECT ST_XMin(g), ST_YMin(g), ST_XMax(g), ST_YMax(g) FROM buf;
    """
    with eng.begin() as con:
        row = con.execute(text(q), {"gj": json.dumps(parcel_geom_geojson), "bufm": float(buffer_m)}).first()
    if not row:
        raise RuntimeError("Impossible de calculer la BBOX du buffer.")
    return [float(row[0]), float(row[1]), float(row[2]), float(row[3])]

# -----------------------------------------------------------------------------
# Core: build_map_html_bbox
# -----------------------------------------------------------------------------
def build_map_html_bbox(
    *,
    eng: Engine,
    insee: str,
    section: str,
    numero4: str,
    mapping_path: str,
    buffer_m: float = 300.0,
    simplify: float = 0.0,
    max_features: int = 1500,
    schema_whitelist: Optional[List[str]] = None,
    out_html: str = "map_bbox_parcelle.html",
    styles_config: Optional[Dict[str, Any]] = None
) -> str:
    """
    Construit un HTML autonome visualisant la parcelle, sa BBOX (buffer) et des couches intersectantes.
    Retourne le chemin HTML écrit.
    """
    # 1) WFS parcelle
    feat = _locate_parcel_feature(insee, section, numero4)
    if not feat or not feat.get("geometry"):
        raise RuntimeError(f"Parcelle introuvable (WFS) — INSEE={insee}, {section} {numero4}")

    # 2) BBOX via buffer métrique
    bbox = buffer_bbox_from_parcel_geojson(eng, feat["geometry"], buffer_m=buffer_m)
    minx, miny, maxx, maxy = bbox

    # 3) Chargement mapping
    layers = _load_layer_map(mapping_path)
    if schema_whitelist:
        wl = set(schema_whitelist)
        layers = [l for l in layers if l["schema"] in wl]

    # 4) Pour chaque couche: count, valeurs distinctes, features clippées
    res_layers: List[Dict[str, Any]] = []
    for lyr in layers:
        schema = lyr["schema"]
        table = lyr["table"]
        geom_col = lyr.get("geom_col", "geom")
        existing = set(list_existing_columns(eng, schema, table))
        keep_cols = [c for c in lyr.get("keep", []) if c in existing]
        qname = qident(schema, table)
        geom_q = f'"{geom_col}"'
        tsrid = get_table_srid(eng, schema, table, geom_col)

        # COUNT
        with eng.begin() as con:
            n = int(con.execute(
                text(COUNT_SQL_BBOX.format(qname=qname, geom_q=geom_q)),
                {"minx": minx, "miny": miny, "maxx": maxx, "maxy": maxy, "tsrid": tsrid}
            ).scalar() or 0)
        if n <= 0:
            continue

        # DISTINCT VALUES
        vals_map: Dict[str, List[Any]] = {}
        for col in keep_cols:
            col_sql = f't."{col}"'
            with eng.begin() as con:
                rows = con.execute(
                    text(VALUES_SQL_BBOX.format(qname=qname, geom_q=geom_q, col_sql=col_sql)),
                    {"minx": minx, "miny": miny, "maxx": maxx, "maxy": maxy, "tsrid": tsrid, "lim": 100}
                ).all()
            vals_map[col] = [r[0] for r in rows if r[0] is not None]

        # FEATURES (clip)
        props_sql = _props_sql(keep_cols)
        with eng.begin() as con:
            rows = con.execute(
                text(FEATURES_SQL_BBOX.format(qname=qname, geom_q=geom_q, props_sql=props_sql)),
                {
                    "minx": minx, "miny": miny, "maxx": maxx, "maxy": maxy,
                    "tsrid": tsrid, "simp": float(simplify), "maxf": int(max_features)
                }
            ).all()

        feats = []
        for gj, props in rows:
            if not gj:
                continue
            try:
                feats.append({"type": "Feature", "geometry": json.loads(gj), "properties": props or {}})
            except Exception:
                # géométrie invalide JSON (rare) : on skippe
                continue

        table_key = f"{schema}.{table}"
        stycfg = (styles_config or STYLES_CONFIG).get(table_key, {})

        res_layers.append({
            "schema": schema,
            "table": table,
            "name": lyr.get("name", table_key),
            "count": n,
            "values": vals_map,
            "preview_mode": "features",
            "geojson": {"type": "FeatureCollection", "features": feats},
            "styles": stycfg
        })

    # 5) Payload → HTML autonome
    data = {
        "parcel": {"label": f"{section} {numero4}", "geojson": feat["geometry"]},
        "buffer_m": float(buffer_m),
        "bbox": [minx, miny, maxx, maxy],
        "layers": res_layers
    }
    payload = json.dumps(data, ensure_ascii=False)
    # anti </script> breakage
    payload = payload.replace("</", "<\\/")
    html = HTML_TEMPLATE_BBOX.replace("{DATA_JSON}", payload)

    out_path = Path(out_html)
    out_path.write_text(html, encoding="utf-8")
    log.info("🗺️  Carte BBOX écrite: %s (layers=%d)", out_path, len(res_layers))
    return str(out_path)

__all__ = ["build_map_html_bbox", "buffer_bbox_from_parcel_geojson", "get_table_srid", "STYLES_CONFIG"]
