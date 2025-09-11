# cua_service.py
# -*- coding: utf-8 -*-
import os, json, socket, logging, uuid, time
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

# ==== Tes modules existants (inchangés) ====
import cerfa_vision_pipeline as cerfa
from intersections_parcelle import (
    get_engine as _get_engine,
    load_layer_map as _load_layer_map,
    locate_parcel_feature as _locate_parcel_feature,
    _intersect_one_parcel as _intersect_one_parcel,
    get_insee_from_csv as _get_insee_from_csv,
)

# Rapport texte (optionnel)
try:
    from report_from_json import generate_text_report as _gen_text_report
except Exception:
    _gen_text_report = None

# Carte HTML (optionnel)
try:
    import folium
    _FOLIUM_OK = True
except Exception:
    _FOLIUM_OK = False

# ========================= Config globale ========================= #
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = Path.cwd() / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

MAPPING_JSON_PATH = os.getenv("MAPPING_JSON_PATH", "mapping_layers.json")
COMMUNES_CSV_PATH = os.getenv("COMMUNES_CSV_PATH", "v_commune_2025.csv")

log = logging.getLogger("cua.service")
if not log.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

# DOCX (optionnel)
try:
    from docx import Document
except Exception:
    Document = None


# ========================= Helpers “purs” ========================= #
def _first_nonempty(*vals):
    for v in vals:
        if v is not None and str(v).strip():
            return str(v).strip()
    return None

def _norm_section(s: Optional[str]) -> Optional[str]:
    if not s: return None
    return str(s).strip().upper().replace(" ", "") or None

def _num4(n: Optional[str]) -> Optional[str]:
    if n is None: return None
    n = str(n).strip()
    return n.zfill(4) if n else None

def _dedup_parcels(items: List[Tuple[str,str]]) -> List[Tuple[str,str]]:
    seen, out = set(), []
    for sec, num in items:
        t = (sec, num)
        if sec and num and t not in seen:
            seen.add(t); out.append(t)
    return out

def _dept_hint_from_pages(pages: List[Dict[str, Any]]) -> Optional[str]:
    for pj in pages or []:
        ta = pj.get("terrain_adresse") or {}
        cp = ta.get("code_postal") or (pj.get("coord_demandeur") or {}).get("adresse", {}).get("code_postal")
        if cp:
            cp_str = str(cp).strip()
            if len(cp_str) >= 2 and cp_str[:2].isdigit():
                return cp_str[:2]
        hdr = pj.get("header_cu")
        if isinstance(hdr, dict) and hdr.get("dept"):
            return str(hdr["dept"]).strip().upper()
    return None

def _parse_parcel_refs_inline(parcels_str: str):
    out = []
    for raw in (parcels_str or "").split(","):
        parts = raw.strip().split()
        if len(parts) != 2:
            continue
        sec = parts[0].upper()
        num4 = str(parts[1]).zfill(4)
        out.append((sec, num4))
    return out

def _label_to_slug(label: str) -> str:
    return label.replace(" ", "")

def _write_json(path: Path, obj: dict):
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

def _write_docx(text: str, path: Path) -> bool:
    if not Document:
        log.warning("python-docx non installé — DOCX non généré")
        return False
    doc = Document()
    for para in text.split("\n\n"):
        for line in para.split("\n"):
            doc.add_paragraph(line)
        doc.add_paragraph("")
    doc.save(path)
    return True

def _write_leaflet_map(geojson: dict, path: Path, title="Carte parcelle"):
    html = f"""<!doctype html>
<html><head>
<meta charset="utf-8"/>
<title>{title}</title>
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<style>html,body,#map{{height:100%;margin:0;}}</style>
</head><body>
<div id="map"></div>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
var map = L.map('map');
var tiles = L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{maxZoom: 20}});
tiles.addTo(map);
var parcel = {json.dumps(geojson, ensure_ascii=False)};
var layer = L.geoJSON(parcel, {{style: {{weight: 3}}}}).addTo(map);
map.fitBounds(layer.getBounds());
</script>
</body></html>"""
    path.write_text(html, encoding="utf-8")


# ========================= Blocs métiers ========================= #
def extract_inputs_from_vision(*, pdf_path: str, communes_csv: str) -> Dict[str, Any]:
    pdf = Path(pdf_path).resolve()
    out_json = Path("cerfa_vision_result.json")
    out_dir = Path("cerfa_pages_out")
    res = cerfa.run(pdf, out_json, out_dir)
    if not res.get("success"):
        raise RuntimeError(res.get("error"))

    data = res.get("data") or {}
    pages = res.get("pages", [])

    commune = (data.get("adresse_terrain") or {}).get("commune")
    if not _first_nonempty(commune):
        for pj in pages or []:
            ta = pj.get("terrain_adresse") or {}
            commune = ta.get("commune") or (pj.get("coord_demandeur") or {}).get("adresse", {}).get("commune")
            if _first_nonempty(commune):
                break
    if not _first_nonempty(commune):
        raise RuntimeError("Impossible de déterminer la commune depuis le CERFA")

    log.info("🔎 INSEE via CSV: commune=%s dept=None", commune)
    insee = _get_insee_from_csv(communes_csv, commune, None)
    if not insee:
        dept_hint = _dept_hint_from_pages(pages)
        log.info("🔎 INSEE via CSV: commune=%s dept=%s", commune, dept_hint)
        if dept_hint:
            insee = _get_insee_from_csv(communes_csv, commune, dept_hint)
    if not insee:
        raise RuntimeError(
            f"INSEE introuvable de façon univoque pour la commune '{commune}'. "
            "Corrige le nom de la commune ou fournis le département."
        )

    raw_refs = data.get("references_cadastrales") or []
    parcels = []
    for r in raw_refs:
        sec = _norm_section(r.get("section"))
        num = _num4(r.get("numero"))
        if sec and num:
            parcels.append((sec, num))
    parcels = _dedup_parcels(parcels)
    if not parcels:
        raise RuntimeError("Aucune référence cadastrale valide trouvée")

    return {"insee": insee, "parcels": parcels, "pages": pages}

def get_engine() -> Engine:
    return _get_engine()

def load_layer_map(path: str):
    return _load_layer_map(path)

def intersect_one_parcel_batch(
    *, eng: Engine, layers: List[Dict[str, Any]], insee: str,
    parcels: List[Tuple[str,str]], carve_enclaves: bool,
    enclave_buffer_m: float, values_limit: int
):
    reports, total_hits = [], 0
    for (sec, num4) in parcels:
        log.info("🌐 WFS IGN: %s %s %s", insee, sec, num4)
        feat = _locate_parcel_feature(insee, sec, num4)
        if not feat:
            log.warning("❌ Parcelle non trouvée: %s %s %s", insee, sec, num4)
            reports.append({
                "parcel": {"label": f"{sec} {num4}", "srid": 4326},
                "error": f"Parcelle non trouvée (INSEE {insee}, {sec} {num4})"
            })
            continue
        rpt = _intersect_one_parcel(
            eng=eng, layers=layers, parcel_feature=feat,
            carve_enclaves=carve_enclaves,
            enclave_buffer_m=enclave_buffer_m,
            values_limit=values_limit
        )
        total_hits += int(rpt.get("layers_with_hits", 0) or 0)
        reports.append(rpt)
    return reports, total_hits

def generate_markdown_report_safe(result: Dict[str, Any]) -> str:
    if _gen_text_report:
        txt = _gen_text_report(result).strip()
        return "\n\n".join(p.strip() for p in txt.split("\n\n")) + "\n"
    lines = [
        "# Rapport CUA",
        f"INSEE: {result['context']['insee']}",
        "Parcelles: " + ", ".join([f"{p['section']} {p['numero']}" for p in result['context']['parcelles']]),
        f"Total couches intersectantes: {result['layers_hit_total']}\n"
    ]
    for rep in result.get("reports", []):
        hdr = rep.get("parcel", {}).get("label", "??")
        lines.append(f"## Parcelle {hdr}")
        lines.append(f"- Couches avec hits: {rep.get('layers_with_hits','?')}")
        for r in rep.get("results", []):
            lines.append(f"### {r.get('schema')}.{r.get('table')} (n={r.get('count')})")
            vals = r.get("values") or {}
            for k, v in vals.items():
                if v:
                    preview = ", ".join(map(str, v[:8])) + ("…" if len(v) > 8 else "")
                    lines.append(f"- **{k}**: {preview}")
    return "\n".join(lines) + "\n"

def build_map_html(*, eng: Engine, result_json: Dict[str, Any], out_html: str, max_per_layer: int = 200):
    if not _FOLIUM_OK:
        raise RuntimeError("folium non installé (carte désactivée)")

    from sqlalchemy import text as _sqltext
    ctx = result_json.get("context", {})
    insee = ctx.get("insee")
    parcels = ctx.get("parcelles") or []
    if not parcels:
        raise RuntimeError("Aucune parcelle dans le contexte")

    sec = parcels[0]["section"]
    num = parcels[0]["numero"]
    feat = _locate_parcel_feature(insee, sec, num)
    if not feat or not feat.get("geometry"):
        raise RuntimeError("Impossible de récupérer la géométrie de la parcelle")

    gj = feat["geometry"]

    m = folium.Map(location=[46.5, 2.2], zoom_start=6, control_scale=True)
    try:
        folium.GeoJson(gj, name=f"Parcelle {sec} {num}", tooltip=f"Parcelle {sec} {num}").add_to(m)
        m.fit_bounds(folium.GeoJson(gj).get_bounds(), padding=(20,20))
    except Exception:
        pass

    for rep in (result_json.get("reports") or []):
        for layer in rep.get("results", []):
            schema, table, geom_col = layer["schema"], layer["table"], layer["geom_col"]
            qname = f'"{schema}"."{table}"'
            sql = f"""
            SET LOCAL statement_timeout='30s';
            WITH parcel AS (
              SELECT ST_SetSRID(ST_GeomFromGeoJSON(:gj), 4326) AS g
            )
            SELECT ST_AsGeoJSON(
                     ST_SimplifyPreserveTopology(
                       ST_CollectionExtract(ST_Intersection(t."{geom_col}", p.g), 3),
                       0.00005
                     )
                   ) AS gj
            FROM {qname} t, parcel p
            WHERE t."{geom_col}" IS NOT NULL
              AND t."{geom_col}" && p.g
              AND ST_Intersects(t."{geom_col}", p.g)
            LIMIT :lim;
            """
            try:
                with eng.begin() as con:
                    rows = con.execute(_sqltext(sql), {"gj": json.dumps(gj), "lim": int(max_per_layer)}).all()
                feats = []
                for r in rows:
                    if not r[0]:
                        continue
                    g = json.loads(r[0])
                    feats.append({"type": "Feature", "geometry": g, "properties": {"layer": f"{schema}.{table}"}})
                if feats:
                    folium.GeoJson({"type": "FeatureCollection", "features": feats},
                                   name=f"{schema}.{table}",
                                   tooltip=f"{schema}.{table}").add_to(m)
            except Exception:
                continue

    folium.LayerControl().add_to(m)
    html = m._repr_html_() if hasattr(m, "_repr_html_") else m.get_root().render()
    Path(out_html).write_text(html, encoding="utf-8")


# ========================= Façades “endpoint-ready” ========================= #
def health_check() -> Dict[str, Any]:
    info = {"ok": True, "dns": {}, "db": {}}
    host_env = os.getenv("SUPABASE_HOST")
    region = os.getenv("SUPABASE_REGION", "eu-west-3")
    project_ref = os.getenv("SUPABASE_PROJECT_REF")
    pooler = host_env or (f"aws-0-{region}.pooler.supabase.com" if region else None)
    direct = f"db.{project_ref}.supabase.co" if project_ref else None
    for h in [pooler, direct]:
        if not h:
            continue
        try:
            socket.getaddrinfo(h, None)
            info["dns"][h] = True
        except Exception:
            info["dns"][h] = False
            info["ok"] = False
    try:
        eng = get_engine()
        with eng.begin() as con:
            con.execute(text("select 1"))
        info["db"]["connect"] = True
    except Exception as e:
        info["db"]["connect"] = False
        info["db"]["error"] = str(e)
        info["ok"] = False
    return info

def run_cua_from_pdf(
    *,
    pdf_bytes: bytes,
    filename: str,
    schema_whitelist: Optional[str],
    values_limit: int,
    carve_enclaves: bool,
    enclave_buffer_m: float,
    make_report: bool,
    make_map: bool,
    max_features_per_layer_on_map: int
) -> Dict[str, Any]:
    if not Path(COMMUNES_CSV_PATH).exists():
        raise RuntimeError(f"COMMUNES_CSV_PATH introuvable: {COMMUNES_CSV_PATH}")
    if not Path(MAPPING_JSON_PATH).exists():
        raise RuntimeError(f"MAPPING_JSON_PATH introuvable: {MAPPING_JSON_PATH}")

    up_path = OUTPUT_DIR / f"upload_{filename}"
    up_path.write_bytes(pdf_bytes)

    vision = extract_inputs_from_vision(pdf_path=str(up_path), communes_csv=COMMUNES_CSV_PATH)
    insee = vision["insee"]
    parcels: List[Tuple[str,str]] = vision["parcels"]

    eng = get_engine()
    layers_all = load_layer_map(MAPPING_JSON_PATH)
    if schema_whitelist:
        allowed = set([s.strip() for s in schema_whitelist.split(",") if s.strip()])
        layers_all = [l for l in layers_all if l["schema"] in allowed]

    reports, total_hits = intersect_one_parcel_batch(
        eng=eng,
        layers=layers_all,
        insee=insee,
        parcels=parcels,
        carve_enclaves=bool(carve_enclaves),
        enclave_buffer_m=float(enclave_buffer_m),
        values_limit=int(values_limit)
    )

    result = {
        "success": True,
        "source": "CERFA -> intersections",
        "context": {
            "insee": insee,
            "nb_parcelles": len(parcels),
            "parcelles": [{"section": s, "numero": n} for s, n in parcels],
        },
        "layers_hit_total": total_hits,
        "reports": reports,
    }

    payload: Dict[str, Any] = {"ok": True, "result": result}

    # Rapport
    if make_report:
        md = generate_markdown_report_safe(result)
        md_name = f"rapport_{insee}_{parcels[0][0]}{parcels[0][1]}.md"
        md_path = OUTPUT_DIR / md_name
        md_path.write_text(md, encoding="utf-8")
        payload["report_markdown_url"] = f"/files/{md_name}"

    # Carte
    if make_map:
        if not _FOLIUM_OK:
            payload["map_error"] = "folium non installé (désactive make_map=0 ou installe folium)"
        else:
            html_name = f"map_{insee}_{parcels[0][0]}{parcels[0][1]}.html"
            html_path = OUTPUT_DIR / html_name
            build_map_html(
                eng=eng,
                result_json=result,
                out_html=str(html_path),
                max_per_layer=int(max_features_per_layer_on_map)
            )
            payload["map_html_url"] = f"/files/{html_name}"

    return payload

def run_cua_direct(
    *,
    parcel: str,
    insee: str = "33234",
    commune: str = "Latresne",
    mapping_path: Optional[str] = None,
    schema_whitelist: Optional[List[str]] = None,
    values_limit: int = 100,
    carve_enclaves: bool = True,
    enclave_buffer_m: float = 120.0,
    make_report: bool = True,
    make_map: bool = True
) -> Dict[str, Any]:
    eng = get_engine()
    mapping_json = mapping_path or str(MAPPING_JSON_PATH)
    layers_all = load_layer_map(mapping_json)
    if schema_whitelist:
        allowed = set(schema_whitelist)
        layers_all = [l for l in layers_all if l["schema"] in allowed]

    refs = _parse_parcel_refs_inline(parcel)
    if not refs:
        raise RuntimeError("Aucune parcelle valide. Format attendu: 'AC 0496' ou 'AC 0496, AC 0497'.")

    log.info("🔎 Direct CUA: %s parcelles, INSEE=%s", len(refs), insee)
    reports, total_hits = [], 0
    for (sec, num4) in refs:
        log.info("🌐 WFS IGN: %s %s %s", insee, sec, num4)
        feat = _locate_parcel_feature(insee, sec, num4)
        if not feat:
            log.warning("❌ Parcelle non trouvée: %s %s %s", insee, sec, num4)
            reports.append({
                "parcel": {"label": f"{sec} {num4}", "srid": 4326},
                "error": f"Parcelle non trouvée (INSEE {insee}, {sec} {num4})"
            })
            continue
        rpt = _intersect_one_parcel(
            eng=eng, layers=layers_all, parcel_feature=feat,
            carve_enclaves=bool(carve_enclaves),
            enclave_buffer_m=float(enclave_buffer_m),
            values_limit=int(values_limit)
        )
        total_hits += int(rpt.get("layers_with_hits", 0) or 0)
        reports.append(rpt)

    result = {
        "success": True,
        "source": "direct",
        "context": {
            "commune": commune,
            "insee": insee,
            "nb_parcelles": len(refs),
            "parcelles": [{"section": s, "numero": n} for s, n in refs],
        },
        "layers_hit_total": total_hits,
        "reports": reports
    }

    # Fichiers de sortie
    insee_tag = str(result["context"]["insee"])
    first_label = (result["reports"][0].get("parcel") or {}).get("label") if result["reports"] else "PARCELLE"
    label_slug = _label_to_slug(first_label or "PARCELLE")

    # JSON complet
    json_path = OUTPUT_DIR / f"result_{insee_tag}_{label_slug}.json"
    _write_json(json_path, result)
    result_json_url = f"/files/{json_path.name}"

    report_md_url = None
    report_docx_url = None
    if make_report:
        try:
            if _gen_text_report:
                report_text = _gen_text_report(result)
            else:
                report_text = generate_markdown_report_safe(result)
            md_path = OUTPUT_DIR / f"rapport_{insee_tag}_{label_slug}.md"
            md_path.write_text(report_text, encoding="utf-8")
            report_md_url = f"/files/{md_path.name}"
            if _write_docx(report_text, OUTPUT_DIR / f"rapport_{insee_tag}_{label_slug}.docx"):
                report_docx_url = f"/files/rapport_{insee_tag}_{label_slug}.docx"
        except Exception as e:
            log.exception(f"Generation rapport échouée: {e}")

    map_html_url = None
    if make_map:
        try:
            if refs:
                sec, num4 = refs[0]
                feat = _locate_parcel_feature(insee, sec, num4)
                gj = feat.get("geometry") if feat else None
                if gj:
                    map_path = OUTPUT_DIR / f"map_{insee_tag}_{label_slug}.html"
                    _write_leaflet_map(gj, map_path, title=f"Carte {first_label} ({insee_tag})")
                    map_html_url = f"/files/{map_path.name}"
        except Exception as e:
            log.exception(f"Generation carte échouée: {e}")

    return {
        "ok": True,
        "result": result,
        **{k: v for k, v in {
            "result_json_url": result_json_url,
            "report_markdown_url": report_md_url,
            "report_docx_url": report_docx_url,
            "map_html_url": map_html_url,
        }.items() if v}
    }
