# -*- coding: utf-8 -*-
"""
Intersections PARCELLE (IGN) → Supabase/PostGIS (whitelist d'attributs)

- Résout une parcelle cadastrale officielle française (commune + département → INSEE,
  puis WFS IGN parcellaire pour la géométrie la plus précise possible).
- Intersecte la géométrie de la parcelle avec toutes les couches listées dans mapping.json.
- Ne renvoie que les attributs de la whitelist ("keep") pour chaque couche.
- Génère un rapport JSON "léger" (counts + valeurs distinctes), sans carte HTML.

Dépendances: sqlalchemy, psycopg2-binary, python-dotenv, pandas, requests
"""

import os, json, argparse, logging, time
from typing import Optional, List, Dict, Any, Tuple
from urllib.parse import urlparse, quote_plus
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

import pandas as pd
import requests

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

def _ms(t0: float) -> float:
    return (time.perf_counter() - t0) * 1000.0

# ADD en haut des imports
from .enclaves import detect_and_carve_enclaves  # nouveau module

# ======================= Constantes =======================
IGN_WFS = "https://data.geopf.fr/wfs/ows"
LAYER_PARCELLE = "CADASTRALPARCELS.PARCELLAIRE_EXPRESS:parcelle"

# ======================= Connexion DB =======================
def _project_ref_from_url(supabase_url: Optional[str]) -> Optional[str]:
    if not supabase_url:
        return None
    try:
        netloc = urlparse(supabase_url).netloc
        return (netloc.split(".")[0] or None)
    except Exception:
        return None

def get_engine() -> Engine:
    load_dotenv()
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        supa_url = os.getenv("SUPABASE_URL")
        pref = _project_ref_from_url(supa_url) or os.getenv("SUPABASE_PROJECT_REF")
        host = os.getenv("SUPABASE_HOST")
        user = os.getenv("SUPABASE_USER")
        pwd  = os.getenv("SUPABASE_PASSWORD")
        db   = os.getenv("SUPABASE_DB", "postgres")
        port = int(os.getenv("SUPABASE_PORT", "5432"))
        if pref and (not host or ".pooler.supabase.com" not in host):
            host = "aws-0-eu-west-3.pooler.supabase.com"
            if not user:
                user = f"postgres.{pref}"
        if all([host, user, pwd, db, port]):
            dsn = f"postgresql+psycopg2://{user}:{quote_plus(pwd)}@{host}:{port}/{db}?sslmode=require"
    if not dsn:
        raise RuntimeError("DATABASE_URL (ou SUPABASE_*) non défini.")
    eng = create_engine(dsn, pool_pre_ping=True, pool_recycle=300, connect_args={"connect_timeout": 20})
    # Sanity check
    with eng.begin() as con:
        con.execute(text("select 1"))
    return eng

# ======================= Utils génériques =======================
def qident(*parts) -> str:
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

def load_layer_map(path: str) -> List[Dict[str, Any]]:
    raw = json.load(open(path, "r", encoding="utf-8"))
    layers = []
    for fq, entry in raw.items():
        if "." not in fq:
            continue
        schema, table = fq.split(".", 1)
        geom_col = entry.get("geom", "geom")
        keep = entry.get("keep", [])
        coverage_by = entry.get("coverage_by", [])  # 👈 nouveau
        if isinstance(coverage_by, str):  # compat backward si jamais string
            coverage_by = [c.strip() for c in coverage_by.split(",") if c.strip()]
        layers.append({
            "schema": schema,
            "table": table,
            "geom_col": geom_col,
            "keep": keep,
            "coverage_by": coverage_by,
        })
    return layers

# ======================= Normalisation texte =======================
def _normalize_string(text: str) -> str:
    if text is None:
        return ""
    s = str(text).lower()
    s = (s.replace('é','e').replace('è','e').replace('ê','e')
           .replace('à','a').replace('â','a')
           .replace('ô','o')
           .replace('ù','u').replace('û','u')
           .replace('ï','i').replace('î','i')
           .replace('ç','c')
           .replace('-', ' ').replace("'", ' '))
    return ' '.join(s.split()).strip()

# ======================= INSEE via CSV local =======================
def get_insee_from_csv(csv_path: str, commune_name: str, department_code: Optional[str]) -> Optional[str]:
    """
    CSV attendu (INSEE 2025) avec colonnes comme: TYPECOM, COM, REG, DEP, NCCENR, LIBELLE, ...
    Retourne le code INSEE 'COM' s'il y a correspondance unique sur nom + dép (si fourni).
    """
    try:
        df = pd.read_csv(csv_path, sep=",", dtype=str)
    except Exception as e:
        raise RuntimeError(f"Impossible de lire le CSV communes: {e}")

    if df.empty or "COM" not in df.columns:
        raise RuntimeError("CSV communes invalide: colonne 'COM' absente.")

    name_norm = _normalize_string(commune_name)
    have_label = "LIBELLE" in df.columns
    have_nccenr = "NCCENR" in df.columns
    if have_label:  df["LIBELLE_NORM"] = df["LIBELLE"].apply(_normalize_string)
    if have_nccenr: df["NCCENR_NORM"] = df["NCCENR"].apply(_normalize_string)

    mask = False
    if have_label:  mask = (df["LIBELLE_NORM"] == name_norm)
    if have_nccenr: mask = mask | (df["NCCENR_NORM"] == name_norm)
    df2 = df[mask] if mask is not False else pd.DataFrame()

    if department_code and "DEP" in df.columns and not df2.empty:
        dep = str(department_code).upper()
        if dep.isdigit() and len(dep) == 1:
            dep = dep.zfill(2)  # "3" -> "03"
        df2 = df2[df2["DEP"].str.upper() == dep]

    if df2.empty:
        return None
    if len(df2) != 1:
        # Ambigu (ex: communes homonymes)
        return None

    return str(df2.iloc[0]["COM"])

# ======================= WFS Parcellaire IGN =======================
def _build_wfs_url(params: dict) -> str:
    from urllib.parse import urlencode
    return f"{IGN_WFS}?{urlencode(params)}"

def _parse_parcel_refs(parcels_str: str):
    """
    "AD 0598, AC 0042" -> [("AD", "0598"), ("AC","0042")]
    """
    out = []
    for raw in (parcels_str or "").split(","):
        parts = raw.strip().split()
        if len(parts) != 2:
            continue
        out.append( (parts[0].upper(), str(parts[1]).zfill(4)) )
    return out

def locate_parcel_feature(insee_code: str, section: str, numero4: str, timeout: int = 30) -> dict:
    import os
    from urllib.parse import urlencode
    cql = f"code_insee='{insee_code}' AND section='{section}' AND numero='{numero4}'"
    params = {
        "service": "WFS", "version": "2.0.0", "request": "GetFeature",
        "typeName": LAYER_PARCELLE, "outputFormat": "application/json",
        "count": 1, "cql_filter": cql, "srsName": "EPSG:4326",
    }
    url = f"{IGN_WFS}?{urlencode(params)}"
    if os.getenv("WFS_DEBUG") == "1":
        print("WFS URL:", url)
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    feats = (r.json().get("features") or [])
    return feats[0] if feats else {}

# ======================= SQL (intersections avec PARCELLE) =======================
# COMPTE les entités qui intersectent la parcelle (optimisé index GiST)
COUNT_SQL_PARCEL = """
SET LOCAL statement_timeout = '30s';
WITH p AS (
  SELECT ST_Transform(
           ST_SetSRID(ST_GeomFromGeoJSON(:gj), 4326),
           2154
         ) AS g
)
SELECT COUNT(*)::bigint
FROM {qname} t, p
WHERE t.geom_2154 IS NOT NULL
  AND t.geom_2154 && ST_Envelope(p.g)
  AND ST_Intersects(t.geom_2154, p.g);
"""

# VALEURS DISTINCTES des attributs "utiles" (optimisé index GiST)
VALUES_SQL_PARCEL = """
SET LOCAL statement_timeout = '30s';
WITH p AS (
  SELECT ST_Transform(
           ST_SetSRID(ST_GeomFromGeoJSON(:gj), 4326),
           2154
         ) AS g
)
SELECT DISTINCT ({col_sql}::text) AS v
FROM {qname} t, p
WHERE t.geom_2154 IS NOT NULL
  AND {col_sql} IS NOT NULL
  AND t.geom_2154 && ST_Envelope(p.g)
  AND ST_Intersects(t.geom_2154, p.g)
LIMIT :lim;
"""

# SURFACES d’intersection par entité (et surface de la parcelle) — optimisé
AREA_SQL_PARCEL = """
SET LOCAL statement_timeout = '30s';
WITH p AS (
  SELECT ST_Transform(
           ST_SetSRID(ST_GeomFromGeoJSON(:gj), 4326),
           2154
         ) AS g
)
SELECT
  {col_sql} AS id,
  ST_Area(
    ST_CollectionExtract(
      ST_Intersection(t.geom_2154, p.g), 3
    )
  ) AS inter_area_m2,
  ST_Area(p.g) AS parcel_area_m2
FROM {qname} t, p
WHERE t.geom_2154 IS NOT NULL
  AND t.geom_2154 && ST_Envelope(p.g)
  AND ST_Intersects(t.geom_2154, p.g);
"""


def _intersect_one_parcel(*, eng, layers, parcel_feature, carve_enclaves: bool, enclave_buffer_m: float, values_limit: int):
    # 1) géométrie JSON pour SQL
    props = parcel_feature.get("properties") or {}
    section = props.get("section", "??")
    numero  = props.get("numero", "????")
    label = f"{section} {numero}"
    logger.info("🔎 Parcelle %s — enclaves=%s", label, "ON" if carve_enclaves else "OFF")
    if carve_enclaves:
        info = detect_and_carve_enclaves(parcel_feature, buffer_m=float(enclave_buffer_m))
        gj = info["host_corrected_geojson_4326"]
        st = info.get("stats", {})
        holes = info.get("holes_in_host", {}) or {}
        parcel_extra = {
            "enclaves": {
                "count": st.get("enclave_count", 0),
                "carved_effective_area_m2": st.get("carved_effective_area_m2", 0.0),
                "host_area_m2": st.get("host_area_m2"),
                "host_corrected_area_m2": st.get("host_corrected_area_m2"),
                "carve_consistency_delta_m2": st.get("carve_consistency_delta_m2"),
            },
            "holes_in_host": {
                "count": holes.get("count", 0),
                "area_m2": holes.get("area_m2", 0.0),
            }
        }
    else:
        gj = parcel_feature.get("geometry")
        parcel_extra = {}

    parcel_geom_json = json.dumps(gj, ensure_ascii=False)

    results_report = []
    hit_count = 0

    for lyr in layers:
        schema, table = lyr["schema"], lyr["table"]
        qname = qident(schema, table)
        layer_tag = f"{schema}.{table}"

        # 👉 utilisation de la colonne métrique indexée
        geom_sql = 't."geom_2154"'
        keep_effective = [c for c in lyr.get("keep", []) if c in set(list_existing_columns(eng, schema, table))]

        t_layer = time.perf_counter()
        logger.info("→ Couche %s (geom=geom_2154)", layer_tag)

        try:
            # COUNT
            t0 = time.perf_counter()
            with eng.begin() as con:
                n = int(con.execute(
                    text(COUNT_SQL_PARCEL.format(qname=qname)),
                    {"gj": parcel_geom_json}
                ).scalar() or 0)
            logger.info("   COUNT: %d (%.1f ms)", n, _ms(t0))
            if n <= 0:
                logger.info("   ✖ Aucun intersect — skip (%.1f ms total)", _ms(t_layer))
                continue

            # VALUES (whitelist)
            t0 = time.perf_counter()
            vals_map = {}
            total_vals = 0
            for col in keep_effective:
                col_sql = f't."{col}"'
                with eng.begin() as con:
                    rows = con.execute(
                        text(VALUES_SQL_PARCEL.format(qname=qname, col_sql=col_sql)),
                        {"gj": parcel_geom_json, "lim": int(values_limit)}
                    ).all()
                vals = [r[0] for r in rows if r[0] is not None]
                vals_map[col] = vals
                total_vals += len(vals)
            logger.info("   VALUES: %d colonnes, %d valeurs (%.1f ms)", len(keep_effective), total_vals, _ms(t0))

            # AREA (surfaces par entité)
            parcel_area_m2 = None
            surfaces = []
            id_col = lyr.get("id_col", "id")
            existing_cols = set(list_existing_columns(eng, schema, table))
            if id_col in existing_cols:
                id_sql = f't."{id_col}"'
            else:
                id_sql = 'ROW_NUMBER() OVER()::text'
                logger.warning("   ⚠ Colonne '%s' absente dans %s, usage ROW_NUMBER()", id_col, layer_tag)

            t0 = time.perf_counter()
            with eng.begin() as con:
                rows = con.execute(
                    text(AREA_SQL_PARCEL.format(qname=qname, col_sql=id_sql)),
                    {"gj": parcel_geom_json}
                ).mappings().all()
            for r in rows:
                if parcel_area_m2 is None:
                    parcel_area_m2 = float(r["parcel_area_m2"] or 0)
                inter_area = float(r["inter_area_m2"] or 0)
                if inter_area > 0:
                    surfaces.append({
                        "id": r["id"],
                        "inter_area_m2": inter_area,
                        "pct_of_parcel": inter_area / parcel_area_m2 * 100 if parcel_area_m2 else None
                    })
            logger.info("   AREA: %d intersections, parcelle=%.1f m² (%.1f ms)", len(surfaces), parcel_area_m2 or 0.0, _ms(t0))

            # COVERAGE (coverage_by)
            coverage_results = {}
            if lyr.get("coverage_by"):
                for cov_col in lyr["coverage_by"]:
                    t_cov = time.perf_counter()
                    with eng.begin() as con:
                        q = f"""
                        SET LOCAL statement_timeout = '30s';
                        WITH p AS (
                          SELECT ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(:gj), 4326), 2154) AS g
                        ),
                        agg AS (
                          SELECT t."{cov_col}"::text AS v,
                                 SUM(ST_Area(ST_CollectionExtract(ST_Intersection(t.geom_2154, p.g), 3))) AS inter_area_m2
                          FROM {qname} t, p
                          WHERE t.geom_2154 IS NOT NULL
                            AND t."{cov_col}" IS NOT NULL
                            AND t.geom_2154 && ST_Envelope(p.g)
                            AND ST_Intersects(t.geom_2154, p.g)
                          GROUP BY t."{cov_col}"
                        )
                        SELECT v, inter_area_m2
                        FROM agg
                        WHERE inter_area_m2 > 0
                        ORDER BY inter_area_m2 DESC
                        """
                        rows = con.execute(text(q), {"gj": parcel_geom_json}).mappings().all()

                    cov_list = []
                    for r in rows:
                        inter_area = float(r["inter_area_m2"] or 0)
                        if inter_area > 0 and parcel_area_m2:
                            cov_list.append({
                                "value": r["v"],
                                "inter_area_m2": inter_area,
                                "pct_of_parcel": inter_area / parcel_area_m2 * 100
                            })
                    coverage_results[cov_col] = cov_list
                    logger.info("   COVERAGE[%s]: %d classes (%.1f ms)", cov_col, len(cov_list), _ms(t_cov))
            else:
                logger.info("   COVERAGE: —")

            # assemble résultat
            results_report.append({
                "nom": table,
                "schema": schema,
                "table": table,
                "geom_col": "geom_2154",
                "srid": 2154,
                "gkind": "geometry",
                "count": n,
                "values": vals_map,
                "surfaces": surfaces,
                "coverage": coverage_results,
                "parcel_area_m2": parcel_area_m2
            })

            logger.info("   ✅ OK (%s) en %.1f ms", layer_tag, _ms(t_layer))

            hit_count += 1

        except Exception as e:
            logger.exception("   ❌ ERREUR couche %s: %s", layer_tag, e)
            continue

    # paquet final pour CETTE parcelle
    props = parcel_feature.get("properties") or {}
    section = props.get("section", "??")
    numero  = props.get("numero", "????")
    label = f"{section} {numero}"

    report_one = {
        "parcel": {
            "label": label,
            "srid": 4326,
            **parcel_extra
        },
        "layers_with_hits": hit_count,
        "results": results_report
    }
    return report_one

# ======================= Runner =======================
def run(args):
    # 1) INSEE
    insee = get_insee_from_csv(args.csv, args.commune.strip(), args.departement.strip())
    if not insee:
        raise RuntimeError("Impossible de déterminer un code INSEE unique.")
    logger.info(
        "INSEE résolu via CSV: commune='%s', departement='%s', insee='%s'",
        args.commune.strip(), args.departement.strip(), insee
    )

    # 2) Connexion / mapping
    eng = get_engine()
    layers_all = load_layer_map(args.mapping)
    if args.schema_whitelist:
        layers_all = [l for l in layers_all if l["schema"] in args.schema_whitelist]

    # 3) Liste de parcelles (multi)
    refs = _parse_parcel_refs(args.parcel)
    if not refs:
        raise RuntimeError("Aucune référence parcellaire valide fournie.")

    all_reports = []
    for (sec, num4) in refs:
        feat = locate_parcel_feature(insee, sec, num4)
        if not feat:
            all_reports.append({
                "parcel": {"label": f"{sec} {num4}", "srid": 4326},
                "error": f"Parcelle non trouvée (INSEE {insee}, {sec} {num4})"
            })
            continue

        r = _intersect_one_parcel(
            eng=eng, layers=layers_all, parcel_feature=feat,
            carve_enclaves=bool(args.carve_enclaves),
            enclave_buffer_m=float(args.enclave_buffer_m),
            values_limit=int(args.values_limit)
        )
        all_reports.append(r)

    out = {
        "commune": args.commune.strip(),
        "departement": str(args.departement).strip(),
        "insee": insee,
        "reports": all_reports
    }

    if args.json_output:
        with open(args.json_output, "w", encoding="utf-8") as jf:
            json.dump(out, jf, ensure_ascii=False)
        print(f"✅ Rapport JSON écrit : {args.json_output}")
    else:
        print(json.dumps(out, ensure_ascii=False))


def run_intersections(
    commune: str,
    departement: str,
    parcels: str,
    csv: str,
    mapping: str,
    out_json: str,
    schema_whitelist: list[str] = ["public"],
    values_limit: int = 100,
    carve_enclaves: bool = True,
    enclave_buffer_m: float = 120.0,
) -> dict:
    """Wrapper Python pour éviter argparse quand on importe en module."""
    class Args:
        pass
    args = Args()
    args.commune = commune
    args.departement = departement
    args.parcel = parcels
    args.csv = csv
    args.mapping = mapping
    args.schema_whitelist = schema_whitelist
    args.values_limit = values_limit
    args.json_output = out_json
    args.carve_enclaves = carve_enclaves
    args.enclave_buffer_m = enclave_buffer_m

    run(args)  # on appelle la vraie fonction run(args)
    return json.load(open(out_json, encoding="utf-8"))

# ======================= CLI =======================
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Intersections PARCELLE → Supabase/PostGIS (whitelist)")
    ap.add_argument("--commune", required=True, help="Nom exact de la commune (ex: 'Latresne')")
    ap.add_argument("--departement", required=True, help="Code département (ex: '33', '2A', '2B')")
    ap.add_argument("--parcel", required=True, help="Parcelle(s) 'SECTION NUM' ou liste séparée par virgules (ex: 'AD 0598' ou 'AD 0598, AC 0042')")
    ap.add_argument("--csv", required=True, help="Chemin vers le CSV INSEE (ex: v_commune_2025.csv)")
    ap.add_argument("--mapping", required=True, help="Chemin du mapping JSON (catalogue + whitelist d'attributs)")
    ap.add_argument("--schema-whitelist", nargs="*", default=["public"], help="Limiter la recherche à certains schémas")
    ap.add_argument("--values-limit", type=int, default=100, help="Nb max de valeurs DISTINCT par attribut (échantillon)")
    ap.add_argument("--json-output", default="rapport_parcelle.json", help="Fichier JSON de sortie")
    ap.add_argument("--carve-enclaves", dest="carve_enclaves", action="store_true", default=True,
                    help="Détecter et retrancher les enclaves de la parcelle avant intersection (défaut: ON)")
    ap.add_argument("--no-carve-enclaves", dest="carve_enclaves", action="store_false",
                    help="Désactiver la détection/soustraction d'enclaves")
    ap.add_argument("--enclave-buffer-m", type=float, default=120.0,
                    help="Rayon (m) pour récupérer les parcelles voisines et détecter les enclaves (def: 120)")
    args = ap.parse_args()
    run(args)
