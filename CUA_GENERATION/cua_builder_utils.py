#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
cua_builder_utils.py — Utilitaires 'légers' pour la génération du CUA (DOCX)

Contient :
- I/O et formatage de base (JSON, dates, adresses, etc.)
- Helpers mapping-aware (résolution schema.table → name/type/coverage_by)
- Normalisation des couvertures (%), formatage FR
- Sélecteurs de couches par type
- Extracteurs : PLU (zones), SUP, PPR (zonage / isocotes), RGA, Sismique,
  Environnement, Autres informations

Ces fonctions n'écrivent pas dans le DOCX directement ; elles renvoient des
chaînes structurées ou des objets prêts à être consommés par le builder.
"""

import os, json, datetime
from typing import Any, Dict, List, Optional, Tuple, Iterable
from collections import defaultdict

# ------------------------------------------------------------
# Chargement du mapping des couches (name/type/coverage_by/keep/geom)
# ------------------------------------------------------------

_LAYER_MAPPING: Dict[str, Dict[str, Any]] = {}

def _put_mapping_key(d: Dict[str, Dict[str, Any]], key: str, val: Dict[str, Any]):
    """Insère la clé normalisée si non présente (évite d'écraser une clé plus précise)."""
    k = (key or "").strip()
    if k and k not in d:
        d[k] = val

try:
    mapping_path = os.path.join(os.path.dirname(__file__), "..", "CONFIG", "mapping_layers.json")
    with open(mapping_path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    # 1) clés exactes (telles que dans le JSON)
    for k, v in raw.items():
        _put_mapping_key(_LAYER_MAPPING, k.strip(), v)

    # 2) variantes utiles : "public.table" et "table" seules
    #    (sans écraser une clé déjà présente)
    extras = {}
    for k, v in list(_LAYER_MAPPING.items()):
        if "." in k:
            schema, table = k.split(".", 1)
            _put_mapping_key(extras, table, v)                 # "table"
            _put_mapping_key(extras, f"public.{table}", v)     # "public.table"
        else:
            table = k
            _put_mapping_key(extras, f"public.{table}", v)     # "public.table"
    _LAYER_MAPPING.update(extras)

except Exception:
    _LAYER_MAPPING = {}


# ------------------------------------------------------------
# I/O + formatage de base
# ------------------------------------------------------------

def read_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def date_fr(iso: Optional[str]) -> str:
    if not iso: return ""
    try:
        d = datetime.date.fromisoformat(iso)
        return d.strftime("%d/%m/%Y")
    except Exception:
        return iso

def safe(x, default=""):
    return default if x in (None, "", [], {}) else x

def join_addr(ad: Dict[str, Any]) -> str:
    if not ad: return ""
    parts = []
    if ad.get("numero"): parts.append(str(ad["numero"]).strip())
    if ad.get("voie"): parts.append(str(ad["voie"]).strip())
    if ad.get("lieu_dit"): parts.append(str(ad["lieu_dit"]).strip())
    line1 = " ".join(parts).strip()
    line2 = " ".join([safe(ad.get("code_postal")), safe(ad.get("ville"))]).strip()
    return (line1 + (", " + line2 if line2 else "")).strip()

def parcels_label_from_cerfa(cerfa: Dict[str, Any]) -> str:
    refs = (cerfa.get("data") or {}).get("references_cadastrales") or []
    pairs = [f'{(r.get("section") or "").upper()} {str(r.get("numero") or "").zfill(4)}' for r in refs]
    return ", ".join([p for p in pairs if p.strip()])

def terrain_addr_from_cerfa(cerfa: Dict[str, Any]) -> str:
    return join_addr(((cerfa.get("data") or {}).get("adresse_terrain") or {}))

def demandeur_block(cerfa: Dict[str, Any]) -> Tuple[str, str]:
    d = (cerfa.get("data") or {}).get("demandeur") or {}
    who = (d.get("denomination") or " ".join([safe(d.get("prenom")), safe(d.get("nom"))]).strip()).strip()
    siret = safe(d.get("siret"))
    who_fmt = (who.upper() + (f" (SIRET {siret})" if siret else ""))
    domicile = join_addr(((cerfa.get("data") or {}).get("coord_demandeur") or {}).get("adresse") or {})
    return who_fmt, domicile

def format_footer_numero(cerfa: Dict[str, Any]) -> str:
    data = cerfa.get("data") or {}
    hdr = data.get("header_cu") or {}
    if all(hdr.get(k) for k in ("dept","commune_code","annee","numero_dossier")):
        dep = str(hdr["dept"]); dep3 = dep.zfill(3) if dep.isdigit() else dep
        com3 = str(hdr["commune_code"]).zfill(3)
        an2  = str(hdr["annee"])[-2:].zfill(2)
        nd   = str(hdr["numero_dossier"]).upper()
        return f"CU {dep3} {com3} {an2} {nd}"
    num = (data.get("numero_cu") or "").strip()
    try:
        dep, insee, yyyy, nd = num.split("-")
        dep3 = dep.zfill(3) if dep.isdigit() else dep
        com3 = insee[-3:]; an2 = yyyy[-2:]
        return f"CU {dep3} {com3} {an2} {nd}"
    except Exception:
        return f"CU {num}".strip()

def parcel_num_only(label: str) -> str:
    # "AC 0494" -> "494"
    if not label:
        return "—"
    bits = label.split()
    if not bits:
        return label
    num = bits[-1]
    return num.lstrip("0") or num


# ------------------------------------------------------------
# Helpers mapping-aware
# ------------------------------------------------------------

def layer_map_key(layer: Dict[str, Any]) -> str:
    schema = (layer.get("schema") or "").strip()
    table  = (layer.get("table")  or layer.get("nom") or "").strip()
    if schema and table:
        return f"{schema}.{table}"
    return table

def mapping_for(layer: Dict[str, Any]) -> Dict[str, Any]:
    """Résout dans cet ordre : 'schema.table' → 'public.table' → 'table'."""
    table = (layer.get("table") or layer.get("nom") or "").strip()
    schema = (layer.get("schema") or "").strip()

    candidates = []
    if schema and table:
        candidates.append(f"{schema}.{table}")
    if table:
        candidates.append(f"public.{table}")
        candidates.append(table)

    for key in candidates:
        if key in _LAYER_MAPPING:
            return _LAYER_MAPPING[key]
    return {}

def display_name(layer: Dict[str, Any]) -> str:
    m = mapping_for(layer)
    if m.get("name"):
        return m["name"]
    return (layer.get("name") or layer.get("table") or layer.get("nom") or "Couche")

def get_layer_type(layer: Dict[str, Any]) -> Optional[str]:
    m = mapping_for(layer)
    return m.get("type")

def pct_fr(p: float) -> str:
    try:
        return f"{float(p):.1f}".replace(".", ",")
    except Exception:
        return str(p)

def normalize_pairs(pairs: List[Tuple[str, float]]) -> List[Tuple[str, float]]:
    """
    Déduplique par valeur, cap par valeur à 100, puis limite la somme à 100 si nécessaire
    (évite les ~200% dus aux doublons assiette/générateur).
    """
    agg = defaultdict(float)
    for v, pct in pairs:
        if v is None:
            v = ""
        agg[str(v)] += float(pct or 0.0)
    norm = [(v, min(p, 100.0)) for v, p in agg.items()]
    norm.sort(key=lambda x: x[1], reverse=True)
    total = sum(p for _, p in norm)
    if total > 100.0001:
        norm = [(v, p * 100.0 / total) for v, p in norm]
    return norm


# ------------------------------------------------------------
# Accès aux layers et coverage
# ------------------------------------------------------------

def iter_layers(inters: Dict[str, Any]):
    for rep in (inters.get("reports") or []):
        for layer in (rep.get("results") or []):
            yield layer

def values_of(layer: Dict[str, Any], key: str) -> List[str]:
    vs = (layer.get("values") or {}).get(key) or []
    return [str(v) for v in vs if v is not None]

def coverage_pairs(layer: Dict[str, Any]) -> List[Tuple[str, float]]:
    """
    Retourne [(valeur, %)] en respectant 'coverage_by' du mapping si dispo.
    Fallback: 1er champ coverage non vide. Résultat normalisé (≤100%).
    """
    cov = layer.get("coverage") or {}
    if not cov:
        return []
    m = mapping_for(layer)
    fields = m.get("coverage_by") or list(cov.keys())
    # Try declared fields first (in order)
    for k in fields:
        arr = cov.get(k) or []
        pairs = []
        for it in arr:
            v = str(it.get("value") or "")
            pct = float(it.get("pct_capped") or it.get("pct_of_parcel") or 0.0)
            pairs.append((v, pct))
        if pairs:
            return normalize_pairs(pairs)
    # Fallback: first non-empty coverage field
    for k, arr in cov.items():
        pairs = []
        for it in arr or []:
            v = str(it.get("value") or "")
            pct = float(it.get("pct_capped") or it.get("pct_of_parcel") or 0.0)
            pairs.append((v, pct))
        if pairs:
            return normalize_pairs(pairs)
    return []

def first_non_empty_values(layer: Dict[str, Any], candidates: List[str]) -> List[str]:
    """Renvoie la première liste non vide parmi candidates dans layer['values']."""
    vals = layer.get("values") or {}
    for key in candidates:
        arr = vals.get(key)
        if arr:
            return [str(x) for x in arr if x is not None]
    return []

def get_layers_by_type(inters: Dict[str, Any], layer_types: List[str]) -> List[Dict[str, Any]]:
    """Récupère toutes les couches d'un/des types donnés (via mapping)."""
    matching_layers = []
    for layer in iter_layers(inters):
        layer_type = get_layer_type(layer)
        if layer_type in layer_types:
            matching_layers.append(layer)
    return matching_layers


# ------------------------------------------------------------
# Extracteurs orientés CUA
# ------------------------------------------------------------

# (Optionnels) — PLU annex helpers (résolus au runtime par le builder)
try:
    from fetch_plu_regulation import canonicalize_zone  # type: ignore
    _HAS_PLU_CANON = True
except Exception:
    canonicalize_zone = None
    _HAS_PLU_CANON = False


def extract_zones_and_pct(inters: Dict[str, Any]) -> Tuple[List[str], Dict[str, float]]:
    """Extrait les zones PLU selon le mapping 'plu_zonage' et coverage_by."""
    zones: List[str] = []
    pct_map: Dict[str, float] = {}

    plu_layers = get_layers_by_type(inters, ["plu_zonage"])
    for l in plu_layers:
        pairs = coverage_pairs(l)
        if pairs:
            for code, pct in pairs:
                if not code:
                    continue
                zones.append(code)
                key = canonicalize_zone(code) if (_HAS_PLU_CANON and canonicalize_zone) else code
                pct_map[key] = max(pct_map.get(key, 0.0), float(pct or 0.0))
            continue

        # Fallback valeurs brutes (ordre préférentiel)
        raw_codes = first_non_empty_values(l, ["libelong", "libelle", "typezone", "codezone"])
        for code in raw_codes:
            zones.append(code)
            key = canonicalize_zone(code) if (_HAS_PLU_CANON and canonicalize_zone) else code
            pct_map.setdefault(key, 0.0)

    seen, uniq = set(), []
    for z in zones:
        if z and z not in seen:
            uniq.append(z)
            seen.add(z)
    return uniq, pct_map


def extract_sup_list(inters: Dict[str, Any]) -> List[Tuple[str, str]]:
    """
    Extrait les servitudes depuis types 'servitudes' et 'servitudes_aeronautiques'.
    Agrège sur (suptype, libellé 'joli' si dispo), déduplique et ordonne.
    """
    items = set()
    sup_layers = get_layers_by_type(inters, ["servitudes", "servitudes_aeronautiques"])

    for l in sup_layers:
        suptypes = [s.upper().strip() for s in values_of(l, "suptype")] or [""]
        labels = (values_of(l, "nomsuplitt") or values_of(l, "libelle") or values_of(l, "nom"))
        if not labels:
            labels = [display_name(l)]
        for st in suptypes:
            for nm in labels:
                items.add((st or "—", (nm or "—").strip()))

    return sorted(items, key=lambda x: (x[0], x[1]))


def build_ppr_detail(inters: Dict[str, Any]) -> str:
    """
    Construit un bloc synthèse + détail:
    - Zonage réglementaire (type ppr_inondation via n_zone_reg_ppri_033 / codezone)
    - Isocotes (type ppr_inondation via l_cote_seuil_ppri_s_033 / codezone)
    Détail par parcelle sous forme: "Parcelle 494 : Bleu (94,5 %), Rouge urbanisé (5,5 %)" etc.
    """
    zonage_by_parcel = defaultdict(list)   # parcel_num -> [(value, pct)]
    isocotes_by_parcel = defaultdict(list) # parcel_num -> [(value, pct)]

    for rep in (inters.get("reports") or []):
        parcel = rep.get("parcel") or {}
        parcel_label = parcel.get("label") or "—"
        parcel_num = parcel_num_only(parcel_label)

        for layer in (rep.get("results") or []):
            ltype = get_layer_type(layer)
            if ltype != "ppr_inondation":
                continue
            namekey = layer_map_key(layer)
            pairs = coverage_pairs(layer)
            if not pairs:
                continue

            # Dispatch par couche (zones réglementaires vs isocotes)
            if namekey.endswith("n_zone_reg_ppri_033") or layer.get("table") == "n_zone_reg_ppri_033":
                zonage_by_parcel[parcel_num].extend(pairs)
            elif namekey.endswith("l_cote_seuil_ppri_s_033") or layer.get("table") == "l_cote_seuil_ppri_s_033":
                isocotes_by_parcel[parcel_num].extend(pairs)
            else:
                disp = display_name(layer).lower()
                if "cote" in disp or "isocote" in disp:
                    isocotes_by_parcel[parcel_num].extend(pairs)
                else:
                    zonage_by_parcel[parcel_num].extend(pairs)

    def _fmt_line(dct) -> str:
        if not dct:
            return "Aucune information PPR issue des données fournies."
        parts = []
        for pnum in sorted(dct.keys(), key=lambda x: (len(x), x)):
            pairs = normalize_pairs(dct[pnum])  # sécurité
            if pairs:
                txt = ", ".join([f"{v} ({pct_fr(p)} %)" for v, p in pairs])
                parts.append(f"Parcelle {pnum} : {txt}")
        return " ; ".join(parts) if parts else "—"

    synth = []
    if zonage_by_parcel:
        synth.append(f"Zonage réglementaire : {_fmt_line(zonage_by_parcel)}")
    if isocotes_by_parcel:
        synth.append(f"Isocotes : {_fmt_line(isocotes_by_parcel)}")
    if not synth:
        return "Aucune information PPR issue des données fournies."

    # Détail multilignes (identique à la synthèse mais multi-lignes + 'pour xx%')
    detail_lines = []
    if zonage_by_parcel:
        detail_lines.append("PPRI (Plan de Prévention des Risques d’Inondation) - Zonage règlementaire :")
        for pnum in sorted(zonage_by_parcel.keys(), key=lambda x: (len(x), x)):
            pairs = normalize_pairs(zonage_by_parcel[pnum])
            if pairs:
                txt = " – ".join([f"{v} pour {pct_fr(p)}%" for v, p in pairs])
                detail_lines.append(f"Parcelle {pnum} : {txt}")
    if isocotes_by_parcel:
        detail_lines.append("Isocotes :")
        for pnum in sorted(isocotes_by_parcel.keys(), key=lambda x: (len(x), x)):
            pairs = normalize_pairs(isocotes_by_parcel[pnum])
            if pairs:
                txt = " – ".join([f"{v} pour {pct_fr(p)}%" for v, p in pairs])
                detail_lines.append(f"Parcelle {pnum} : {txt}")

    return " ; ".join(synth) + ("\n" + "\n".join(detail_lines) if detail_lines else "")


def build_rga_detail(inters: Dict[str, Any]) -> str:
    """
    Si un type 'rga' est ajouté dans le mapping, on l'utilise.
    Sinon fallback par mots-clés ('argile', 'rga', 'retrait', 'gonflement').
    """
    rga_layers = get_layers_by_type(inters, ["rga", "ppr_mouvement_terrain"])
    for l in rga_layers:
        pairs = coverage_pairs(l)
        if pairs:
            return ", ".join([f"{v} ({pct_fr(p)} %)" for v, p in pairs])
        vals = l.get("values") or {}
        if vals:
            items = []
            for k, vs in vals.items():
                if vs:
                    items.append(f"{k}: {', '.join([str(x) for x in vs[:6]])}")
            if items:
                return "; ".join(items)

    # Fallback mots-clés
    for l in iter_layers(inters):
        nm = (l.get("table") or l.get("nom") or "").lower()
        if any(k in nm for k in ["argile", "rga", "retrait", "gonflement"]):
            pairs = coverage_pairs(l)
            if pairs:
                return ", ".join([f"{v} ({pct_fr(p)} %)" for v, p in pairs])
            vals = l.get("values") or {}
            if vals:
                items=[]
                for k, vs in vals.items():
                    if vs: items.append(f"{k}: {', '.join([str(x) for x in vs[:6]])}")
                if items: return "; ".join(items)
    return "Non renseigné dans les données."


def build_sismique_detail(inters: Dict[str, Any]) -> str:
    """
    Si un type 'sismique' est ajouté dans le mapping, on l'utilisera automatiquement.
    Fallback actuel par mots-clés ('sismique', 'sismicite').
    """
    sism_layers = get_layers_by_type(inters, ["sismique"])
    for l in sism_layers:
        pairs = coverage_pairs(l)
        if pairs:
            return ", ".join([f"{v} ({pct_fr(p)} %)" for v, p in pairs])
        vals = l.get("values") or {}
        if vals:
            items=[]
            for k, vs in vals.items():
                if vs: items.append(f"{k}: {', '.join([str(x) for x in vs[:6]])}")
            if items: return "; ".join(items)

    # Fallback mots-clés
    for l in iter_layers(inters):
        nm = (l.get("table") or l.get("nom") or "").lower()
        if any(k in nm for k in ["sismique", "sismicite"]):
            pairs = coverage_pairs(l)
            if pairs:
                return ", ".join([f"{v} ({pct_fr(p)} %)" for v, p in pairs])
            vals = l.get("values") or {}
            if vals:
                items=[]
                for k, vs in vals.items():
                    if vs: items.append(f"{k}: {', '.join([str(x) for x in vs[:6]])}")
                if items: return "; ".join(items)
    return "Non renseigné dans les données."


def build_env_detail(inters: Dict[str, Any]) -> str:
    """
    Construit les expositions environnementales à partir des types définis dans le mapping.
    """
    env_bits = []
    env_types = [
        "patrimoine_naturel", "nuisances_sonores", "radon",
        "installations_classees", "captage_eau", "sites_classes",
        "protection_marais", "transport_matieres_dangereuses"
    ]

    env_layers = get_layers_by_type(inters, env_types)

    for l in env_layers:
        pairs = coverage_pairs(l)
        title = display_name(l)
        if pairs:
            env_bits.append(f"{title}: " + ", ".join([f"{v} ({pct_fr(p)} %)" for v, p in pairs]))
        else:
            vals = l.get("values") or {}
            vals_txt = []
            for k, vs in vals.items():
                if vs:
                    vals_txt.append(f"{k}={', '.join([str(x) for x in vs[:6]])}")
            env_bits.append(f"{title}: " + ("; ".join(vals_txt) if vals_txt else "—"))

    if not env_bits:
        return "Aucune exposition particulière détectée dans les données fournies."
    return "\n".join([f"- {x}" for x in env_bits])


def build_other_infos(inters: Dict[str, Any]) -> str:
    """
    Liste les couches restantes non déjà détaillées, via mapping.type uniquement.
    Exclut explicitement les types déjà traités par ailleurs.
    """
    exclude_types = set([
        "plu_zonage",
        "servitudes", "servitudes_aeronautiques",
        "preemption",
        "ppr_inondation", "ppr_mouvement_terrain", "ppr_feux_forets",
        "patrimoine_naturel", "nuisances_sonores", "radon",
        "installations_classees", "captage_eau", "sites_classes",
        "protection_marais", "transport_matieres_dangereuses",
        "habillage_cartographique"
    ])

    bullets = []
    for l in iter_layers(inters):
        ltype = get_layer_type(l)
        if ltype in exclude_types:
            continue

        pairs = coverage_pairs(l)
        title = display_name(l)
        if pairs:
            bullets.append(f"- {title}: " + ", ".join([f"{v} ({pct_fr(p)} %)" for v, p in pairs]))
            continue

        vals = l.get("values") or {}
        pieces = []
        for k, v in vals.items():
            if v:
                pieces.append(f"{k} = {', '.join([str(x) for x in v][:5])}")
        if pieces:
            bullets.append(f"- {title}: " + " ; ".join(pieces))

    return "\n".join(bullets) if bullets else "—"


# -------- Structs pour Article 5 : PPR + Environnement (par parcelle) --------

def build_ppr_struct(inters: Dict[str, Any]) -> Dict[str, Any]:
    """
    Renvoie un dict structuré pour un rendu 'propre' :
    {
      "zonage": { "494": [(value, pct), ...], "496": [...], ... },
      "isocotes": { "494": [(value, pct), ...], ... },
      "sources": set(["PM1_PPRI_...pdf", ...])
    }
    """
    zonage_by_parcel = defaultdict(list)
    isocotes_by_parcel = defaultdict(list)
    sources: set = set()

    for rep in (inters.get("reports") or []):
        parcel = rep.get("parcel") or {}
        parcel_label = parcel.get("label") or "—"
        pnum = parcel_num_only(parcel_label)

        for layer in (rep.get("results") or []):
            ltype = get_layer_type(layer)
            if ltype == "servitudes":
                # Récupérer 'fichier' pour citer la source PPRI (si présente)
                for f in (layer.get("values") or {}).get("fichier", []) or []:
                    if f: sources.add(str(f))
            if ltype != "ppr_inondation":
                continue

            namekey = layer_map_key(layer)
            pairs = coverage_pairs(layer)
            if not pairs:
                continue

            if namekey.endswith("n_zone_reg_ppri_033") or layer.get("table") == "n_zone_reg_ppri_033":
                zonage_by_parcel[pnum].extend(pairs)
            elif namekey.endswith("l_cote_seuil_ppri_s_033") or layer.get("table") == "l_cote_seuil_ppri_s_033":
                isocotes_by_parcel[pnum].extend(pairs)
            else:
                disp = display_name(layer).lower()
                if "cote" in disp or "isocote" in disp:
                    isocotes_by_parcel[pnum].extend(pairs)
                else:
                    zonage_by_parcel[pnum].extend(pairs)

    # normalisation de sécurité
    for p in list(zonage_by_parcel.keys()):
        zonage_by_parcel[p] = normalize_pairs(zonage_by_parcel[p])
    for p in list(isocotes_by_parcel.keys()):
        isocotes_by_parcel[p] = normalize_pairs(isocotes_by_parcel[p])

    return {"zonage": dict(zonage_by_parcel), "isocotes": dict(isocotes_by_parcel), "sources": sources}


def build_env_struct(inters: Dict[str, Any]) -> Dict[str, Any]:
    """
    Regroupe l'environnement par thème et par parcelle :
    {
      "radon": { "494": [("1", 100.0)], ... , "_label": "IRSN - Radon métropole" },
      "nuisances": { "494": [("3", 17.0, "D113:1")], ... , "_label": "Nuisances sonores Gironde" }
    }
    """
    radon = defaultdict(list)
    nuis = defaultdict(list)
    radon_label = None
    nuis_label = None

    for rep in (inters.get("reports") or []):
        parcel = rep.get("parcel") or {}
        parcel_label = parcel.get("label") or "—"
        pnum = parcel_num_only(parcel_label)

        for layer in (rep.get("results") or []):
            ltype = get_layer_type(layer)
            if ltype == "radon":
                if radon_label is None:
                    radon_label = display_name(layer)
                for v, p in coverage_pairs(layer) or []:
                    radon[pnum].append((v, p))
            elif ltype == "nuisances_sonores":
                if nuis_label is None:
                    nuis_label = display_name(layer)
                trunk = None
                # essayer de récupérer un identifiant d'axe (route / tronc)
                vals = layer.get("values") or {}
                cand = (vals.get("nom_tronc") or vals.get("toponyme") or [])
                if cand:
                    trunk = str(cand[0])
                for v, p in coverage_pairs(layer) or []:
                    nuis[pnum].append((v, p, trunk))

    # normaliser
    for p in list(radon.keys()):
        radon[p] = normalize_pairs(radon[p])
    # pour nuisances, normaliser sur (valeur, pct) → on conserve trunk à part
    norm_nuis = defaultdict(list)
    for p, items in nuis.items():
        agg = defaultdict(float)
        trunk_seen = {}
        for (v, pct, tr) in items:
            agg[v] += float(pct or 0.0)
            if v not in trunk_seen and tr:
                trunk_seen[v] = tr
        pairs = normalize_pairs(list(agg.items()))
        # réinjecter trunk le plus représentatif si connu
        for (v, pct) in pairs:
            norm_nuis[p].append((v, pct, trunk_seen.get(v)))
    nuis = norm_nuis

    out = {}
    if radon:
        out["radon"] = dict(radon)
        out["radon"]["_label"] = radon_label or "Radon"
    if nuis:
        out["nuisances"] = dict(nuis)
        out["nuisances"]["_label"] = nuis_label or "Nuisances sonores"
    return out


def group_parcels_by_value_pct(parcels_map, *, with_trunk: bool = False):
    """
    Regroupe les parcelles ayant la même (valeur, pct[, trunk]).
    - parcels_map: dict "pnum" -> [(value, pct)] ou [(value, pct, trunk)]
    - with_trunk: si True (nuisances), on groupe aussi par 'trunk'
    Retourne: list de tuples (value, pct, [trunk or None], [liste_parcelles])
              triés par valeur puis pct desc.
    """
    from collections import defaultdict
    bucket = defaultdict(list)
    for pnum, items in (parcels_map or {}).items():
        for it in items:
            if with_trunk:
                v, pct, trunk = it
                key = (str(v), round(float(pct or 0.0), 1), trunk or None)
            else:
                v, pct = it
                key = (str(v), round(float(pct or 0.0), 1), None)
            bucket[key].append(pnum)

    rows = []
    for (v, pct, trunk), plist in bucket.items():
        plist_sorted = sorted(plist, key=lambda x: (len(x), x))
        rows.append((v, pct, trunk, plist_sorted))
    # tri: valeur, pct décroissant
    rows.sort(key=lambda x: (x[0], -x[1]))
    return rows
