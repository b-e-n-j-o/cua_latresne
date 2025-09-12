# report_from_json.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import json, argparse, logging, re, os
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

from llm_utils import call_gpt5_text  # wrapper existant

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("report_from_json_text")

# ===================== Config par défaut (overridable CLI) ===================== #
DEFAULT_PLU_REG_TABLE = os.getenv("PLU_REG_TABLE", "plu_chunks")
DEFAULT_PLU_REG_COLUMN = os.getenv("PLU_REG_COLUMN", "zonage")


# ------------------------- Helpers numériques ------------------------- #
def _round1(x: Optional[float]) -> Optional[float]:
    try:
        return float(round(float(x or 0), 1))
    except Exception:
        return None

def _fmt_m2(x: Optional[float]) -> str:
    v = _round1(x)
    return f"{v} m²" if v is not None else "—"

def _fmt_pct(x: Optional[float]) -> str:
    v = _round1(x)
    return f"{v} %" if v is not None else "—"


# ---------------------- Pré-compaction + agrégats --------------------- #
def _add_surface_totals(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ajoute pour chaque layer :
      - surface_totals: { inter_total_m2, parcel_pct_total }
    Ajoute aussi context.parcel_area_est_m2 si récupérable.
    """
    d = json.loads(json.dumps(payload))  # deep copy
    parcel_area_est = None

    for rep in d.get("reports", []):
        for layer in rep.get("results", []):
            surfs = layer.get("surfaces") or []
            inter_total = 0.0
            for s in surfs:
                a = s.get("inter_area_m2")
                if isinstance(a, (int, float)):
                    inter_total += float(a)
            parcel_area = layer.get("parcel_area_m2")
            if parcel_area and not parcel_area_est:
                parcel_area_est = float(parcel_area)
            pct_total = (inter_total / float(parcel_area)) * 100 if parcel_area else None
            layer["surface_totals"] = {
                "inter_total_m2": inter_total if inter_total > 0 else None,
                "parcel_pct_total": pct_total if pct_total is not None else None,
            }

    if isinstance(parcel_area_est, (int, float)):
        d.setdefault("context", {}).setdefault("parcel_area_est_m2", parcel_area_est)
    return d


def _compact_payload(data: Dict[str, Any], max_values_per_attr: int = 8, max_surfaces: int = 60) -> Dict[str, Any]:
    """
    - Tronque les valeurs DISTINCT trop longues.
    - Tronque la liste de surfaces (top N par surface).
    - Calcule et ajoute des totaux de surface.
    """
    d = _add_surface_totals(data)
    for rep in d.get("reports", []):
        for layer in rep.get("results", []):
            # valeurs distinctes
            vals = layer.get("values", {})
            for k, arr in list(vals.items()):
                if isinstance(arr, list) and len(arr) > max_values_per_attr:
                    vals[k] = arr[:max_values_per_attr] + ["…"]

            # surfaces (top N)
            surfs = layer.get("surfaces")
            if isinstance(surfs, list) and len(surfs) > max_surfaces:
                surfs_sorted = sorted(
                    [s for s in surfs if isinstance(s, dict) and isinstance(s.get("inter_area_m2"), (int, float))],
                    key=lambda s: s.get("inter_area_m2", 0.0),
                    reverse=True
                )
                layer["surfaces"] = surfs_sorted[:max_surfaces] + [{"id": "…", "inter_area_m2": None, "pct_of_parcel": None}]
    return d


# ------------------------ Détection des zones PLU ---------------------- #
def _is_like(name: str, *needles: str) -> bool:
    n = (name or "").lower()
    return any(k in n for k in needles)

def _find_plu_layers(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Repère les couches de zonage PLU, tolérant des variantes de nommage.
    """
    out = []
    for lyr in results:
        t = (lyr.get("table") or "")
        if _is_like(t, "zonage_plu", "zone_urba", "wfs_du:zone_urba", "b_zonage_plu"):
            out.append(lyr)
    return out

def _first_val(d: Dict[str, Any], keys: List[str]) -> Optional[str]:
    vals = d.get("values", {})
    for k in keys:
        arr = vals.get(k) or vals.get(k.upper())
        if isinstance(arr, list) and arr:
            v = arr[0]
            if v is not None:
                return str(v).strip()
    return None

def _pct_from_layer(lyr: Dict[str, Any]) -> Optional[float]:
    st = lyr.get("surface_totals") or {}
    pct = st.get("parcel_pct_total")
    return float(pct) if isinstance(pct, (int, float)) else None

def _extract_plu_zones_from_payload(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Retourne une liste de zones issues des couches PLU trouvées.
    Chaque zone: {"libelle": str|None, "typezone": str|None, "pct": float|None}
    """
    rep = (payload.get("reports") or [{}])[0]
    results = rep.get("results") or []
    layers = _find_plu_layers(results)
    zones: List[Dict[str, Any]] = []

    for lyr in layers:
        z = {
            "libelle": _first_val(lyr, ["libelle", "LIBELLE"]),
            "typezone": _first_val(lyr, ["typezone", "TYPEZONE", "zone", "ZONE"]),
            "pct": _pct_from_layer(lyr),
        }
        # ne duplique pas si déjà présent
        if any((z.get("libelle") and z["libelle"] == e.get("libelle")) or
               (z.get("typezone") and z["typezone"] == e.get("typezone")) for e in zones):
            continue
        zones.append(z)

    # Trie par pourcentage décroissant si disponible
    zones.sort(key=lambda x: (x["pct"] is not None, x.get("pct") or 0.0), reverse=True)
    return zones


# ------------------ Inférence de codes + fetch règlement ------------------ #
_CAND_RE = re.compile(r"\b(\d+AU|AU[A-Z]?|U[A-Z]?|N[A-Z]?|A[A-Z]?)\b", re.IGNORECASE)

def _cand_for_zone(z: str) -> List[str]:
    """
    Génère des codes candidats déterministes à partir d'un libellé ou d’un code.
    (logique alignée avec fetch_plu_regulation.py)
    """
    z = (z or "").strip().upper()
    if not z:
        return []

    # Si le libellé contient déjà un code plausible (ex: "Zone AUc", "Secteur UA")
    m = _CAND_RE.findall(z)
    if m:
        # privilégie le premier match explicite
        primary = m[0].upper()
        # Normalise variantes AUc -> AU ; ajoute 1AU comme candidate prioritaire
        if primary.startswith("AU"):
            return ["1AU", "AU", "2AU", "3AU"]
        return [primary]

    # Si on nous donne directement "AUc"/"AUb"…
    if z.startswith("AU") and len(z) >= 2:
        return ["1AU", "AU", "2AU", "3AU"]

    # 1AU / 2AU / 3AU…
    m2 = re.match(r"^(\d+)AU$", z)
    if m2:
        n = m2.group(1)
        if n == "1":
            return ["1AU", "AU"]
        elif n == "2":
            return ["2AU", "AU", "1AU"]
        else:
            return [f"{n}AU", "AU", "1AU"]

    # UA / UB / UC / N / A…
    return [z]

def _infer_zone_codes(libelle: Optional[str], typezone: Optional[str]) -> List[str]:
    """
    Produit une petite liste de codes candidats à partir de typezone prioritaire puis libellé.
    """
    cand: List[str] = []
    if typezone:
        cand.extend(_cand_for_zone(typezone))
    if libelle:
        # évite les doublons, conserve ordre
        for c in _cand_for_zone(libelle):
            if c not in cand:
                cand.append(c)
    return cand[:6]  # bornage sécurité

def _fetch_plu_regulation_for_zones(
    zones: List[Dict[str, Any]],
    table: str,
    column: str,
    max_chars_total: int = 12000,
) -> str:
    """
    Pour chaque zone, tente de récupérer un texte de règlement via fetch_plu_regulation().
    Concatène avec un en-tête par zone. Coupe globalement à max_chars_total.
    """
    if not zones:
        return ""

    try:
        from fetch_plu_regulation import fetch_plu_regulation as _fetch
    except Exception as e:
        log.warning(f"⚠️ Module fetch_plu_regulation introuvable/inimportable: {e}")
        return ""

    parts: List[str] = []
    used = 0

    for z in zones:
        codes = _infer_zone_codes(z.get("libelle"), z.get("typezone"))
        pct = z.get("pct")
        pretty_pct = f" ({_fmt_pct(pct)})" if isinstance(pct, (int, float)) else ""

        got_txt = ""
        for c in codes:
            txt = _fetch(c, table=table, column=column)
            if txt and not txt.startswith(("❌", "ℹ️")):
                got_txt = txt.strip()
                heading = f"=== {c}{pretty_pct} ==="
                block = f"{heading}\n{got_txt}".strip()
                # coupe si besoin
                if used + len(block) > max_chars_total:
                    block = block[: max(0, max_chars_total - used)]
                parts.append(block)
                used += len(block)
                break  # on s'arrête au premier code concluant

        if used >= max_chars_total:
            break

    final = "\n\n".join([p for p in parts if p])
    log.info(f"📚 Règlement PLU injecté: {len(final)} caractères")
    return final


# ----------------------------- Prompt LLM ----------------------------- #
def _build_prompt_text(compact_json: Dict[str, Any], plu_reg_text: str = "") -> str:
    """
    Objectif : produire un rapport PARAGRAPHIQUE (pas de Markdown),
    en français, avec consolidation PPRI / PLU / Nuisances / Radon, chiffres (m², %) et sans doublons.
    Si 'plu_reg_text' est fourni, l'assistant peut s'y référer STRICTEMENT pour
    enrichir la partie PLU (extraits neutres). Ne rien inventer si vide.
    """
    style = """Tu es un assistant spécialisé en urbanisme réglementaire.
Transforme STRICTEMENT le JSON fourni en un rapport textuel clair, professionnel et concis.

Contraintes de sortie :
- Texte brut uniquement : pas de Markdown, pas de listes à puces, pas de tableaux, pas de code.
- Paragraphes courts, séparés par une ligne vide.
- Ne mentionne que ce qui est présent dans le JSON (pas d'invention).
- Utilise les m² et % fournis ou déductibles via les totaux calculés (1 décimale).
- Évite les doublons : si plusieurs couches PPRI couvrent 100 % (assiette/générateur/prescription), fusionne en un seul constat.
- Évite de citer les noms techniques de tables ; privilégie des libellés métiers.

Si un TEXTE DE RÈGLEMENT PLU est fourni ci-dessous, tu peux l'utiliser pour rédiger des extraits neutres
dans la section Urbanisme (PLU) : cite uniquement ce qui figure dans ce texte, de façon factuelle.
S'il est absent, n'invente rien et reste descriptif.

Structure attendue :
1) En-tête : commune (INSEE), parcelle (section-numéro) et surface estimée si présente.
2) Résumé exécutif : 3 à 6 phrases qui donnent l'essentiel.
3) Développement par thématiques (si présentes) :
   - PPRI / inondation : consolider assiette/générateur/prescription ; détailler la répartition par zones (ex: Rouge urbanisé, Bleu) avec m² et % cumulés.
   - Urbanisme (PLU) : zonage de la parcelle (ex: zone N), avec couverture. Si du règlement est fourni, ajoute des extraits neutres strictement issus de ce texte.
   - Nuisances sonores routières : tronçon concerné, catégorie de bruit, couverture.
   - Radon : classe de potentiel.
   - Autres éléments significatifs s'il y en a.
4) Conclusion : 2 à 4 phrases sur le niveau de contrainte et les points d'attention.

Rappels :
- N'évoque jamais une thématique absente du JSON.
- Si la répartition par zones PPRI est disponible, affiche la couverture de chaque zone.
- Ne colle pas les identifiants internes (id, fid, ctid...)."""

    # Exemple exact repris de ta version précédente (inchangé)
    example = """Rapport d’analyse parcellaire – Latresne (INSEE 33234)

Parcelle concernée : AC 0496
Surface : 778,0 m²

Résumé exécutif

La parcelle est entièrement couverte par les servitudes du PPRI (Plan de Prévention des Risques d’Inondation).
Elle se situe dans une zone de risque d’inondation, avec x% en zone rouge urbanisée et x% en zone bleue.
Le zonage PLU classe la parcelle en zone N (naturelle).
Une prescription « secteur soumis à un risque inondation » s’applique.
La parcelle est également dans un secteur de nuisance sonore (catégorie 3) lié à la route D113.
Le potentiel radon est classé en niveau 1 (faible).

Analyse détaillée

Servitudes d’utilité publique – PPRI (Latresne)
La parcelle est entièrement concernée par le périmètre réglementé du PPRI (x% de la surface).
Document de référence : PM1_PPRI_Latresne_20220223_act.pdf.

Risques d’inondation (zonage PPRI)
Zone rouge urbanisée : x m² (x% de la parcelle).
Zone bleue : x m² (x% de la parcelle).
Fais toujours bien à associer les surfaces aux bonnesvaleurs distinctes disponibles.
Cotes de seuil présentes : x ; x.

Urbanisme (PLU)
Parcelle classée en zone N. Couverture : x% de la parcelle.

Nuisances sonores
Tronçon D113:1 ; catégorie 3. Impact : x m² (x% de la parcelle).

Radon
Classe de potentiel : 1 (faible).

Conclusion
La parcelle AC 0496 à Latresne est fortement contrainte par le PPRI et le zonage naturel.
Exposition partielle aux nuisances sonores.
Niveau de risque radon faible (pas de contrainte majeure)."""

    return (
        style
        + "\n\nEXEMPLE DE STYLE À IMITER (adapter aux données réelles) :\n\n"
        + example
        + "\n\nDONNÉES JSON (à transformer en rapport) :\n"
        + json.dumps(compact_json, ensure_ascii=False)
        + "\n\nTEXTE DE RÈGLEMENT PLU (extraits, par zone — si vide, ignorer) :\n"
        + (plu_reg_text.strip() or "(aucun)")
    )


# ------------------- Fallback déterministe (texte) -------------------- #
def _find_layer(results: List[Dict[str, Any]], table: str) -> Optional[Dict[str, Any]]:
    for l in results:
        if l.get("table") == table:
            return l
    return None

def _deterministic_text(payload: Dict[str, Any], plu_reg_text: str = "") -> str:
    """
    Texte lisible si l'appel LLM échoue.
    Ajoute en fin d'Urbanisme un bloc "Extraits du règlement (si dispo)".
    """
    ctx = payload.get("context", {})
    commune = ctx.get("commune") or "?"
    insee = ctx.get("insee") or "?"
    parcelles = ctx.get("parcelles") or []
    p_label = (f"{parcelles[0].get('section')} {parcelles[0].get('numero')}" if parcelles else "?")
    parcel_area = ctx.get("parcel_area_est_m2")

    out: List[str] = []

    head = f"Analyse parcellaire – {commune} (INSEE {insee}) – Parcelle {p_label}"
    if isinstance(parcel_area, (int, float)):
        head += f" – Surface { _fmt_m2(parcel_area) }"
    out.append(head)

    rep = (payload.get("reports") or [{}])[0]
    results = rep.get("results") or []

    # PPRI – zonage détaillé
    ppri_zone = _find_layer(results, "n_zone_reg_ppri_033")
    ppri_ass  = _find_layer(results, "b_assiette_de_servitude_d_utilite_publique")
    ppri_gen  = _find_layer(results, "b_generateur_de_servitude_d_utilite_publique")
    psc_surf  = _find_layer(results, "b_prescriptions_surfaciques")
    l_cotes   = _find_layer(results, "l_cote_seuil_ppri_s_033")

    # PLU
    plu = None
    # tolérant : on prend la 1ère couche détectée comme PLU
    for lyr in results:
        if _is_like(lyr.get("table") or "", "zonage_plu", "zone_urba"):
            plu = lyr
            break

    # Nuisances
    bruit = _find_layer(results, "nuisances_sonores_gironde")

    # Radon
    radon = _find_layer(results, "irsn_radon_metropole")

    # Résumé
    resume_parts = []
    if ppri_zone or ppri_ass or ppri_gen or psc_surf:
        resume_parts.append("La parcelle est concernée par le PPRI.")
    if plu and (plu.get("values", {}).get("libelle") or plu.get("values", {}).get("typezone")):
        z = (plu.get("values", {}).get("libelle") or plu.get("values", {}).get("typezone") or ["?"])[0]
        resume_parts.append(f"Zonage PLU : {z}.")
    if bruit:
        resume_parts.append("Présence d’un secteur de nuisance sonore routière.")
    if radon:
        v = (radon.get("values", {}).get("CLASSE_POT") or ["?"])[0]
        resume_parts.append(f"Potentiel radon de classe {v}.")
    if resume_parts:
        out.append("")
        out.append("Résumé exécutif : " + " ".join(resume_parts))

    # PPRI détaillé
    if any([ppri_zone, ppri_ass, ppri_gen, psc_surf, l_cotes]):
        out.append("")
        p = []
        cov_glob = None
        for lyr in [ppri_ass, ppri_gen, psc_surf, ppri_zone, l_cotes]:
            if lyr and isinstance(lyr.get("surface_totals"), dict):
                pct = lyr["surface_totals"].get("parcel_pct_total")
                if isinstance(pct, (int, float)) and pct >= 99.5:
                    cov_glob = "100.0 %"
                    break
        if cov_glob:
            p.append("La parcelle est entièrement comprise dans le périmètre réglementé du PPRI.")
        else:
            p.append("La parcelle est partiellement soumise au périmètre PPRI.")
        out.append(" ".join(p))

    # Urbanisme – PLU
    if plu:
        out.append("")
        v = plu.get("values", {})
        zone = (v.get("libelle") or v.get("typezone") or ["?"])[0]
        tot = plu.get("surface_totals", {}).get("parcel_pct_total")
        cov = f" ({_fmt_pct(tot)})" if isinstance(tot, (int, float)) else ""
        out.append(f"Zonage PLU : la parcelle est classée en zone {zone}{cov}.")
        # Extraits règlement si dispo
        if plu_reg_text.strip():
            # on ne met qu'un court rappel indicatif (le texte complet est dans le prompt LLM d'habitude)
            preview = plu_reg_text.strip().splitlines()
            preview = "\n".join(preview[: min(12, len(preview))])
            out.append("")
            out.append("Extraits du règlement PLU (indicatif) :")
            out.append(preview)

    # Nuisances sonores
    if bruit:
        out.append("")
        v = bruit.get("values", {})
        troncon = (v.get("nom_tronc") or ["?"])[0]
        cat = (v.get("cat_bruit") or ["?"])[0]
        st = bruit.get("surface_totals", {}).get("parcel_pct_total")
        cov = f" ({_fmt_pct(st)})" if isinstance(st, (int, float)) else ""
        out.append(f"Nuisances sonores routières : secteur lié au tronçon {troncon}, catégorie {cat}{cov}.")

    # Radon
    if radon:
        out.append("")
        v = radon.get("values", {})
        classe = (v.get("CLASSE_POT") or ["?"])[0]
        out.append(f"Radon : potentiel de classe {classe}.")

    # Conclusion simple
    out.append("")
    out.append("Conclusion : au vu des éléments, la parcelle est principalement contrainte par le PPRI et son zonage d’urbanisme. "
               "Les pourcentages et surfaces indiqués permettent d’apprécier la part de terrain concernée par chaque règle.")

    return "\n".join(out).strip() + "\n"


# ------------------------- Génération du rapport ------------------------ #
def generate_text_report(
    payload: Dict[str, Any],
    enable_plu_reg: bool = True,
    plu_reg_table: str = DEFAULT_PLU_REG_TABLE,
    plu_reg_column: str = DEFAULT_PLU_REG_COLUMN,
) -> str:
    # 1) Compact JSON (surfaces, totaux…)
    compact = _compact_payload(payload, max_values_per_attr=8, max_surfaces=60)

    # 2) Règlement PLU (optionnel)
    plu_reg_text = ""
    if enable_plu_reg:
        try:
            zones = _extract_plu_zones_from_payload(compact)
            plu_reg_text = _fetch_plu_regulation_for_zones(
                zones=zones,
                table=plu_reg_table,
                column=plu_reg_column,
                max_chars_total=12000,
            )
        except Exception as e:
            log.warning(f"⚠️ Impossible d'injecter le règlement PLU: {e}")
            plu_reg_text = ""

    # 3) Prompt LLM avec règlement (si dispo)
    prompt = _build_prompt_text(compact, plu_reg_text=plu_reg_text)

    # 4) LLM en qualité "moyenne" (comme avant)
    res = call_gpt5_text(prompt, reasoning_effort="medium", verbosity="medium")
    if res.get("success") and res.get("response"):
        return res["response"].strip() + "\n"

    # 5) Fallback déterministe enrichi d’un aperçu règlement si dispo
    return _deterministic_text(_add_surface_totals(payload), plu_reg_text=plu_reg_text)


# --------------------------------- CLI --------------------------------- #
def main():
    ap = argparse.ArgumentParser(description="Génère un rapport texte depuis un JSON d'intersections (avec règlement PLU optionnel).")
    ap.add_argument("--json", required=True, help="Chemin du rapport JSON (quick_intersections.json ou autre)")
    ap.add_argument("--out", default="rapport_cua.txt", help="Chemin de sortie .txt")
    ap.add_argument("--no-plu-reg", action="store_true", help="Désactive l'injection du règlement PLU")
    ap.add_argument("--plu-reg-table", default=DEFAULT_PLU_REG_TABLE, help="Table Supabase des extraits de règlement (def: plu_chunks)")
    ap.add_argument("--plu-reg-column", default=DEFAULT_PLU_REG_COLUMN, help="Nom de colonne du code de zone (def: zonage)")
    args = ap.parse_args()

    jpath = Path(args.json)
    if not jpath.exists():
        raise SystemExit(f"JSON introuvable: {jpath}")

    data = json.loads(jpath.read_text(encoding="utf-8"))
    txt = generate_text_report(
        data,
        enable_plu_reg=(not args.no_plu_reg),
        plu_reg_table=args.plu_reg_table,
        plu_reg_column=args.plu_reg_column,
    )

    out_txt = Path(args.out)
    out_txt.write_text(txt, encoding="utf-8")
    log.info(f"✅ Rapport texte écrit: {out_txt}")


if __name__ == "__main__":
    main()
