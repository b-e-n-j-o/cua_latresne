# report_from_json.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import json, argparse, logging, re
from pathlib import Path
from typing import Dict, Any, List, Optional

from llm_utils import call_gpt5_text  # wrapper existant

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("report_from_json_text")


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


# ----------------------------- Prompt LLM ----------------------------- #
def _build_prompt_text(compact_json: Dict[str, Any]) -> str:
    """
    Objectif : produire un rapport PARAGRAPHIQUE (pas de Markdown),
    en français, avec consolidation PPRI / PLU / Nuisances / Radon, chiffres (m², %) et sans doublons.
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

Structure attendue :
1) En-tête : commune (INSEE), parcelle (section-numéro) et surface estimée si présente.
2) Résumé exécutif : 3 à 6 phrases qui donnent l'essentiel.
3) Développement par thématiques (si présentes) :
   - PPRI / inondation : consolider assiette/générateur/prescription ; détailler la répartition par zones (ex: Rouge urbanisé, Bleu) avec m² et % cumulés.
   - Urbanisme (PLU) : zonage de la parcelle (ex: zone N), avec couverture.
   - Nuisances sonores routières : tronçon concerné, catégorie de bruit, couverture.
   - Radon : classe de potentiel.
   - Autres éléments significatifs s'il y en a.
4) Conclusion : 2 à 4 phrases sur le niveau de contrainte et les points d'attention.

Rappels :
- N'évoque jamais une thématique absente du JSON.
- Si la répartition par zones est disponible (ex: n_zone_reg_ppri_033 avec codezone), affiche la couverture de chaque zone.
- Ne colle pas les identifiants internes (id, fid, ctid...)."""

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
    )


# ------------------- Fallback déterministe (texte) -------------------- #
def _find_layer(results: List[Dict[str, Any]], table: str) -> Optional[Dict[str, Any]]:
    for l in results:
        if l.get("table") == table:
            return l
    return None

def _deterministic_text(payload: Dict[str, Any]) -> str:
    """
    Texte lisible si l'appel LLM échoue.
    Regroupe l'essentiel : PPRI (avec répartition), PLU, Nuisances, Radon, synthèse.
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
    plu = _find_layer(results, "b_zonage_plu")

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
        # couverture globale issue des totaux si dispo
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

        # répartition des zones (si disponible)
        if ppri_zone and isinstance(ppri_zone.get("surfaces"), list):
            # sommer par codezone si besoin
            by_zone: Dict[str, float] = {}
            for s in ppri_zone["surfaces"]:
                a = s.get("inter_area_m2") or 0.0
                # essaye d'utiliser 'codezone' dans values si présent
                # ici, on retombe sur les valeurs distinctes disponibles
            vals = ppri_zone.get("values", {})
            zones = vals.get("codezone") or []
            # Si on a deux surfaces, on prend l'ordre décroissant pour associer aux valeurs
            surfs_sorted = sorted(
                [s for s in ppri_zone["surfaces"] if isinstance(s.get("inter_area_m2"), (int, float))],
                key=lambda s: s["inter_area_m2"],
                reverse=True
            )
            # Texte indicatif : on affiche les surfaces telles que calculées
            parts = []
            for s in surfs_sorted[:3]:
                pct = _fmt_pct(s.get("pct_of_parcel"))
                area = _fmt_m2(s.get("inter_area_m2"))
                parts.append(f"{area} ({pct})")
            if parts:
                p.append("Répartition PPRI sur la parcelle : " + " ; ".join(parts) + ".")

        # cotes de seuil si présentes
        if l_cotes and l_cotes.get("values"):
            c = l_cotes["values"]
            codes = c.get("codezone") or []
            if codes:
                uniq = ", ".join(sorted(set(codes)))
                p.append(f"Cotes de seuil présentes : {uniq}.")

        out.append(" ".join(p))

    # Urbanisme – PLU
    if plu:
        out.append("")
        v = plu.get("values", {})
        zone = (v.get("libelle") or v.get("typezone") or ["?"])[0]
        tot = plu.get("surface_totals", {}).get("parcel_pct_total")
        cov = f" ({_fmt_pct(tot)})" if isinstance(tot, (int, float)) else ""
        out.append(f"Zonage PLU : la parcelle est classée en zone {zone}{cov}.")

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
def generate_text_report(payload: Dict[str, Any]) -> str:
    compact = _compact_payload(payload, max_values_per_attr=8, max_surfaces=60)
    prompt = _build_prompt_text(compact)
    # LLM en haute qualité
    res = call_gpt5_text(prompt, reasoning_effort="medium", verbosity="medium")
    if res.get("success") and res.get("response"):
        return res["response"].strip() + "\n"
    # Fallback déterministe
    return _deterministic_text(_add_surface_totals(payload))


# --------------------------------- CLI --------------------------------- #
def main():
    ap = argparse.ArgumentParser(description="Génère un rapport texte depuis un JSON d'intersections.")
    ap.add_argument("--json", required=True, help="Chemin du rapport JSON (quick_intersections.json ou autre)")
    ap.add_argument("--out", default="rapport_cua.txt", help="Chemin de sortie .txt")
    args = ap.parse_args()

    jpath = Path(args.json)
    if not jpath.exists():
        raise SystemExit(f"JSON introuvable: {jpath}")

    data = json.loads(jpath.read_text(encoding="utf-8"))
    txt = generate_text_report(data)

    out_txt = Path(args.out)
    out_txt.write_text(txt, encoding="utf-8")
    log.info(f"✅ Rapport texte écrit: {out_txt}")


if __name__ == "__main__":
    main()
