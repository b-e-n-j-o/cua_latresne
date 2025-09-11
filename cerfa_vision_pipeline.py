#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
cerfa_vision_pipeline.py — Pipeline simple « pages→images→LLM vision » pour CERFA CU (13410*11)

Principe :
  1) Convertir chaque page PDF en PNG.
  2) Interroger un modèle vision (ex: GPT‑4o) avec un prompt SPÉCIFIQUE par page.
  3) Agréger les résultats via un dernier appel texte (ex: GPT‑5) pour produire le JSON final + un rapport de conformité.

⚠️ Dépendances :
    pip install pdf2image pillow
    # et installer Poppler sur l’OS pour pdf2image

⚠️ llm_utils attendus :
    - call_gpt4o_vision(image_bytes: bytes, prompt: str, temperature: float = 0.0) -> {success, response, raw_response}
    - call_gpt5_text(prompt: str, reasoning_effort: str = "medium", verbosity: str = "medium") -> {success, response, raw_response}

Usage :
    python cerfa_vision_pipeline.py /chemin/cerfa.pdf --out-json sortie.json --out-dir ./out_pages

Sorties :
  - out_dir/page_001.png … images de pages
  - out_dir/page_001.json … JSON par page (réponse LLM vision nettoyée)
  - out_dir/pages_raw.jsonl … toutes les réponses brutes (1 ligne JSON par page)
  - sortie.json … synthèse finale (JSON demandé + conformité page par page)
"""

from __future__ import annotations
import argparse, json, logging, os
from pathlib import Path
from typing import Dict, Any, List, Optional

from pdf2image import convert_from_path
from PIL import Image

# ============================ Logging ======================================= #
logger = logging.getLogger("cerfa_vision")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ============================ LLM utils ===================================== #
try:
    from llm_utils import call_gpt4o_vision, call_gpt5_text  # type: ignore
except Exception as e:
    raise ImportError(
        "llm_utils doit exposer call_gpt4o_vision(...) et call_gpt5_text(...).\n"
        "Ajoutez une fonction vision dans llm_utils (voir docstring de ce script)."
    )

# ============================ Conversion PDF→PNG ============================ #

def pdf_page_to_png_bytes(pdf_path: Path, page_index_1: int, dpi: int = 200) -> bytes:
    imgs = convert_from_path(str(pdf_path), dpi=dpi, first_page=page_index_1, last_page=page_index_1)
    if not imgs:
        return b""
    img: Image.Image = imgs[0]
    from io import BytesIO
    buf = BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()

# ============================ Prompts par page ============================== #

def prompt_for_page(page_no: int, total_pages: int) -> str:
    """Prompts concis, calibrés sur CERFA 13410*11.\n"""
    common_rules = (
        "RÈGLES GÉNÉRALES:\n"
        "- Tu renvoies STRICTEMENT un JSON valide, sans prose.\n"
        "- Si une info est absente/illisible, mets null.\n"
        "- Conserve les zéros non significatifs (ex: '0496').\n"
        "- Détecte la présence de signatures/cachets/cases cochées (booléens).\n"
    )

    if page_no == 1:
        return (
            "Analyse CERFA CU — PAGE 1.\n" + common_rules +
            "CLÉS ATTENDUES:\n" \
            "{\n"
            "  \"header_cu\": {\"dept\": str|null, \"commune_code\": str|null, \"annee\": str|null, \"numero_dossier\": str|null},\n"
            "  \"date_reception_mairie\": str|null,\n"
            "  \"cachet_mairie_present\": bool,\n"
            "  \"signature_receveur_presente\": bool,\n"
            "  \"type_cu\": \"information\"|\"operationnel\"|null,\n"
            "  \"demandeur\": {\"type\": \"particulier\"|\"personne_morale\"|null, \"nom\": str|null, \"prenom\": str|null, \"denomination\": str|null, \"siret\": str|null, \"representant_nom\": str|null, \"representant_prenom\": str|null},\n"
            "  \"conformite_page\": {\"zones_obligatoires_remplies\": bool, \"commentaires\": [str]}\n"
            "}\n"
            "EXTRAIS ces champs à partir de l'image fournie.\n"
        )

    if page_no == 2:
        return (
            "Analyse CERFA CU — PAGE 2.\n" + common_rules +
            "CLÉS ATTENDUES:\n" \
            "{\n"
            "  \"coord_demandeur\": {\"adresse\": {\"numero\": str|null, \"voie\": str|null, \"lieu_dit\": str|null, \"commune\": str|null, \"code_postal\": str|null}},\n"
            "  \"terrain_adresse\": {\"numero\": str|null, \"voie\": str|null, \"lieu_dit\": str|null, \"commune\": str|null, \"code_postal\": str|null},\n"
            "  \"ref_cadastrales_p1\": [{\"prefixe\": str|null, \"section\": str|null, \"numero\": str|null, \"superficie_m2\": str|null}],\n"
            "  \"equipements_existants\": {\"voirie\": bool|null, \"eau\": bool|null, \"assainissement\": bool|null, \"electricite\": bool|null},\n"
            "  \"conformite_page\": {\"zones_obligatoires_remplies\": bool, \"commentaires\": [str]}\n"
            "}\n"
        )

    if page_no == 3:
        return (
            "Analyse CERFA CU — PAGE 3.\n" + common_rules +
            "CLÉS ATTENDUES:\n" \
            "{\n"
            "  \"equipements_prevus\": {\"voirie\": bool|null, \"eau\": bool|null, \"assainissement\": bool|null, \"electricite\": bool|null},\n"
            "  \"engagement\": {\"lieu\": str|null, \"date_signature\": str|null, \"signature_demandeur_presente\": bool},\n"
            "  \"conformite_page\": {\"zones_obligatoires_remplies\": bool, \"commentaires\": [str]}\n"
            "}\n"
            "Les signatures peuvent etre aussi un simple oui, un cachet, le nom, etc... analyse le cadre prévu a cette effigie."
        )

    if page_no == 4:
        return (
            "Analyse CERFA CU — PAGE 4 (Fiche complémentaire références cadastrales).\n" + common_rules +
            "CLÉS ATTENDUES:\n" \
            "{\n"
            "  \"ref_cadastrales_complementaires\": [{\"prefixe\": str|null, \"section\": str|null, \"numero\": str|null, \"superficie_m2\": str|null}],\n"
            "  \"superficie_totale_m2\": str|null,\n"
            "  \"conformite_page\": {\"zones_obligatoires_remplies\": bool, \"commentaires\": [str]}\n"
            "}\n"
            "Toutes lesrubriques ne sont pas forcément obligatoires il s'agit de complement de parcelles a la parcelle pricnipale, ne renvoie rien pour les cases vides, pour la conformité assure toi que la superficie globale soit bine indiquée."
        )

    if page_no == 7:
        return (
            "Analyse CERFA CU — PAGE 7 (Pièces à joindre).\n" + common_rules +
            "CLÉS ATTENDUES:\n" \
            "{\n"
            "  \"pieces_jointes\": {\"CU1\": bool|null, \"CU2\": bool|null, \"CU3\": bool|null},\n"
            "  \"conformite_page\": {\"zones_obligatoires_remplies\": bool, \"commentaires\": [str]}\n"
            "}\n"
            "Pour la conformité assure toi que les pièces jointes soient bien cochées."
        )

    if page_no == 8:
        return (
            "Analyse CERFA CU — PAGE 8 (Note descriptive succincte).\n" + common_rules +
            "CLÉS ATTENDUES:\n" \
            "{\n"
            "  \"note_descriptive_presente\": bool,\n"
            "  \"resume_note\": str|null,\n"
            "  \"conformite_page\": {\"zones_obligatoires_remplies\": bool, \"commentaires\": [str]}\n"
            "}\n"
            "Pour la conformité ici c'est une page de complément, de commentaire, il n'est pas obligatoire de remplir quelque chose."
        )

    # Par défaut (pages 5-6) : peu d'info utile → retour minimal
    return (
        f"Analyse CERFA CU — PAGE {page_no}.\n" + common_rules +
        "CLÉS ATTENDUES:\n{"
        "  \"conformite_page\": {\"zones_obligatoires_remplies\": bool, \"commentaires\": [str]}\n"
        "}"
    )

# ============================ Helpers JSON ================================= #

def extract_json_from_text(text: str) -> Optional[Dict[str, Any]]:
    t = (text or "").strip()
    # Nettoyage éventuels fences ```json ... ```
    if t.startswith("```"):
        t = t.strip("`")
        t = t.lstrip("json\n\r")
    i, j = t.find("{"), t.rfind("}")
    if i == -1 or j == -1 or j <= i:
        return None
    try:
        return json.loads(t[i:j+1])
    except Exception:
        return None

# ============================ Synthèse finale =============================== #

def build_synthesis_prompt(page_results: List[Dict[str, Any]]) -> str:
    schema = {
        "nom_demandeur": None,
        "prenom_demandeur": None,
        "denomination": None,
        "adresse_terrain": {"numero": None, "voie": None, "lieu_dit": None, "commune": None, "code_postal": None},
        "commune_insee": None,
        "references_cadastrales": [{"section": "AC", "numero": "0496", "prefixe": None, "superficie_m2": None}],
        "numero_cu": None,
        "date_depot": None,
        "conformite": {
            "global": None,
            "issues": [],
            "page_checks": {}
        }
    }
    return (
        "Synthétise les EXTRACTIONS PAGE PAR PAGE ci-dessous en un JSON FINAL STRICT.\n"
        "RÈGLES:\n"
        "- Respecte STRICTEMENT le schéma indiqué.\n"
        "- Ne renvoie que du JSON (pas de texte libre).\n"
        "- Déduplique toutes les références cadastrales (section+numero), conserve les zéros.\n"
        "- Préfère l'adresse du terrain issue de la section 4.1.\n"
        "- Pour 'commune_insee', renvoie le code INSEE si présent (pas le code postal).\n"
        "- 'date_depot' = date de réception mairie si présente, sinon 'Fait le' (JJ/MM/AAAA).\n"
        "- 'numero_cu' = concat plausible du cartouche (Dpt-Commune-Année-N°).\n"
        "- 'conformite.global' = vrai si signatures/cachets/sections obligatoires sont présents; sinon faux.\n"
        f"SCHÉMA: \n{json.dumps(schema, ensure_ascii=False, indent=2)}\n\n"
        f"ENTRÉES: \n{json.dumps(page_results, ensure_ascii=False)}\n"
    )

# ============================ Orchestration ================================= #

def run(pdf: Path, out_json: Path, out_dir: Path, dpi: int = 200) -> Dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    pages = convert_from_path(str(pdf), dpi=dpi)
    total = len(pages)
    logger.info(f"PDF: {pdf} — {total} page(s)")

    rawl_path = out_dir / "pages_raw.jsonl"
    with open(rawl_path, "w", encoding="utf-8") as rawl:
        page_results: List[Dict[str, Any]] = []

        for idx in range(1, total + 1):
            # Ignorer les pages 5 et 6 (peu d'info utile)
            if idx in [5, 6]:
                logger.info(f"→ Page {idx}/{total} : ignorée (pages 5-6)")
                continue
                
            logger.info(f"→ Page {idx}/{total} : conversion & analyse vision…")

            # Sauvegarde image
            img_bytes = pdf_page_to_png_bytes(pdf, idx, dpi=dpi)
            img_path = out_dir / f"page_{idx:03d}.png"
            with open(img_path, "wb") as f:
                f.write(img_bytes)

            # Prompt spécifique à la page
            prompt = prompt_for_page(idx, total)

            # Appel vision
            r = call_gpt4o_vision(img_bytes, prompt=prompt, temperature=0.0)
            if not r.get("success"):
                logger.error(f"LLM vision échec page {idx}: {r.get('error')}")
                page_json = {"page": idx, "error": r.get("error")}
            else:
                txt = r.get("response") or ""
                pj = extract_json_from_text(txt)
                if not pj:
                    logger.warning(f"Réponse non‑JSON page {idx} → encapsulation brute")
                    page_json = {"page": idx, "raw": txt}
                else:
                    pj["page"] = idx
                    page_json = pj

            # Sauvegardes intermédiaires
            (out_dir / f"page_{idx:03d}.json").write_text(json.dumps(page_json, ensure_ascii=False, indent=2), encoding="utf-8")
            rawl.write(json.dumps(page_json, ensure_ascii=False) + "\n")
            page_results.append(page_json)

    # Synthèse finale (texte)
    logger.info("→ Synthèse finale (GPT‑5 texte)…")
    s_prompt = build_synthesis_prompt(page_results)
    s_res = call_gpt5_text(prompt=s_prompt, reasoning_effort="low", verbosity="low")
    if not s_res.get("success"):
        logger.error(f"Échec synthèse GPT‑5: {s_res.get('error')}")
        final = {"success": False, "error": s_res.get("error"), "pages": page_results}
    else:
        txt = s_res.get("response") or ""
        final_json = extract_json_from_text(txt)
        if not final_json:
            logger.error("Synthèse: impossible de décoder le JSON renvoyé par le modèle.")
            final = {"success": False, "error": "synthesis_json_decode_error", "raw": txt, "pages": page_results}
        else:
            final = {"success": True, "data": final_json, "pages": page_results}

    out_json.write_text(json.dumps(final, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"✅ Terminé → {out_json}")
    return final

# ============================ CLI =========================================== #

def main():
    ap = argparse.ArgumentParser(description="Pipeline vision pour CERFA CU (page-wise)")
    ap.add_argument("pdf", type=str, help="Chemin du CERFA PDF")
    ap.add_argument("--out-json", type=str, default="cerfa_vision_result.json", help="Chemin de sortie JSON")
    ap.add_argument("--out-dir", type=str, default="cerfa_pages_out", help="Dossier de sorties intermédiaires")
    ap.add_argument("--dpi", type=int, default=200, help="DPI de rendu des pages")
    args = ap.parse_args()

    pdf = Path(args.pdf)
    out_json = Path(args.out_json)
    out_dir = Path(args.out_dir)

    if not pdf.exists():
        raise SystemExit(f"PDF introuvable: {pdf}")

    run(pdf, out_json, out_dir, dpi=args.dpi)

if __name__ == "__main__":
    main()
