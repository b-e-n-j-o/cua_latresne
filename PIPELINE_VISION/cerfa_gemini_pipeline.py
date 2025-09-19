#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pipeline OCR→LLM (Gemini) pour CERFA CU (13410*11)

⚠️ Modifié : 
- Le champ `commune_insee` n’est PLUS extrait par Gemini, il est toujours laissé à null.
- Le code INSEE est déterminé uniquement via le CSV des communes (v_commune_2025.csv).
"""

import argparse, json, logging, os, re, tempfile, hashlib, time, random
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from pypdf import PdfReader, PdfWriter
import dotenv

dotenv.load_dotenv()

from google import genai
from google.genai import types

# Import du juge pour validation des métadonnées
try:
    from cerfa_meta_judge import judge_meta
except ImportError:
    # Si le juge n'est pas disponible, on crée une fonction de fallback
    def judge_meta(meta: dict) -> dict:
        logger.warning("⚠️ cerfa_meta_judge non disponible, validation basique uniquement")
        return {"pass": True, "reasons": [], "autofixes": {}, "must_rerun": False}

# ============================ Logging ======================================= #
logger = logging.getLogger("cerfa_gemini")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ============================ Utils communs (INSEE, dates, SIRET) =========== #
_DOM_DEPT = {"971", "972", "973", "974", "976"}

def _to_iso_date(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    from datetime import datetime
    s = str(s).strip()
    for fmt in ("%d/%m/%Y", "%d%m%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except Exception:
            pass
    return None

def _luhn_ok(num: str) -> bool:
    digits = [int(c) for c in re.sub(r"\D","", num)]
    if len(digits) != 14:
        return False
    s, parity = 0, len(digits) % 2
    for i, d in enumerate(digits):
        if i % 2 == parity:
            d *= 2
            if d > 9:
                d -= 9
        s += d
    return (s % 10) == 0

def _norm(s: str) -> str:
    """Normalisation pour matcher les noms de communes avec le CSV."""
    if not s:
        return ""
    s = str(s).lower()
    s = re.sub(r"[-']", " ", s)        # unifie tirets et apostrophes
    s = re.sub(r"\s+", " ", s)         # compresse espaces multiples
    return s.strip()

def get_insee_from_csv(csv_path: str, commune_name: Optional[str], department_code: Optional[str]) -> Optional[str]:
    if not commune_name:
        return None
    try:
        df = pd.read_csv(csv_path, dtype=str)
    except Exception as e:
        logger.warning(f"INSEE CSV lecture échouée ({csv_path}): {e}")
        return None
    if df.empty or "COM" not in df.columns:
        logger.warning("INSEE CSV invalide (colonne 'COM' absente).")
        return None

    name = _norm(commune_name)
    df["LIBELLE_n"] = df.get("LIBELLE", "").map(_norm)
    df["NCCENR_n"] = df.get("NCCENR", "").map(_norm) if "NCCENR" in df.columns else ""

    mask = (df["LIBELLE_n"] == name) | (df["NCCENR_n"] == name)
    if department_code and "DEP" in df.columns:
        mask = mask & (df["DEP"].str.upper().str.zfill(2) == str(department_code).upper().zfill(2))

    sub = df[mask]
    if len(sub) == 1:
        return str(sub.iloc[0]["COM"])
    if len(sub) > 1:
        logger.warning(f"INSEE CSV ambigu ({commune_name}, dep={department_code}) → {len(sub)} correspondances.")
    return None

# ============================ Page skipping ================================= #
def build_reduced_pdf(src_pdf: Path, pages_to_keep: List[int]) -> Path:
    reader, writer = PdfReader(str(src_pdf)), PdfWriter()
    for i in pages_to_keep:
        if 1 <= i <= len(reader.pages):
            writer.add_page(reader.pages[i-1])
    tmp = Path(tempfile.mkstemp(prefix="cerfa_reduced_", suffix=".pdf")[1])
    with open(tmp,"wb") as f:
        writer.write(f)
    return tmp

# ============================ Prompts ======================================= #
def build_master_prompt(total_pages: int, kept_pages: List[int], source_file: str) -> str:
    schema = {
      "cerfa_reference": None,
      "commune_nom": None,
      "commune_insee": None,   # ⚠️ toujours null
      "departement_code": None,
      "numero_cu": None,
      "type_cu": None,
      "date_depot": None,
      "demandeur": {
        "type": None,
        "nom": None,
        "prenom": None,
        "denomination": None,
        "siret": None
      },
      "coord_demandeur": {
        "adresse": {
          "numero": None, "voie": None, "lieu_dit": None,
          "code_postal": None, "ville": None
        },
        "contact": {"telephone": None, "email": None}
      },
      "mandataire": {},
      "adresse_terrain": {
        "numero": None, "voie": None, "lieu_dit": None,
        "code_postal": None, "ville": None
      },
      "references_cadastrales": [
        {"section": None, "numero": None}
      ],
      "superficie_totale_m2": None,
      "header_cu": {"dept": None, "commune_code": None, "annee": None, "numero_dossier": None},
      "source_file": None
    }

    rules = [
      "RENVOIE STRICTEMENT du JSON valide selon le schéma ci-dessous (aucun texte libre).",
      "Ne crée PAS d'autres clés que celles du schéma.",
      "Toujours laisser `commune_insee` = null (il sera complété via le CSV).",
      "Conserve les zéros significatifs des numéros de parcelles (ex: '0496').",
      "SIRET : DOIT comporter 14 chiffres; si incertain ou illisible, renvoyer null.",
      "Ne pas inventer le code INSEE.",
      "Dates : si possible renvoyer 'date_depot' au format ISO 'YYYY-MM-DD'; sinon garde tel quel.",
      "Si une rubrique n'apparaît pas, mets null (ou {}/[] vides pour les objets/listes).",
      "Différencie bien la commune du demandeur et la commune du terrain : c’est la commune du terrain qui est importante."
      "Sur la première page, le departement ou dept est souvent paddé d'un 0 devant le numéro de département, donc à bien prendre en compte qu'il peut être 033 et utilser 33 comme valeur, ne pas s'arreter à 03"
      "Dans le cadre d'identification du cerfa page 1 qui contient le departmeent le schema est : 'CU', puis departmeent en 3 chiffres (les deux derniers à garder), puis commune en 3 chiffres, puis année en deux chiffres les deux derniers de l'année, et numero de dossier souvent 'Xnuméro', donc un exemple, serait 'CU-033-234-24-X0052'"
    ]

    hints = {
      1: "Page 1: cerfa_reference, commune_nom, departement_code, header_cu, type_cu, demandeur, date_depot.",
      2: "Page 2: coord_demandeur, mandataire, adresse_terrain, references_cadastrales.",
      3: "Page 3: engagement (lieu/date), équipements prévus si renseignés.",
      4: "Page 4: références cadastrales complémentaires + superficie_totale_m2."
    }

    parts = [
        "\n".join(rules),
        "SCHEMA:\n" + json.dumps(schema, ensure_ascii=False, indent=2)
    ]
    for p in kept_pages:
        parts.append(f"=== PAGE {p} ===\nHINT:\n{hints.get(p, f'Page {p}: vérifier les informations utiles si présentes.')}")

    parts.append(f"SOURCE_FILE:\n{json.dumps(source_file)}")
    return "\n".join(parts)

# ============================ Extraction JSON robuste ======================= #
def _extract_json(text: str) -> Optional[dict]:
    t = (text or "").strip()
    if t.startswith("```"):
        t = t.strip("`")
        t = t.lstrip("json\n\r")
    i, j = t.find("{"), t.rfind("}")
    if i == -1 or j == -1 or j <= i:
        return None
    raw = t[i:j+1]
    try:
        return json.loads(raw)
    except Exception:
        raw2 = re.sub(r",\s*}", "}", raw)
        raw2 = re.sub(r",\s*]", "]", raw2)
        try:
            return json.loads(raw2)
        except Exception:
            return None

# ============================ Post-traitement =============================== #
def _normalize_dept_for_numero(dep_raw: str) -> str:
    d = (dep_raw or "").strip().upper()
    if not d:
        return d
    if d in {"2A", "2B"}:
        return d
    if d in _DOM_DEPT:
        return d
    d2 = re.sub(r"\D", "", d)
    if not d2:
        return d
    if len(d2) == 1:
        d2 = d2.zfill(2)
    return d2

def sanitize_and_enrich(final: Dict[str, Any], *, insee_csv: Optional[str], pdf_name: str) -> Dict[str, Any]:
    if not final.get("success") or not isinstance(final.get("data"), dict):
        return final

    meta = final["data"]

    if not meta.get("source_file"):
        meta["source_file"] = pdf_name

    # Date ISO
    if meta.get("date_depot"):
        iso = _to_iso_date(meta["date_depot"])
        if iso:
            meta["date_depot"] = iso

    # INSEE forcé via CSV
    csv_insee = None
    if insee_csv and meta.get("commune_nom"):
        csv_insee = get_insee_from_csv(insee_csv, meta.get("commune_nom"), meta.get("departement_code"))
    if csv_insee:
        meta["commune_insee"] = csv_insee
        logger.info(f"INSEE défini via CSV: {csv_insee}")
    else:
        logger.error(f"❌ Impossible de trouver le code INSEE dans le CSV pour la commune='{meta.get('commune_nom')}', dep='{meta.get('departement_code')}'")
        meta["commune_insee"] = None

    # Numéro CU reconstruit si absent
    if not meta.get("numero_cu"):
        hdr = meta.get("header_cu") or {}
        dep_raw = (meta.get("departement_code") or hdr.get("dept") or "")
        dep_for_num = _normalize_dept_for_numero(dep_raw)
        insee_code = meta.get("commune_insee") or ""
        an = (hdr.get("annee") or "").strip()
        nd = (hdr.get("numero_dossier") or "").strip()
        if dep_for_num and insee_code and an and nd:
            yyyy = f"20{an.zfill(2)}" if len(an) == 2 else an
            meta["numero_cu"] = f"{dep_for_num}-{insee_code}-{yyyy}-{nd}"

    final["data"] = meta
    return final

# ============================ Orchestration ================================= #
def run(pdf: Path, out_json: Path, out_dir: Path, *,
        skip_pages: Optional[List[int]] = None,
        insee_csv: Optional[str] = None,
        model: str = "gemini-2.5-flash",
        judge: bool = True,
        max_judge_retries: int = 2) -> Dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)

    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("⚠️ GEMINI_API_KEY non défini")
    client = genai.Client(api_key=api_key)

    pdf_sha1 = hashlib.sha1(pdf.read_bytes()).hexdigest()
    logger.info(f"PDF: {pdf.name} — sha1={pdf_sha1}")

    reader = PdfReader(str(pdf))
    total = len(reader.pages)
    if skip_pages:
        keep = [i for i in range(1, total + 1) if i not in set(skip_pages)]
        logger.info(f"PDF original {total} pages. On garde: {keep}")
        target_pdf = build_reduced_pdf(pdf, keep)
    else:
        keep = list(range(1, total + 1))
        target_pdf = pdf

    master_prompt = build_master_prompt(total, keep, pdf.name)
    (out_dir / "master_prompt.txt").write_text(master_prompt, encoding="utf-8")

    # Boucle de validation avec retry automatique
    judge_attempt = 0
    final = None
    judge_results = []

    while judge_attempt <= max_judge_retries:
        judge_attempt += 1
        
        logger.info(f"→ Appel Gemini avec PDF complet (tentative validation {judge_attempt}/{max_judge_retries + 1})…")
        
        # Appel Gemini avec retry sur erreurs techniques
        max_retries = 5
        backoff_base = 5.0
        text = None
        for attempt in range(1, max_retries + 1):
            try:
                response = client.models.generate_content(
                    model=model,
                    contents=[
                        types.Part.from_bytes(data=target_pdf.read_bytes(), mime_type="application/pdf"),
                        master_prompt
                    ]
                )
                text = response.text or ""
                break
            except Exception as e:
                err_msg = str(e)
                if attempt < max_retries and ("503" in err_msg or "UNAVAILABLE" in err_msg or "overloaded" in err_msg.lower()):
                    wait = backoff_base * attempt + random.uniform(0, 2)
                    logger.warning(f"⚠️ Tentative {attempt}/{max_retries} échouée ({err_msg}). Retry dans {wait:.1f}s…")
                    time.sleep(wait)
                    continue
                else:
                    final = {"success": False, "error": err_msg, "stage": "gemini_call"}
                    out_json.write_text(json.dumps(final, ensure_ascii=False, indent=2), encoding="utf-8")
                    logger.error(f"Échec appel Gemini: {err_msg}")
                    return final

        # Parsing et enrichissement
        parsed = _extract_json(text)
        if not parsed:
            final = {"success": False, "error": "json_parse_error", "raw": text}
            break
        else:
            final = {"success": True, "data": parsed}

        final = sanitize_and_enrich(final, insee_csv=insee_csv, pdf_name=pdf.name)

        # Validation avec le juge si activé
        if judge and final.get("success") and final.get("data"):
            logger.info(f"🔍 Validation des données extraites (juge, tentative {judge_attempt})...")
            judge_result = judge_meta(final["data"])
            judge_results.append(judge_result)
            
            # Sauvegarde du résultat du juge
            judge_file = out_dir / f"judge_result_attempt_{judge_attempt}.json"
            judge_file.write_text(json.dumps(judge_result, ensure_ascii=False, indent=2), encoding="utf-8")
            
            if judge_result.get("pass", False):
                logger.info("✅ Validation réussie ! Données acceptées par le juge.")
                
                # Application des autofixes si disponibles
                autofixes = judge_result.get("autofixes", {})
                if autofixes:
                    logger.info("🔧 Application des corrections automatiques du juge...")
                    for key, value in autofixes.items():
                        if value is not None:
                            final["data"][key] = value
                            logger.info(f"  - {key} corrigé : {value}")
                
                break
            else:
                reasons = judge_result.get("reasons", [])
                must_rerun = judge_result.get("must_rerun", False)
                
                logger.warning(f"❌ Validation échouée (tentative {judge_attempt}):")
                for reason in reasons:
                    logger.warning(f"  - {reason}")
                
                if judge_attempt > max_judge_retries or not must_rerun:
                    logger.error(f"💥 Échec définitif après {judge_attempt} tentatives de validation")
                    final["judge_validation"] = {
                        "final_pass": False,
                        "attempts": judge_results,
                        "final_reasons": reasons
                    }
                    break
                else:
                    logger.info(f"🔄 Nouvelle tentative d'extraction recommandée par le juge...")
                    # On continue la boucle pour une nouvelle extraction
        else:
            # Pas de juge ou échec avant validation
            break

    # Ajout des informations de validation au résultat final
    if judge and judge_results:
        if "judge_validation" not in final:
            final["judge_validation"] = {
                "final_pass": judge_results[-1].get("pass", False),
                "attempts": judge_results,
                "total_attempts": len(judge_results)
            }

    out_json.write_text(json.dumps(final, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"✅ Terminé → {out_json}")

    print("\n=== CERFA Gemini JSON (aperçu) ===")
    print(json.dumps(final, ensure_ascii=False, indent=2))

    return final

# ============================ CLI =========================================== #
def main():
    ap = argparse.ArgumentParser(description="Pipeline OCR→LLM (Gemini) pour CERFA CU (13410*11)")
    ap.add_argument("pdf", type=str, help="Chemin du CERFA PDF")
    ap.add_argument("--out-json", type=str, default="cerfa_gemini_result.json", help="Chemin de sortie JSON")
    ap.add_argument("--out-dir", type=str, default="cerfa_gemini_out", help="Dossier de sorties intermédiaires")
    ap.add_argument("--skip-pages", type=str, default="5,6,7", help="Pages à ignorer, ex: '5,6,7'")
    ap.add_argument("--insee-csv", type=str, default="CONFIG/v_commune_2025.csv", help="CSV communes INSEE")
    ap.add_argument("--model", type=str, default="gemini-2.5-flash", help="Modèle Gemini")
    ap.add_argument("--judge", action="store_true", default=True, help="Activer l'appel au judge pour validation (défaut: True)")
    ap.add_argument("--no-judge", action="store_false", dest="judge", help="Désactiver l'appel au judge")
    ap.add_argument("--max-judge-retries", type=int, default=2, help="Nombre max de tentatives si le juge refuse (défaut: 2)")
    args = ap.parse_args()

    pdf = Path(args.pdf)
    if not pdf.exists():
        raise SystemExit(f"PDF introuvable: {pdf}")

    skip_pages = [int(x) for x in args.skip_pages.split(",") if x.strip().isdigit()] if args.skip_pages else None

    run(
        pdf,
        Path(args.out_json),
        Path(args.out_dir),
        skip_pages=skip_pages,
        insee_csv=args.insee_csv,
        model=args.model,
        judge=args.judge,
        max_judge_retries=args.max_judge_retries
    )

if __name__ == "__main__":
    main()
