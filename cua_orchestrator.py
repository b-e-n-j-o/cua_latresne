# cua_orchestrator.py
import argparse
import json
import logging
import os
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy import create_engine, text as sql_text
from jose import jwt  # pip install python-jose

# ⬇️ Import direct des étapes
from PIPELINE_VISION.cerfa_gemini_pipeline import run as run_cerfa_gemini
from INTERSECTIONS.intersections_parcelle import run_intersections
from CUA_GENERATION.cua_builder import build_cua_docx
import MAP_GENERATION.bbox_map as bbox_map
from UTILS.storage_service import upload_artifact
from UTILS.notifier import render_email, send_mail

log = logging.getLogger("cua_orchestrator")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")

STORAGE_BUCKET = "cua-artifacts"


# ------------------------ Utils ------------------------

def _to_parcel_arg(refs: List[Dict[str, str]]) -> str:
    """Transforme une liste de références cadastrales en une chaîne 'AC 0494, AD 0123'."""
    parts = []
    for r in refs or []:
        sec = (r.get("section") or "").strip().upper()
        num = str(r.get("numero") or "").strip().zfill(4)
        if sec and num:
            parts.append(f"{sec} {num}")
    return ", ".join(parts)


def _get_db_engine():
    url = os.getenv("SUPABASE_DATABASE_URL") or os.getenv("DATABASE_URL")
    if not url:
        log.warning("Pas d'URL DB (SUPABASE_DATABASE_URL/DATABASE_URL). Les écritures DB seront ignorées.")
        return None
    try:
        return create_engine(url, pool_pre_ping=True, future=True)
    except Exception as e:
        log.error(f"Connexion DB impossible : {e}")
        return None


def _upload_or_none(path: Path) -> Optional[str]:
    try:
        return upload_artifact(STORAGE_BUCKET, path, str(uuid.uuid4()))
    except Exception as e:
        log.error(f"Upload artefact échoué ({path}): {e}")
        return None


def _maybe_send_notification(result: Dict[str, Any], notify_emails: Optional[List[str]]):
    if not (notify_emails and result.get("success")):
        return
    try:
        # Label d’info simple
        ctx = result.get("result_json", {}) or {}
        first_label = None
        try:
            reports = (ctx.get("reports") or [])
            if reports and "parcel" in reports[0]:
                first_label = reports[0]["parcel"].get("label")
        except Exception:
            pass
        subject = f"[CUA] Rapport disponible — {first_label or 'Dossier'}"

        html = render_email(
            "report_ready.html",
            parcel_label=first_label or "Dossier",
            insee=ctx.get("insee"),
            report_docx_url=result.get("report_docx_url"),
            map_html_url=result.get("map_html_url"),
            portal_url=os.getenv("PORTAL_BASE_URL", "").rstrip("/") or None,
        )
        ok = send_mail(notify_emails, subject, html)
        if ok:
            log.info("Notification envoyée : %s", ", ".join(notify_emails))
        else:
            log.warning("Échec d’envoi de notification : %s", ", ".join(notify_emails))
        result["email_sent"] = ok
        result["email_recipients"] = notify_emails
    except Exception as e:
        log.error(f"Erreur notification: {e}")
        result["email_error"] = str(e)


def _extract_user_id_from_jwt(access_token: str) -> Optional[str]:
    """Récupère l'UUID utilisateur depuis un JWT Supabase (champ sub)."""
    try:
        payload = jwt.get_unverified_claims(access_token)
        return payload.get("sub")
    except Exception as e:
        log.error(f"Impossible d'extraire user_id du token : {e}")
        return None


# ------------------------ Pipelines ------------------------

def run_full_pipeline(
    pdf_path: str,
    temp_dir: Path,
    insee_csv_path: str,
    mapping_path: str,
    user_id: Optional[str] = None,
    access_token: Optional[str] = None,
    notify_emails: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Pipeline « PDF CERFA → CUA » :
    1) Extraction CERFA (Gemini)
    2) Intersections parcelles
    3) DOCX CUA
    4) Carte HTML
    5) Upload artefacts
    6) (optionnel) Écriture DB + notification
    """
    temp_dir.mkdir(parents=True, exist_ok=True)
    cerfa_json_path = temp_dir / "cerfa_gemini.json"
    inters_json_path = temp_dir / "intersections.json"
    docx_path = temp_dir / "CUA_final.docx"
    map_path = temp_dir / "map.html"

    try:
        # 1) CERFA
        log.info("▶️ Étape 1/6 : Extraction CERFA (Gemini)…")
        cerfa = run_cerfa_gemini(
            pdf=Path(pdf_path),
            out_json=cerfa_json_path,
            out_dir=temp_dir,
            skip_pages=[5, 6, 7],
            insee_csv=insee_csv_path,
            model=os.getenv("GEMINI_MODEL", "gemini-2.5-flash"),
            judge=True,
            max_judge_retries=2,
        )
        (temp_dir / "cerfa_gemini_result.pretty.json").write_text(
            json.dumps(cerfa, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        meta = cerfa.get("data") or {}
        commune = (meta.get("commune_nom") or "").strip()
        departement = (meta.get("departement_code") or "").strip()
        refs = meta.get("references_cadastrales") or []
        parcels = _to_parcel_arg(refs)
        if not all([commune, departement, parcels]):
            raise ValueError("Champs extraits du CERFA insuffisants (commune/dep/parcelles).")

        # 2) Intersections
        log.info("▶️ Étape 2/6 : Intersections parcelles…")
        inters = run_intersections(
            commune=commune,
            departement=departement,
            parcels=parcels,
            csv=insee_csv_path,
            mapping=mapping_path,
            out_json=str(inters_json_path),
            schema_whitelist=["public"],
            values_limit=100,
            carve_enclaves=True,
            enclave_buffer_m=120.0,
        )
        (temp_dir / "intersections.pretty.json").write_text(
            json.dumps(inters, ensure_ascii=False, indent=2), encoding="utf-8"
        )

        # 3) DOCX
        log.info("▶️ Étape 3/6 : Génération du DOCX…")
        build_cua_docx(
            cerfa_json=cerfa,
            intersections_json=inters,
            output_docx=str(docx_path),
        )

        # 4) Carte
        log.info("▶️ Étape 4/6 : Génération carte HTML…")
        if refs:
            eng = _get_db_engine()
            if eng:
                first = refs[0]
                bbox_map.build_map_html_bbox(
                    eng=eng,
                    insee=meta.get("commune_insee") or "",
                    section=first.get("section", ""),
                    numero4=str(first.get("numero", "")).zfill(4),
                    mapping_path=mapping_path,
                    out_html=str(map_path),
                )

        # 5) Upload
        log.info("▶️ Étape 5/6 : Upload artefacts…")
        report_url = _upload_or_none(docx_path)
        map_url = _upload_or_none(map_path)

        # --- Détermination du user_id ---
        if access_token:
            user_id_final = _extract_user_id_from_jwt(access_token)
        else:
            user_id_final = user_id or str(uuid.uuid4())

        # 6) DB
        log.info("▶️ Étape 6/6 : Écriture DB…")
        engine = _get_db_engine()
        job_id = None
        if engine:
            try:
                with engine.begin() as con:
                    row = con.execute(sql_text("""
                        INSERT INTO public.cua_jobs (user_id, status, report_docx_path, map_html_path, result_json)
                        VALUES (:user_id, 'success', :docx, :html, :res)
                        RETURNING id
                    """), {
                        "user_id": user_id_final,
                        "docx": report_url,
                        "html": map_url,
                        "res": json.dumps(inters),
                    }).first()
                    job_id = row[0] if row else None
            except Exception as e:
                log.error(f"Échec enregistrement DB : {e}")

        result = {
            "success": True,
            "job_id": job_id,
            "report_docx_url": report_url,
            "map_html_url": map_url,
            "result_json": inters,
        }

        _maybe_send_notification(result, notify_emails)
        log.info("✅ Pipeline PDF → CUA terminé.")
        return result

    except Exception as e:
        log.exception("❌ Erreur pipeline PDF → CUA")
        return {"success": False, "error": str(e)}


def run_parcel_pipeline(
    parcels: str,
    insee: str,
    commune: str,
    temp_dir: Path,
    insee_csv_path: str,
    mapping_path: str,
    user_id: Optional[str] = None,
    access_token: Optional[str] = None,
    notify_emails: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Pipeline « Parcelles directes → CUA » :
    1) Intersections
    2) DOCX (avec CERFA mock)
    3) Carte HTML
    4) Upload
    5) (optionnel) Écriture DB + notification
    """
    temp_dir.mkdir(parents=True, exist_ok=True)
    inters_json_path = temp_dir / "intersections.json"
    docx_path = temp_dir / "CUA_final_direct.docx"
    map_path = temp_dir / "map_direct.html"

    try:
        # 1) Intersections
        log.info("▶️ Étape 1/5 : Intersections parcelles…")
        inters = run_intersections(
            commune=commune,
            departement=insee[:2],
            parcels=parcels,
            csv=insee_csv_path,
            mapping=mapping_path,
            out_json=str(inters_json_path),
            schema_whitelist=["public"],
            values_limit=100,
            carve_enclaves=True,
            enclave_buffer_m=120.0,
        )

        # 2) DOCX
        log.info("▶️ Étape 2/5 : Génération DOCX…")
        refs = []
        for raw in parcels.split(","):
            p = raw.strip().split()
            if len(p) == 2:
                refs.append({"section": p[0].upper(), "numero": p[1]})
        cerfa_mock = {
            "success": True,
            "data": {
                "commune_nom": commune,
                "commune_insee": insee,
                "departement_code": insee[:2],
                "references_cadastrales": refs,
            }
        }
        build_cua_docx(
            cerfa_json=cerfa_mock,
            intersections_json=inters,
            output_docx=str(docx_path),
        )

        # 3) Carte
        log.info("▶️ Étape 3/5 : Génération carte HTML…")
        eng = _get_db_engine()
        if eng and refs:
            first = refs[0]
            bbox_map.build_map_html_bbox(
                eng=eng,
                insee=insee,
                section=first["section"],
                numero4=str(first["numero"]).zfill(4),
                mapping_path=mapping_path,
                out_html=str(map_path),
            )

        # 4) Upload
        log.info("▶️ Étape 4/5 : Upload artefacts…")
        report_url = _upload_or_none(docx_path)
        map_url = _upload_or_none(map_path)

        # --- Détermination du user_id ---
        if access_token:
            user_id_final = _extract_user_id_from_jwt(access_token)
        else:
            user_id_final = user_id or str(uuid.uuid4())

        # 5) DB
        log.info("▶️ Étape 5/5 : Écriture DB…")
        engine = _get_db_engine()
        job_id = None
        if engine:
            try:
                with engine.begin() as con:
                    row = con.execute(sql_text("""
                        INSERT INTO public.cua_jobs (user_id, status, report_docx_path, map_html_path, result_json)
                        VALUES (:user_id, 'success', :docx, :html, :res)
                        RETURNING id
                    """), {
                        "user_id": user_id_final,
                        "docx": report_url,
                        "html": map_url,
                        "res": json.dumps(inters),
                    }).first()
                    job_id = row[0] if row else None
            except Exception as e:
                log.error(f"Échec enregistrement DB : {e}")

        result = {
            "success": True,
            "job_id": job_id,
            "report_docx_url": report_url,
            "map_html_url": map_url,
            "result_json": inters,
        }

        _maybe_send_notification(result, notify_emails)
        log.info("✅ Pipeline Parcelles → CUA terminé.")
        return result

    except Exception as e:
        log.exception("❌ Erreur pipeline Parcelles → CUA")
        return {"success": False, "error": str(e)}


# ------------------------ CLI ------------------------

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Pipelines CUA")
    sub = ap.add_subparsers(dest="mode", required=True)

    p_pdf = sub.add_parser("from-pdf")
    p_pdf.add_argument("pdf")
    p_pdf.add_argument("--insee-csv", required=True)
    p_pdf.add_argument("--mapping", required=True)
    p_pdf.add_argument("--out-dir", type=Path, default=Path("./cua_output"))
    p_pdf.add_argument("--user-id")
    p_pdf.add_argument("--access-token")
    p_pdf.add_argument("--notify-emails")

    p_par = sub.add_parser("from-parcel")
    p_par.add_argument("--parcel", required=True)
    p_par.add_argument("--insee", required=True)
    p_par.add_argument("--commune", required=True)
    p_par.add_argument("--insee-csv", required=True)
    p_par.add_argument("--mapping", required=True)
    p_par.add_argument("--out-dir", type=Path, default=Path("./cua_output"))
    p_par.add_argument("--user-id")
    p_par.add_argument("--access-token")
    p_par.add_argument("--notify-emails")

    args = ap.parse_args()
    notify = [e.strip() for e in (args.notify_emails or "").split(",") if e and e.strip()]

    args.out_dir.mkdir(parents=True, exist_ok=True)

    if args.mode == "from-pdf":
        r = run_full_pipeline(
            pdf_path=args.pdf,
            temp_dir=args.out_dir,
            insee_csv_path=args.insee_csv,
            mapping_path=args.mapping,
            user_id=args.user_id,
            access_token=args.access_token,
            notify_emails=notify or None,
        )
    else:
        r = run_parcel_pipeline(
            parcels=args.parcel,
            insee=args.insee,
            commune=args.commune,
            temp_dir=args.out_dir,
            insee_csv_path=args.insee_csv,
            mapping_path=args.mapping,
            user_id=args.user_id,
            access_token=args.access_token,
            notify_emails=notify or None,
        )

    print(json.dumps(r, ensure_ascii=False, indent=2))
