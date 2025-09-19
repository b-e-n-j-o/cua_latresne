# -*- coding: utf-8 -*-
"""
cerfa_meta_judge.py — Module de validation de la qualité des métadonnées CERFA
Utilise GPT-5 pour évaluer la complétude et la cohérence des données extraites
"""

import json
import re
import sys
from pathlib import Path
from typing import Dict, Any

# Configuration des chemins d'import
import path_setup  # Configure automatiquement le Python path

from UTILS.llm_utils import call_gpt5_text

JUDGE_SCHEMA = {
    "pass": None,              # true/false
    "reasons": [],             # pourquoi ça échoue (si false)
    "autofixes": {             # corrections sûres (optionnel)
        "commune_insee": None,   # ex si fourni par CSV externe
        "date_depot": None,
        "numero_cu": None
    },
    "must_rerun": None         # true si on recommande de relancer la vision
}

def judge_meta(meta: dict) -> dict:
    """
    Juge la qualité des métadonnées CERFA extraites.
    
    Args:
        meta: Dictionnaire contenant les métadonnées CERFA
        
    Returns:
        Dict avec pass, reasons, autofixes, must_rerun
    """
    # Règles minimales
    checks = {
        "has_commune": bool(meta.get("commune_nom")),
        "has_insee": bool(meta.get("commune_insee")) and len(str(meta.get("commune_insee"))) == 5,
        "has_parcels": bool(meta.get("references_cadastrales")),
        "has_type": bool(meta.get("type_cu")),
        "siret_ok": not meta.get("_quality_flags", {}).get("siret_invalid", False)
    }
    baseline_fail = not (checks["has_commune"] and checks["has_parcels"] and checks["has_type"])

    # LLM: cohérence douce (formatage, libellés…)
    prompt = (
        "Analyse ce JSON de meta CERFA et dis si le dossier est exploitable pour la suite (PASS) ou non.\n"
        "Renvoie STRICTEMENT du JSON au format suivant:\n" + json.dumps(JUDGE_SCHEMA, ensure_ascii=False, indent=2) +
        "\nCRITÈRES: commune/INSEE cohérents; au moins une parcelle; type CU; date cohérente (facultatif); SIRET valide si présent.\n"
        "Veille au numéro INSEE il est important qu'il soit correct car il est utilisé dans la suite du pipeline, si selon toi il n'est pas coherent avec le nom de commune alors marque le comme incorrect"
        "DONNÉES:\n" + json.dumps(meta, ensure_ascii=False)
    )
    res = call_gpt5_text(prompt, reasoning_effort="low", verbosity="low")
    if res.get("success"):
        try:
            j = json.loads(res.get("response",""))
            # sécurité: si baseline_fail, force pass=false
            if baseline_fail:
                j["pass"] = False
                j["must_rerun"] = True
            return j
        except Exception:
            pass
    return {"pass": (not baseline_fail), "reasons": [], "autofixes": {}, "must_rerun": baseline_fail}

if __name__ == "__main__":
    # Test avec un exemple
    test_meta = {
        "commune_nom": "Latresne",
        "commune_insee": "33234",
        "references_cadastrales": [{"section": "AC", "numero": "0496"}],
        "type_cu": "information",
        "_quality_flags": {}
    }
    
    result = judge_meta(test_meta)
    print("Résultat du jugement:")
    print(json.dumps(result, ensure_ascii=False, indent=2))
