#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de test pour vérifier l'intégration du juge dans le pipeline Gemini
"""

import json
from pathlib import Path
from cerfa_meta_judge import judge_meta

def test_judge_scenarios():
    """Test différents scénarios de validation"""
    
    print("=== Test du juge CERFA ===\n")
    
    # Scénario 1: Données valides
    print("1. Test avec données valides:")
    valid_meta = {
        "commune_nom": "Latresne",
        "commune_insee": "33234",
        "references_cadastrales": [{"section": "AC", "numero": "0496"}],
        "type_cu": "information",
        "date_depot": "2024-07-19",
        "_quality_flags": {}
    }
    
    result = judge_meta(valid_meta)
    print(f"   Pass: {result.get('pass')}")
    print(f"   Reasons: {result.get('reasons', [])}")
    print(f"   Must rerun: {result.get('must_rerun')}")
    print()
    
    # Scénario 2: Données manquantes (commune)
    print("2. Test avec commune manquante:")
    invalid_meta = {
        "commune_nom": None,
        "commune_insee": "33234",
        "references_cadastrales": [{"section": "AC", "numero": "0496"}],
        "type_cu": "information"
    }
    
    result = judge_meta(invalid_meta)
    print(f"   Pass: {result.get('pass')}")
    print(f"   Reasons: {result.get('reasons', [])}")
    print(f"   Must rerun: {result.get('must_rerun')}")
    print()
    
    # Scénario 3: INSEE incohérent
    print("3. Test avec INSEE incohérent:")
    incoherent_meta = {
        "commune_nom": "Latresne",
        "commune_insee": "75001",  # Paris au lieu de Latresne
        "references_cadastrales": [{"section": "AC", "numero": "0496"}],
        "type_cu": "information"
    }
    
    result = judge_meta(incoherent_meta)
    print(f"   Pass: {result.get('pass')}")
    print(f"   Reasons: {result.get('reasons', [])}")
    print(f"   Must rerun: {result.get('must_rerun')}")
    print()
    
    # Scénario 4: Parcelles manquantes
    print("4. Test avec parcelles manquantes:")
    no_parcels_meta = {
        "commune_nom": "Latresne",
        "commune_insee": "33234",
        "references_cadastrales": [],
        "type_cu": "information"
    }
    
    result = judge_meta(no_parcels_meta)
    print(f"   Pass: {result.get('pass')}")
    print(f"   Reasons: {result.get('reasons', [])}")
    print(f"   Must rerun: {result.get('must_rerun')}")
    print()

def test_pipeline_integration():
    """Test d'intégration avec le pipeline (simulation)"""
    print("=== Test d'intégration pipeline ===\n")
    
    # Simulation d'un résultat Gemini qui échouerait à la validation
    gemini_result = {
        "success": True,
        "data": {
            "commune_nom": "Latresne",
            "commune_insee": None,  # Manquant
            "references_cadastrales": [],  # Vide
            "type_cu": None  # Manquant
        }
    }
    
    print("Données Gemini simulées:")
    print(json.dumps(gemini_result["data"], ensure_ascii=False, indent=2))
    print()
    
    judge_result = judge_meta(gemini_result["data"])
    print("Résultat du juge:")
    print(json.dumps(judge_result, ensure_ascii=False, indent=2))
    print()
    
    if not judge_result.get("pass"):
        print("❌ Le juge recommande de relancer l'extraction")
        if judge_result.get("must_rerun"):
            print("🔄 Relancement automatique recommandé")
    else:
        print("✅ Données validées par le juge")

if __name__ == "__main__":
    test_judge_scenarios()
    test_pipeline_integration()
