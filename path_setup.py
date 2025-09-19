#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
path_setup.py - Utilitaire pour configurer les imports entre dossiers
À importer en haut de chaque fichier qui a besoin d'accéder aux autres modules
"""

import sys
from pathlib import Path

def setup_paths():
    """Configure le Python path pour permettre les imports entre dossiers"""
    current_file = Path(__file__).resolve()
    
    # Trouver le répertoire INTERSECTION_APP (celui qui contient ce fichier)
    intersection_app_dir = current_file.parent
    
    # Ajouter au Python path si pas déjà présent
    if str(intersection_app_dir) not in sys.path:
        sys.path.insert(0, str(intersection_app_dir))
    
    return intersection_app_dir

# Auto-configuration quand le module est importé
setup_paths()
