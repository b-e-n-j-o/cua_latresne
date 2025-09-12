#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_plu_regulation.py — Récupère le règlement PLU pour une zone demandée en testant
une liste de codes candidats (sans introspection 'distinct' de la base).

Usage CLI (optionnel):
    python3 fetch_plu_regulation.py "1AU"
    python3 fetch_plu_regulation.py "AUc"
    python3 fetch_plu_regulation.py "N"

Depuis un autre script:
    from fetch_plu_regulation import fetch_plu_regulation
    txt = fetch_plu_regulation("AUc")  # tentera 1AU, AU, 2AU, 3AU…
"""

import os
import sys
import re
from typing import List, Optional
import dotenv

dotenv.load_dotenv()

def _cand_for_zone(z: str) -> List[str]:
    """
    Génère une liste courte et déterministe de codes candidats pour 'z'.

    Exemples:
      "AUc" -> ["1AU", "AU", "2AU", "3AU"]
      "AUb" -> ["1AU", "AU", "2AU", "3AU"]
      "1AU" -> ["1AU", "AU"]
      "2AU" -> ["2AU", "AU", "1AU"]
      "UA"  -> ["UA"]
      "N"   -> ["N"]
    """
    z = (z or "").strip().upper()
    if not z:
        return []

    # AUc / AUb / AUa / AU* => mappe vers AU/1AU
    if z.startswith("AU") and len(z) >= 2:
        return ["1AU", "AU", "2AU", "3AU"]

    # 1AU / 2AU / 3AU…
    m = re.match(r"^(\d+)AU$", z)
    if m:
        n = m.group(1)
        if n == "1":
            return ["1AU", "AU"]
        elif n == "2":
            return ["2AU", "AU", "1AU"]
        else:
            return [f"{n}AU", "AU", "1AU"]

    # UA, UB, UC… ou N, A, etc. -> direct
    return [z]

def fetch_plu_regulation(zone_name: str, table: str = "plu_chunks", column: str = "zonage") -> str:
    """
    Concatène 'article_title' + 'content' pour la première variante de zone qui existe vraiment.
    Ne tente PAS de découvrir les zonages existants (évite distinct / not_.is_).
    """
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_SERVICE_KEY")
    if not supabase_url or not supabase_key:
        return "❌ Erreur: SUPABASE_URL et SUPABASE_KEY/SUPABASE_SERVICE_KEY requis dans .env"

    try:
        from supabase import create_client
    except Exception:
        return "❌ Erreur: Module 'supabase' non installé. Installer avec: pip install supabase"

    client = create_client(supabase_url, supabase_key)
    print("🔌 Supabase OK")

    cands = _cand_for_zone(zone_name)
    if not cands:
        return "ℹ️ Code de zone vide"

    for cand in cands:
        print(f"📇 Tentative sur '{cand}' …")
        try:
            # NB: pas d'arg desc=… (certains bindings n'aiment pas),
            #     pas d'espaces dans la liste des colonnes.
            resp = (
                client.table(table)
                .select("id,article_title,content")
                .eq(column, cand)
                .order("id")
                .execute()
            )
        except TypeError as e:
            # Cas typique: "SyncRequestBuilder.select() got an unexpected ..."
            return f"❌ Erreur client Supabase (version API?): {e}"
        except Exception as e:
            return f"❌ Erreur lors de la récupération: {e}"

        rows = resp.data or []
        if rows:
            print(f"✅ {len(rows)} article(s) trouvé(s) pour '{cand}'")
            parts: List[str] = []
            for r in rows:
                title = (r.get("article_title") or "").strip()
                content = (r.get("content") or "").strip()
                if title and content:
                    parts.append(f"=== {title} ===\n{content}")
                elif title:
                    parts.append(f"=== {title} ===")
                elif content:
                    parts.append(content)
            txt = "\n\n".join(parts)
            print(f"📝 Règlement assemblé (len={len(txt)})")
            return txt
        else:
            print(f"— aucun article pour '{cand}', on essaie le suivant…")

    return f"ℹ️ Aucun article trouvé pour {zone_name} (candidats testés: {cands})"

def main():
    if len(sys.argv) != 2:
        print("Usage: python3 fetch_plu_regulation.py '1AU' | 'AUc' | 'N'")
        sys.exit(1)
    zone = sys.argv[1].strip()
    print("=" * 60)
    print(f"🎯 Récupération du règlement PLU pour: '{zone}'")
    print("=" * 60)
    txt = fetch_plu_regulation(zone)
    print("\n" + "=" * 60)
    print("📋 RÈGLEMENT PLU RÉCUPÉRÉ")
    print("=" * 60)
    print(txt)

if __name__ == "__main__":
    main()
