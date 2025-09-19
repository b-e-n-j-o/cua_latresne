# -*- coding: utf-8 -*-
"""
fetch_plu_regulation.py - Version optimisée
Récupération des règlements PLU (table nettoyée 'plu_regulations_clean').
- Récupère directement le texte complet et propre pour un zonage donné.
- Ne dépend plus de l'API LLM pour le nettoyage à la volée.

Dépendances:
  - supabase
  - python-dotenv (si vous utilisez un .env)
"""

from __future__ import annotations
import os
import re
from typing import Dict, List, Tuple, Optional
from supabase import create_client, Client

# Load .env file (optional)
try:
    import dotenv
    dotenv.load_dotenv()
except ImportError:
    pass

# --- Supabase Client ---
def _get_supabase_client() -> Client:
    """Creates and returns a Supabase client."""
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_SERVICE_KEY")
    if not supabase_url or not supabase_key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_KEY/SUPABASE_SERVICE_KEY are required.")
    try:
        from supabase import create_client
    except Exception as e:
        raise RuntimeError("Module 'supabase' not installed. pip install supabase") from e
    return create_client(supabase_url, supabase_key)

# --- Normalization (kept for robustness) ---
def canonicalize_zone(z: str | None) -> str:
    """
    Normalizes a zone code:
      - upper
      - removes all non-alphanumeric characters (spaces, periods, hyphens)
    """
    if not z:
        return ""
    z = str(z).upper()
    z = re.sub(r"[^0-9A-Z]+", "", z)
    return z

def candidate_zones(z: str | None) -> List[str]:
    """
    Generates deterministic candidates for searching.
    This is less critical now that your data is clean, but useful for robustness.
    """
    canon = canonicalize_zone(z)
    if not canon:
        return []
    # Simplified logic since your new table should contain the canonical name.
    return [canon]

# --- Main Fetching Function ---
def fetch_plu_regulation_for_zone(
    zone_name: str,
    table: str = "plu_regulations_clean",
    *,
    debug: bool = False
) -> Tuple[str, Optional[str]]:
    """
    Fetches the regulation text for a given zone from the cleaned table.
    Returns (regulation_text, effective_zone_code) or ("", None) if not found.
    """
    client = _get_supabase_client()
    cands = candidate_zones(zone_name)
    if debug:
        print(f"🔎 Requested zone='{zone_name}' → searching for: {cands}")
    if not cands:
        return ("", None)

    for cand in cands:
        try:
            resp = (
                client.table(table)
                .select("regulation_text")
                .eq("zonage", cand)
                .single()
                .execute()
            )
            text = resp.data.get("regulation_text")
            if text:
                if debug:
                    print(f"✅ Regulation found for '{cand}'.")
                return (text, cand)
        except Exception as e:
            if debug:
                print(f"⚠️ Supabase error for candidate='{cand}': {e}")
            # Continue to the next candidate if there's an error (e.g., row not found)
            continue
    
    if debug:
        print(f"❌ No regulation found for any candidate.")
    return ("", None)

def fetch_plu_regulations_for_zones(
    zones: List[str],
    table: str = "plu_regulations_clean",
    *,
    debug: bool = False
) -> Dict[str, str]:
    """
    For a list of zone codes, returns a dictionary {canonical_zone: regulation_text}.
    """
    out: Dict[str, str] = {}
    seen_effective: set[str] = set()

    for z in zones:
        txt, effective = fetch_plu_regulation_for_zone(
            z, table=table, debug=debug
        )
        if txt and effective and effective not in seen_effective:
            out[effective] = txt
            seen_effective.add(effective)
    return out

def join_regulations_for_docx(zones_to_text: Dict[str, str], pct_by_zone: Dict[str, float]) -> str:
    """
    Concatenates regulation texts for DOCX insertion, including percentage if available.
    This function remains unchanged and works with the output of the new fetching functions.
    """
    if not zones_to_text:
        return ""
    blocks: List[str] = []
    for code in sorted(zones_to_text.keys()):
        pct = pct_by_zone.get(code)
        head = f"Zone {code}"
        blocks.append(head + "\n\n" + zones_to_text[code].strip())
    return "\n\n\n".join(blocks)

# --- CLI for Testing ---
if __name__ == "__main__":
    import argparse, sys
    ap = argparse.ArgumentParser(description="Test fetching PLU regulations from a pre-cleaned table.")
    ap.add_argument("--zone", help="Zone code (e.g., UA, N, 1AU)")
    ap.add_argument("--zones", help="Comma-separated list of zones (e.g., UA,N,1AU)")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    zs: List[str] = []
    if args.zone:
        zs = [args.zone]
    elif args.zones:
        zs = [z.strip() for z in args.zones.split(",") if z.strip()]
    else:
        print("⚠️ Please specify --zone UA or --zones 'UA,N,1AU'")
        sys.exit(1)

    print(f"🎯 Input zones: {zs}")
    regulations_map = fetch_plu_regulations_for_zones(zs, debug=args.debug)
    
    if not regulations_map:
        print("No regulations found.")
        sys.exit(0)

    print("\n================= OUTPUT =================\n")
    print(join_regulations_for_docx(regulations_map, pct_by_zone={}))