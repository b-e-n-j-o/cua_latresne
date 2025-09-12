# plu_regulation_context.py
from typing import Dict, List
from fetch_plu_regulation import fetch_plu_regulation

def infer_zone_codes(libelle: str|None, typezone: str|None) -> List[str]:
    z = (typezone or libelle or "").strip().upper()
    if not z:
        return []
    if z.startswith("AU"):
        return ["1AU","AU","2AU","3AU"]
    return [z]

def get_regulation_for_cumodel_zones(cumodel: Dict) -> Dict[str, str]:
    """
    Retourne { "N": "texte…", "UA": "texte…", ... } pour les zones trouvées.
    """
    out: Dict[str, str] = {}
    zones = (cumodel.get("plu") or {}).get("zones") or []
    for z in zones:
        libelle = (z.get("libelle") or "").strip()
        typezone = (z.get("typezone") or "").strip()
        for code in infer_zone_codes(libelle, typezone):
            txt = fetch_plu_regulation(code, table="plu_chunks", column="zonage")
            if txt and not txt.startswith(("❌","ℹ️")):
                out[code] = txt
                break
    return out

def join_regulations_for_docx(zones_to_text: Dict[str,str], pct_by_zone: Dict[str,float]) -> str:
    parts = []
    for code, txt in zones_to_text.items():
        pct = pct_by_zone.get(code)
        head = f"=== {code}{(f' ({pct:.1f} %)' if isinstance(pct,(int,float)) else '')} ==="
        parts.append(head + "\n" + txt.strip())
    return "\n\n".join(parts)
