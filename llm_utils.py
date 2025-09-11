"""
llm_utils.py — Utilitaires pour l'appel des LLMs OpenAI
"""

import os
import json
import logging
import base64
from typing import Dict, Any
from openai import OpenAI

logger = logging.getLogger(__name__)

import dotenv
dotenv.load_dotenv()

# Configuration OpenAI
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def extract_text_from_response(response) -> str:
    """
    Extraction robuste du texte depuis la réponse OpenAI.
    Couvre tous les formats possibles du SDK 'responses'.
    """
    
    # 1) helper universel du SDK responses (s'il existe)
    if hasattr(response, "output_text") and response.output_text:
        logger.info("✅ Texte extrait via response.output_text")
        return response.output_text

    # 2) format "responses": output -> content -> text.value
    try:
        parts = []
        for item in getattr(response, "output", []) or []:
            for c in getattr(item, "content", []) or []:
                # certains SDK mettent text.value, d'autres text
                if hasattr(c, "text") and c.text:
                    val = getattr(c.text, "value", None) or c.text
                    if isinstance(val, str):
                        parts.append(val)
        if parts:
            logger.info(f"✅ Texte extrait via response.output[].content[].text ({len(parts)} parties)")
            return "".join(parts)
    except Exception as e:
        logger.debug(f"Tentative d'extraction via output[] échouée: {e}")

    # 3) fallback pour anciens formats (chat.completions-like)
    if hasattr(response, "choices"):
        try:
            text = response.choices[0].message.content
            if text:
                logger.info("✅ Texte extrait via response.choices[0].message.content")
                return text
        except Exception as e:
            logger.debug(f"Tentative d'extraction via choices échouée: {e}")

    # 4) dernier recours : essayer d'accéder directement aux attributs
    for attr in ["content", "text", "message", "response"]:
        try:
            if hasattr(response, attr):
                val = getattr(response, attr)
                if isinstance(val, str) and val.strip():
                    logger.info(f"✅ Texte extrait via response.{attr}")
                    return val.strip()
        except Exception:
            continue

    logger.error("❌ Aucune méthode d'extraction n'a fonctionné")
    return ""

def call_gpt5_text(prompt: str, reasoning_effort: str = "medium", verbosity: str = "medium") -> Dict[str, Any]:
    """
    Appel de l'API GPT-5 via le format 'responses'.
    Retourne un dict avec success, response, et error si applicable.
    """
    
    logger.info(f"📝 Appel API GPT-5 - Texte uniquement (reasoning: {reasoning_effort}, verbosity: {verbosity})")
    
    try:
        # Vérification de la clé API
        if not os.getenv("OPENAI_API_KEY"):
            return {
                "success": False, 
                "error": "OPENAI_API_KEY manquant dans l'environnement"
            }
        
        # Appel API avec format 'responses'
        response = client.responses.create(
            model="gpt-5",
            input=[{"role": "user", "content": prompt}],   # ✅ format messages
            reasoning={"effort": reasoning_effort},
            text={"verbosity": verbosity}
        )
        
        # Extraction du texte
        output_text = extract_text_from_response(response)
        
        if not output_text:
            return {
                "success": False,
                "error": "Réponse reçue mais texte non extractible",
                "raw_response": str(response)
            }
        
        logger.info(f"✅ Réponse reçue ({len(output_text)} caractères)")
        
        # Affichage des tokens si disponibles
        if hasattr(response, 'usage') and response.usage:
            usage = response.usage
            logger.info(f"💰 Tokens utilisés:")
            logger.info(f"  - Input tokens: {getattr(usage, 'input_tokens', 'N/A')}")
            if hasattr(usage, 'input_tokens_details') and usage.input_tokens_details:
                logger.info(f"    - Cached tokens: {getattr(usage.input_tokens_details, 'cached_tokens', 'N/A')}")
            logger.info(f"  - Output tokens: {getattr(usage, 'output_tokens', 'N/A')}")
            if hasattr(usage, 'output_tokens_details') and usage.output_tokens_details:
                logger.info(f"    - Reasoning tokens: {getattr(usage.output_tokens_details, 'reasoning_tokens', 'N/A')}")
            logger.info(f"  - Total tokens: {getattr(usage, 'total_tokens', 'N/A')}")
        
        return {
            "success": True, 
            "response": output_text, 
            "raw_response": response
        }
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'appel API: {e}")
        return {
            "success": False, 
            "error": str(e)
        }

def call_gpt4o_text(prompt: str, max_tokens: int = 20000) -> Dict[str, Any]:
    """
    Fallback vers GPT-4o si GPT-5 n'est pas disponible.
    """
    
    logger.info(f"📝 Appel API GPT-4o - Fallback (max_tokens: {max_tokens})")
    
    try:
        if not os.getenv("OPENAI_API_KEY"):
            return {
                "success": False, 
                "error": "OPENAI_API_KEY manquant dans l'environnement"
            }
        
        # Appel classique chat completions
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=max_tokens,
            temperature=0.1
        )
        
        output_text = response.choices[0].message.content
        
        if not output_text:
            return {
                "success": False,
                "error": "Réponse GPT-4o vide"
            }
        
        logger.info(f"✅ Réponse GPT-4o reçue ({len(output_text)} caractères)")
        
        # Affichage des tokens si disponibles
        if hasattr(response, 'usage') and response.usage:
            usage = response.usage
            logger.info(f"💰 Tokens GPT-4o utilisés:")
            logger.info(f"  - Input tokens: {getattr(usage, 'prompt_tokens', 'N/A')}")
            logger.info(f"  - Output tokens: {getattr(usage, 'completion_tokens', 'N/A')}")
            logger.info(f"  - Total tokens: {getattr(usage, 'total_tokens', 'N/A')}")
        
        return {
            "success": True,
            "response": output_text,
            "raw_response": response
        }
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'appel GPT-4o: {e}")
        return {
            "success": False, 
            "error": str(e)
        }

def call_gpt4o_json(prompt: str, temperature: float = 0.1) -> Dict[str, Any]:
    """
    Appel GPT-4o via chat.completions avec forçage de JSON valide.
    Retourne un dict avec success, response (JSON parsé), et error si applicable.
    """
    logger.info("🤖 Appel API GPT-4o (chat.completions) - force JSON")
    
    try:
        # Vérification de la clé API
        if not os.getenv("OPENAI_API_KEY"):
            return {
                "success": False, 
                "error": "OPENAI_API_KEY manquant dans l'environnement"
            }
        
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=temperature
        )
        
        json_text = response.choices[0].message.content
        
        if not json_text:
            return {
                "success": False,
                "error": "Réponse GPT-4o JSON vide"
            }
        
        # Parsing du JSON
        try:
            parsed_json = json.loads(json_text)
            logger.info(f"✅ JSON GPT-4o reçu et parsé ({len(json_text)} caractères)")
            
            # Affichage des tokens si disponibles
            if hasattr(response, 'usage') and response.usage:
                usage = response.usage
                logger.info(f"💰 Tokens GPT-4o JSON utilisés:")
                logger.info(f"  - Input tokens: {getattr(usage, 'prompt_tokens', 'N/A')}")
                logger.info(f"  - Output tokens: {getattr(usage, 'completion_tokens', 'N/A')}")
                logger.info(f"  - Total tokens: {getattr(usage, 'total_tokens', 'N/A')}")
            
            return {
                "success": True,
                "response": parsed_json,  # JSON déjà parsé
                "raw_response": response,
                "json_text": json_text  # Texte JSON brut aussi disponible
            }
        except json.JSONDecodeError as e:
            logger.error(f"❌ JSON GPT-4o invalide: {e}")
            return {
                "success": False,
                "error": f"JSON invalide: {e}",
                "raw_response": response,
                "json_text": json_text
            }
        
    except Exception as e:
        logger.error(f"❌ Erreur GPT-4o JSON: {e}")
        return {
            "success": False, 
            "error": str(e)
        }

def call_gpt5_json(prompt: str, temperature: float = 0.1) -> Dict[str, Any]:
    """
    Appel GPT-5 via chat.completions avec forçage de JSON valide.
    Retourne un dict avec success, response (JSON parsé), et error si applicable.
    """
    logger.info("🤖 Appel API GPT-5 (chat.completions) - force JSON")
    
    try:
        # Vérification de la clé API
        if not os.getenv("OPENAI_API_KEY"):
            return {
                "success": False, 
                "error": "OPENAI_API_KEY manquant dans l'environnement"
            }
        
        response = client.chat.completions.create(
            model="gpt-5",  # ou "gpt-5" selon disponibilité
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"}
        )
        
        json_text = response.choices[0].message.content
        
        if not json_text:
            return {
                "success": False,
                "error": "Réponse GPT-5 JSON vide"
            }
        
        # Parsing du JSON
        try:
            parsed_json = json.loads(json_text)
            logger.info(f"✅ JSON GPT-5 reçu et parsé ({len(json_text)} caractères)")
            
            # Affichage des tokens si disponibles
            if hasattr(response, 'usage') and response.usage:
                usage = response.usage
                logger.info(f"💰 Tokens GPT-5 JSON utilisés:")
                logger.info(f"  - Input tokens: {getattr(usage, 'prompt_tokens', 'N/A')}")
                logger.info(f"  - Output tokens: {getattr(usage, 'completion_tokens', 'N/A')}")
                logger.info(f"  - Total tokens: {getattr(usage, 'total_tokens', 'N/A')}")
            
            return {
                "success": True,
                "response": parsed_json,  # JSON déjà parsé
                "raw_response": response,
                "json_text": json_text  # Texte JSON brut aussi disponible
            }
        except json.JSONDecodeError as e:
            logger.error(f"❌ JSON GPT-5 invalide: {e}")
            return {
                "success": False,
                "error": f"JSON invalide: {e}",
                "raw_response": response,
                "json_text": json_text
            }
        
    except Exception as e:
        logger.error(f"❌ Erreur GPT-5 JSON: {e}")
        return {
            "success": False, 
            "error": str(e)
        }

def call_gpt4o_vision(image_bytes: bytes, prompt: str = "Extrait le texte brut lisible de ce document. Ne renvoie que le texte, sans ajout.", temperature: float = 0.0) -> Dict[str, Any]:
    """
    Appel API GPT-4o avec une image (vision) pour extraire du texte.

    Args:
        image_bytes (bytes): Contenu binaire de l'image (PNG/JPEG)
        prompt (str): Instruction utilisateur
        temperature (float): Créativité

    Returns:
        Dict: {success, response (texte), raw_response}
    """
    logger.info(f"🖼️ Appel API GPT-4o Vision (temperature: {temperature})")

    try:
        # Vérification de la clé API
        if not os.getenv("OPENAI_API_KEY"):
            return {
                "success": False, 
                "error": "OPENAI_API_KEY manquant dans l'environnement"
            }

        b64 = base64.b64encode(image_bytes).decode("ascii")
        data_url = f"data:image/png;base64,{b64}"

        api_params = {
            "model": "gpt-4o",
            "input": [
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": prompt},
                        {"type": "input_image", "image_url": data_url},
                    ],
                }
            ],
            "temperature": temperature,
        }

        response = client.responses.create(**api_params)
        output_text = extract_text_from_response(response)
        logger.info("✅ Réponse vision reçue avec succès")
        
        # Affichage des tokens si disponibles
        if hasattr(response, 'usage') and response.usage:
            usage = response.usage
            logger.info(f"💰 Tokens GPT-4o Vision utilisés:")
            logger.info(f"  - Input tokens: {getattr(usage, 'input_tokens', 'N/A')}")
            if hasattr(usage, 'input_tokens_details') and usage.input_tokens_details:
                logger.info(f"    - Cached tokens: {getattr(usage.input_tokens_details, 'cached_tokens', 'N/A')}")
            logger.info(f"  - Output tokens: {getattr(usage, 'output_tokens', 'N/A')}")
            if hasattr(usage, 'output_tokens_details') and usage.output_tokens_details:
                logger.info(f"    - Reasoning tokens: {getattr(usage.output_tokens_details, 'reasoning_tokens', 'N/A')}")
            logger.info(f"  - Total tokens: {getattr(usage, 'total_tokens', 'N/A')}")
        
        return {"success": True, "response": output_text, "raw_response": response}

    except Exception as e:
        logger.error(f"❌ Erreur vision: {e}")
        return {"success": False, "error": str(e)}

# Test rapide si exécuté directement
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Test de la configuration
    api_key = os.getenv("OPENAI_API_KEY")
    if api_key:
        logger.info(f"✅ OPENAI_API_KEY configuré ({len(api_key)} caractères)")
    else:
        logger.warning("⚠️ OPENAI_API_KEY non configuré")
    
    # Test d'appel simple
    test_prompt = "Dis-moi bonjour en une phrase."
    logger.info("🧪 Test d'appel API...")
    
    result = call_gpt5_text(test_prompt)
    if result["success"]:
        logger.info(f"✅ Test réussi: {result['response'][:50]}...")
    else:
        logger.warning(f"⚠️ Test GPT-5 échoué: {result['error']}")
        # Fallback GPT-4o
        result_fallback = call_gpt4o_text(test_prompt)
        if result_fallback["success"]:
            logger.info(f"✅ Fallback GPT-4o réussi: {result_fallback['response'][:50]}...")
        else:
            logger.error(f"❌ Fallback GPT-4o aussi échoué: {result_fallback['error']}")
    
    # Test des nouvelles fonctions JSON
    logger.info("🧪 Test des fonctions JSON...")
    
    json_prompt = 'Crée un objet JSON avec les clés "nom" et "age". Exemple: {"nom": "Jean", "age": 30}'
    
    # Test GPT-4o JSON
    result_json_4o = call_gpt4o_json(json_prompt)
    if result_json_4o["success"]:
        logger.info(f"✅ GPT-4o JSON réussi: {result_json_4o['response']}")
    else:
        logger.warning(f"⚠️ GPT-4o JSON échoué: {result_json_4o['error']}")
    
    # Test GPT-5 JSON
    result_json_5 = call_gpt5_json(json_prompt)
    if result_json_5["success"]:
        logger.info(f"✅ GPT-5 JSON réussi: {result_json_5['response']}")
    else:
        logger.warning(f"⚠️ GPT-5 JSON échoué: {result_json_5['error']}")
    
    # Test GPT-4o Vision (simulation avec bytes vides)
    logger.info("🧪 Test GPT-4o Vision...")
    try:
        # Simulation d'une image (bytes vides pour le test)
        test_image = b"fake_image_data"
        result_vision = call_gpt4o_vision(test_image, "Test de vision")
        if result_vision["success"]:
            logger.info(f"✅ GPT-4o Vision réussi: {result_vision['response'][:50]}...")
        else:
            logger.warning(f"⚠️ GPT-4o Vision échoué: {result_vision['error']}")
    except Exception as e:
        logger.warning(f"⚠️ Test Vision échoué (normal sans vraie image): {e}")
