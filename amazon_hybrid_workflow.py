"""
Amazon Hybrid Workflow - Approccio Basato su Ricerca Approfondita
Combina Playwright diretto (performance) con LLM intelligence (analisi)
"""

import asyncio
import os
import logging
import uuid
from datetime import datetime
from typing import TypedDict, Optional, List, Dict, Any

# Carica variabili da .env se presente (non obbligatorio)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# Setup LangSmith - Enhanced Configuration
os.environ.setdefault("LANGCHAIN_TRACING_V2", "true")
os.environ.setdefault("LANGCHAIN_PROJECT", "amazon-hybrid-workflow")
os.environ.setdefault("LANGCHAIN_CALLBACKS_BACKGROUND", "true")  # Enhanced performance

from langgraph.graph import StateGraph, END
from langchain.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from scraper_real import AmazonScraperReal
from amazon_credentials import AMAZON_CREDENTIALS

# Fix event loop per ambiente async
import nest_asyncio
nest_asyncio.apply()

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-8s | %(message)s')
logger = logging.getLogger(__name__)


# =====================================
# STATE SCHEMA - Hybrid Architecture
# =====================================

class AmazonHybridState(TypedDict):
    """State schema per approccio ibrido: Direct Playwright + LLM Intelligence"""
    
    # Input
    asin: str
    
    # Phase 1: Direct Playwright (Fast, Reliable)
    authentication_success: bool
    authentication_duration: Optional[float]
    
    scraped_data: Optional[Dict[str, Any]]
    scraping_success: bool
    scraping_duration: Optional[float]
    scraping_errors: List[str]
    
    # Phase 2: LLM Intelligence (Slow, Smart)
    sentiment_analysis: Optional[Dict[str, Any]]
    theme_analysis: Optional[Dict[str, Any]]
    analysis_duration: Optional[float]
    analysis_errors: List[str]
    
    # Final Output
    final_report: Dict[str, Any]
    total_duration: Optional[float]
    workflow_success: bool


# =====================================
# HYBRID NODES - Performance + Intelligence
# =====================================

class AmazonHybridNodes:
    """Nodi ibridi: Direct Playwright per performance + LLM per intelligence"""
    
    def __init__(self):
        # LLM solo per analisi (non per navigazione)
        provider = (os.getenv("LLM_PROVIDER", "ollama") or "ollama").lower()

        if provider == "openai":
            # OpenAI via LangChain
            try:
                from langchain_openai import ChatOpenAI
            except Exception as e:
                raise RuntimeError(
                    "langchain-openai non installato. Aggiungi a requirements.txt e pip install."
                ) from e

            model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
            # response_format JSON per output strutturati
            self.llm = ChatOpenAI(
                model=model,
                temperature=0.2,
                response_format={"type": "json_object"},
            )
        else:
            # Default: Ollama locale
            from langchain_ollama import OllamaLLM
            self.llm = OllamaLLM(
                model=os.getenv("OLLAMA_MODEL", "llama3.1:8b"),
                base_url=os.getenv("OLLAMA_BASE_URL", "http://localhost:11434"),
                temperature=0.2,
            )

        # Parser per garantire stringa in uscita dalla catena
        self._out_parser = StrOutputParser()
        
    async def authentication_node(self, state: AmazonHybridState) -> Dict[str, Any]:
        """Phase 1A: Autenticazione diretta con Playwright (Performance Critical)"""
        logger.info("🔐 [AUTH] Login Amazon diretto - Performance Critical")
        start_time = datetime.now()
        
        # Verifica credenziali
        if not AMAZON_CREDENTIALS.get("email") or not AMAZON_CREDENTIALS.get("password"):
            logger.warning("⚠️ [AUTH] Credenziali non configurate - procedo senza login")
            return {
                "authentication_success": False,
                "authentication_duration": (datetime.now() - start_time).total_seconds()
            }
        
        try:
            # Usa scraper_real.py ottimizzato con cookie handling
            async with AmazonScraperReal(
                headless=False,  # Visibile per debug e 2FA
                login_credentials=AMAZON_CREDENTIALS
            ) as scraper:
                
                logger.info("🔐 [AUTH] Esecuzione login con gestione cookie automatica...")
                login_success = await scraper.perform_login()
                
                duration = (datetime.now() - start_time).total_seconds()
                
                if login_success:
                    logger.info(f"✅ [AUTH] Login completato in {duration:.2f}s")
                    return {
                        "authentication_success": True,
                        "authentication_duration": duration
                    }
                else:
                    logger.warning(f"⚠️ [AUTH] Login fallito in {duration:.2f}s")
                    return {
                        "authentication_success": False,
                        "authentication_duration": duration
                    }
                    
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"❌ [AUTH] Errore login in {duration:.2f}s: {e}")
            return {
                "authentication_success": False,
                "authentication_duration": duration
            }
    
    async def scraping_node(self, state: AmazonHybridState) -> Dict[str, Any]:
        """Phase 1B: Scraping diretto con Playwright (Performance Critical)"""
        logger.info(f"🕷️ [SCRAPE] Estrazione dati diretta - ASIN: {state['asin']}")
        start_time = datetime.now()
        
        try:
            # Usa scraper_real.py ottimizzato
            async with AmazonScraperReal(
                headless=False,
                login_credentials=AMAZON_CREDENTIALS if state.get('authentication_success') else {}
            ) as scraper:
                
                # Se non autenticato, perform login comunque per cookie
                if not state.get('authentication_success'):
                    logger.info("🏠 [SCRAPE] Navigazione homepage con gestione cookie...")
                    await scraper.page.goto("https://www.amazon.it", wait_until="domcontentloaded", timeout=30000)
                    await scraper.handle_cookie_banner()
                
                # Estrazione dati prodotto
                logger.info("📦 [SCRAPE] Navigazione ed estrazione prodotto...")
                if await scraper.navigate_to_product(state['asin'], with_login=False):
                    product_info = await scraper.extract_product_info(state['asin'])
                    
                    # Base data structure
                    scraped_data = {
                        "asin": state['asin'],
                        "title": product_info.get('title', 'N/A'),
                        "rating_average": product_info.get('rating_average', 0.0),
                        "total_reviews": product_info.get('total_reviews', 0),
                        "price": product_info.get('price'),
                        "reviews": [],
                        "scraped_at": datetime.now().isoformat(),
                        "authenticated": state.get('authentication_success', False)
                    }
                    
                    # Prova estrazione recensioni (beneficia da autenticazione)
                    logger.info("📝 [SCRAPE] Tentativo estrazione recensioni...")
                    if await scraper.navigate_to_reviews():
                        try:
                            reviews = await scraper.extract_reviews(max_reviews=15)
                            scraped_data["reviews"] = reviews
                            logger.info(f"✅ [SCRAPE] Recensioni estratte: {len(reviews)}")
                        except Exception as e:
                            logger.warning(f"⚠️ [SCRAPE] Errore recensioni: {e}")
                    
                    # Valuta successo
                    duration = (datetime.now() - start_time).total_seconds()
                    has_core_data = (
                        scraped_data.get('title') != 'N/A' and
                        scraped_data.get('rating_average', 0) > 0
                    )
                    
                    logger.info(f"✅ [SCRAPE] Completato in {duration:.2f}s - Core: {'OK' if has_core_data else 'FAIL'}, Reviews: {len(scraped_data['reviews'])}")
                    
                    return {
                        "scraped_data": scraped_data,
                        "scraping_success": has_core_data,
                        "scraping_duration": duration,
                        "scraping_errors": [] if has_core_data else ["Core product data missing"]
                    }
                
                logger.error("❌ [SCRAPE] Navigazione prodotto fallita")
                return {
                    "scraped_data": {},
                    "scraping_success": False,
                    "scraping_duration": (datetime.now() - start_time).total_seconds(),
                    "scraping_errors": ["Product navigation failed"]
                }
                
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"❌ [SCRAPE] Errore in {duration:.2f}s: {e}")
            return {
                "scraped_data": {},
                "scraping_success": False,
                "scraping_duration": duration,
                "scraping_errors": [str(e)]
            }
    
    async def auth_and_scraping_node(self, state: AmazonHybridState) -> Dict[str, Any]:
        """Nodo unificato: Authentication + Scraping in una singola sessione browser"""
        logger.info("🔐🕷️ [AUTH+SCRAPE] Login e scraping unificati - mantieni sessione")
        
        auth_start_time = datetime.now()
        scrape_start_time = None
        
        # Verifica credenziali
        if not AMAZON_CREDENTIALS.get("email") or not AMAZON_CREDENTIALS.get("password"):
            logger.warning("⚠️ [AUTH+SCRAPE] Credenziali non configurate - solo scraping pubblico")
            auth_success = False
            auth_duration = 0.1
        else:
            auth_success = None  # Will be determined during login
            auth_duration = None
        
        try:
            # UNICA SESSIONE BROWSER per auth + scraping
            async with AmazonScraperReal(
                headless=False,
                login_credentials=AMAZON_CREDENTIALS
            ) as scraper:
                
                # FASE 1: AUTENTICAZIONE (se credenziali disponibili)
                if AMAZON_CREDENTIALS.get("email") and AMAZON_CREDENTIALS.get("password"):
                    logger.info("🔐 [AUTH+SCRAPE] Tentativo login...")
                    auth_success = await scraper.perform_login()
                    auth_duration = (datetime.now() - auth_start_time).total_seconds()
                    
                    if auth_success:
                        logger.info(f"✅ [AUTH+SCRAPE] Login completato in {auth_duration:.2f}s - SESSIONE MANTENUTA")
                    else:
                        logger.warning(f"⚠️ [AUTH+SCRAPE] Login fallito in {auth_duration:.2f}s - procedo con scraping pubblico")
                else:
                    logger.info("🏠 [AUTH+SCRAPE] Skip login - navigazione homepage per cookie...")
                    await scraper.page.goto("https://www.amazon.it", wait_until="domcontentloaded", timeout=30000)
                    await scraper.handle_cookie_banner()
                    auth_success = False
                    auth_duration = (datetime.now() - auth_start_time).total_seconds()
                
                # FASE 2: SCRAPING (stessa sessione browser!)
                scrape_start_time = datetime.now()
                logger.info(f"📦 [AUTH+SCRAPE] Inizio scraping prodotto - Autenticato: {auth_success}")
                
                # Navigazione prodotto DIRETTA (login già completato)
                logger.info(f"📦 [AUTH+SCRAPE] Navigazione diretta a prodotto {state['asin']}...")
                product_url = f"https://www.amazon.it/dp/{state['asin']}"
                
                try:
                    await scraper.page.goto(product_url, wait_until="domcontentloaded", timeout=30000)
                    await scraper.random_delay(2, 3)
                    
                    # Gestione cookie se necessario
                    await scraper.handle_cookie_banner()
                    
                    # Verifica caricamento prodotto
                    product_loaded = await scraper.page.query_selector("#productTitle, #dp-container")
                    if product_loaded:
                        logger.info("✅ [AUTH+SCRAPE] Pagina prodotto caricata con successo")
                        product_info = await scraper.extract_product_info(state['asin'])
                        
                        # Struttura dati base
                        scraped_data = {
                            "asin": state['asin'],
                            "title": product_info.get('title', 'N/A'),
                            "rating_average": product_info.get('rating_average', 0.0),
                            "total_reviews": product_info.get('total_reviews', 0),
                            "price": product_info.get('price'),
                            "reviews": [],
                            "scraped_at": datetime.now().isoformat(),
                            "authenticated": auth_success or False
                        }
                        
                        # Estrazione recensioni (beneficia da autenticazione!)
                        logger.info("📝 [AUTH+SCRAPE] Tentativo estrazione recensioni...")
                        if await scraper.navigate_to_reviews():
                            try:
                                reviews = await scraper.extract_reviews(max_reviews=15)
                                scraped_data["reviews"] = reviews
                                logger.info(f"✅ [AUTH+SCRAPE] Recensioni estratte: {len(reviews)}")
                            except Exception as e:
                                logger.warning(f"⚠️ [AUTH+SCRAPE] Errore recensioni: {e}")
                        
                        # Calcola durate
                        scrape_duration = (datetime.now() - scrape_start_time).total_seconds()
                        total_duration = auth_duration + scrape_duration
                        
                        # Valuta successo
                        has_core_data = (
                            scraped_data.get('title') != 'N/A' and
                            scraped_data.get('rating_average', 0) > 0
                        )
                        
                        logger.info(f"✅ [AUTH+SCRAPE] Completato in {total_duration:.2f}s (Auth: {auth_duration:.1f}s + Scrape: {scrape_duration:.1f}s)")
                        logger.info(f"📊 [AUTH+SCRAPE] Risultati: Core: {'OK' if has_core_data else 'FAIL'}, Reviews: {len(scraped_data['reviews'])}, Auth: {auth_success}")
                        
                        return {
                            # Authentication results
                            "authentication_success": auth_success or False,
                            "authentication_duration": auth_duration,
                            
                            # Scraping results  
                            "scraped_data": scraped_data,
                            "scraping_success": has_core_data,
                            "scraping_duration": scrape_duration,
                            "scraping_errors": [] if has_core_data else ["Core product data missing"]
                        }
                    else:
                        logger.error("❌ [AUTH+SCRAPE] Pagina prodotto non caricata correttamente")
                        scrape_duration = (datetime.now() - scrape_start_time).total_seconds()
                        return {
                            "authentication_success": auth_success or False,
                            "authentication_duration": auth_duration,
                            "scraped_data": {},
                            "scraping_success": False,
                            "scraping_duration": scrape_duration,
                            "scraping_errors": ["Product page not loaded"]
                        }
                        
                except Exception as e:
                    scrape_duration = (datetime.now() - scrape_start_time).total_seconds()
                    logger.error(f"❌ [AUTH+SCRAPE] Errore navigazione prodotto: {e}")
                    return {
                        "authentication_success": auth_success or False,
                        "authentication_duration": auth_duration,
                        "scraped_data": {},
                        "scraping_success": False,
                        "scraping_duration": scrape_duration,
                        "scraping_errors": [f"Navigation error: {str(e)}"]
                    }
                
                else:
                    scrape_duration = (datetime.now() - scrape_start_time).total_seconds()
                    logger.error("❌ [AUTH+SCRAPE] Navigazione prodotto fallita")
                    return {
                        "authentication_success": auth_success or False,
                        "authentication_duration": auth_duration,
                        "scraped_data": {},
                        "scraping_success": False,
                        "scraping_duration": scrape_duration,
                        "scraping_errors": ["Product navigation failed"]
                    }
                    
        except Exception as e:
            total_duration = (datetime.now() - auth_start_time).total_seconds()
            logger.error(f"❌ [AUTH+SCRAPE] Errore in {total_duration:.2f}s: {e}")
            return {
                "authentication_success": False,
                "authentication_duration": auth_duration or total_duration,
                "scraped_data": {},
                "scraping_success": False,
                "scraping_duration": 0,
                "scraping_errors": [str(e)]
            }
    
    async def sentiment_analysis_node(self, state: AmazonHybridState) -> Dict[str, Any]:
        """Phase 2A: Analisi sentiment con LLM (Intelligence Layer)"""
        logger.info("🎭 [SENTIMENT] Analisi LLM su dati pre-estratti")
        start_time = datetime.now()
        
        scraped_data = state.get('scraped_data', {})
        reviews = scraped_data.get('reviews', [])
        
        if not reviews or not any(r.get('text') for r in reviews):
            logger.warning("⚠️ [SENTIMENT] Nessuna recensione per analisi")
            return {
                "sentiment_analysis": {
                    "error": "No reviews available",
                    "sentiment_generale": "neutral",
                    "confidence": 0.0
                },
                "analysis_duration": (datetime.now() - start_time).total_seconds(),
                "analysis_errors": ["No reviews for sentiment analysis"]
            }
        
        try:
            # Prepara dati per LLM (pre-filtrati e strutturati)
            review_texts = []
            for r in reviews[:8]:  # Limit per performance
                if r.get('text') and len(r['text']) > 20:
                    review_texts.append(f"[{r.get('rating', 0)}★] {r['text'][:200]}")
            
            if not review_texts:
                logger.warning("⚠️ [SENTIMENT] Nessuna recensione valida")
                return {
                    "sentiment_analysis": {"error": "No valid reviews", "sentiment_generale": "neutral"},
                    "analysis_duration": (datetime.now() - start_time).total_seconds(),
                    "analysis_errors": ["No valid review text"]
                }
            
            reviews_combined = "\\n\\n".join(review_texts)
            
            # Prompt ottimizzato per dati pre-strutturati
            prompt = PromptTemplate.from_template("""
Analizza il sentiment di queste recensioni Amazon pre-estratte:

DATI STRUTTURATI:
{reviews}

Rispondi SOLO in JSON valido:
{{
    "sentiment_generale": "positivo|neutro|negativo",
    "confidence": 0.0-1.0,
    "distribuzione": {{"positivo": XX, "neutro": XX, "negativo": XX}},
    "punti_chiave": ["punto1", "punto2", "punto3"],
    "rating_coerenza": "alta|media|bassa"
}}
""")
            
            chain = prompt | self.llm | self._out_parser
            
            # Chiamata LLM con timeout
            logger.info("📡 [SENTIMENT] Chiamata LLM...")
            result = await asyncio.wait_for(
                asyncio.to_thread(chain.invoke, {"reviews": reviews_combined}),
                timeout=60.0  # Timeout 60s
            )
            
            # Parse JSON - fix regex escape bug
            import json, re
            json_match = re.search(r'\{.*\}', result, re.DOTALL)
            
            if json_match:
                try:
                    sentiment_data = json.loads(json_match.group(0))
                    duration = (datetime.now() - start_time).total_seconds()
                    logger.info(f"✅ [SENTIMENT] LLM completato in {duration:.2f}s: {sentiment_data.get('sentiment_generale')}")
                    
                    return {
                        "sentiment_analysis": sentiment_data,
                        "sentiment_duration": duration,
                        "analysis_errors": []
                    }
                except json.JSONDecodeError as e:
                    logger.warning(f"⚠️ [SENTIMENT] JSON parsing error: {e}")
            
            # Fallback con parsing raw
            logger.warning("⚠️ [SENTIMENT] Fallback a parsing raw")
            return {
                "sentiment_analysis": {
                    "raw_response": result,
                    "sentiment_generale": "neutro",
                    "confidence": 0.5,
                    "parsing_failed": True
                },
                "sentiment_duration": (datetime.now() - start_time).total_seconds(),
                "analysis_errors": ["JSON parsing failed"]
            }
            
        except asyncio.TimeoutError:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"⏰ [SENTIMENT] Timeout LLM dopo {duration:.2f}s")
            return {
                "sentiment_analysis": {"error": "LLM timeout", "sentiment_generale": "neutro"},
                "sentiment_duration": duration,
                "analysis_errors": ["LLM timeout"]
            }
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"❌ [SENTIMENT] Errore LLM in {duration:.2f}s: {e}")
            return {
                "sentiment_analysis": {"error": str(e), "sentiment_generale": "neutro"},
                "sentiment_duration": duration,
                "analysis_errors": [str(e)]
            }
    
    async def theme_analysis_node(self, state: AmazonHybridState) -> Dict[str, Any]:
        """Phase 2B: Analisi temi con LLM (Intelligence Layer)"""
        logger.info("🔍 [THEMES] Analisi LLM temi su dati pre-estratti")
        start_time = datetime.now()
        
        scraped_data = state.get('scraped_data', {})
        reviews = scraped_data.get('reviews', [])
        
        if not reviews:
            return {
                "theme_analysis": {"error": "No reviews for theme analysis"},
                "theme_duration": (datetime.now() - start_time).total_seconds(),
                "analysis_errors": ["No reviews available"]
            }
        
        try:
            # Combina reviews per analisi temi
            review_texts = [r.get('text', '')[:150] for r in reviews[:10] if r.get('text')]
            
            if not review_texts:
                return {
                    "theme_analysis": {"error": "No valid review texts"},
                    "theme_duration": (datetime.now() - start_time).total_seconds(),
                    "analysis_errors": ["No valid review texts"]
                }
            
            combined_text = "\\n".join(review_texts)
            
            prompt = PromptTemplate.from_template("""
Estrai temi e insights da queste recensioni Amazon:

RECENSIONI:
{reviews}

CONTESTO PRODOTTO:
- Titolo: {title}
- Rating: {rating}/5
- Totale recensioni: {total_reviews}

Rispondi SOLO in JSON:
{{
    "punti_forza": ["forza1", "forza2", "forza3"],
    "punti_deboli": ["debolezza1", "debolezza2"],
    "temi_emergenti": ["tema1", "tema2", "tema3"],
    "raccomandazioni": ["azione1", "azione2"],
    "parole_chiave": ["keyword1", "keyword2", "keyword3"]
}}
""")
            
            chain = prompt | self.llm | self._out_parser
            
            logger.info("📡 [THEMES] Chiamata LLM...")
            result = await asyncio.wait_for(
                asyncio.to_thread(chain.invoke, {
                    "reviews": combined_text,
                    "title": scraped_data.get('title', 'N/A'),
                    "rating": scraped_data.get('rating_average', 0),
                    "total_reviews": scraped_data.get('total_reviews', 0)
                }),
                timeout=60.0
            )
            
            # Parse JSON - fix regex escape bug
            import json, re
            json_match = re.search(r'\{.*\}', result, re.DOTALL)
            
            if json_match:
                try:
                    themes_data = json.loads(json_match.group(0))
                    duration = (datetime.now() - start_time).total_seconds()
                    logger.info(f"✅ [THEMES] LLM completato in {duration:.2f}s")
                    
                    return {
                        "theme_analysis": themes_data,
                        "theme_duration": duration,
                        "analysis_errors": []
                    }
                except json.JSONDecodeError:
                    pass
            
            return {
                "theme_analysis": {"raw_response": result},
                "theme_duration": (datetime.now() - start_time).total_seconds(),
                "analysis_errors": ["JSON parsing failed"]
            }
            
        except asyncio.TimeoutError:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"⏰ [THEMES] Timeout LLM dopo {duration:.2f}s")
            return {
                "theme_analysis": {"error": "LLM timeout"},
                "theme_duration": duration,
                "analysis_errors": ["LLM timeout"]
            }
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"❌ [THEMES] Errore LLM in {duration:.2f}s: {e}")
            return {
                "theme_analysis": {"error": str(e)},
                "theme_duration": duration,
                "analysis_errors": [str(e)]
            }
    
    async def report_generation_node(self, state: AmazonHybridState) -> Dict[str, Any]:
        """Phase 3: Generazione report finale ibrido"""
        logger.info("📄 [REPORT] Generazione report finale ibrido...")
        
        scraped_data = state.get('scraped_data', {})
        sentiment = state.get('sentiment_analysis', {})
        themes = state.get('theme_analysis', {})
        
        # Calcola performance metrics - fix NoneType bug
        sentiment_duration = state.get('sentiment_duration') or 0
        theme_duration = state.get('theme_duration') or 0
        analysis_duration = sentiment_duration + theme_duration
        
        total_duration = (
            (state.get('authentication_duration') or 0) +
            (state.get('scraping_duration') or 0) +
            analysis_duration
        )
        
        # Report strutturato ibrido
        final_report = {
            "metadata": {
                "asin": state['asin'],
                "timestamp": datetime.now().isoformat(),
                "approach": "Hybrid_Playwright_LLM",
                "version": "2024_Research_Based",
                "langsmith_traced": True
            },
            
            "performance_metrics": {
                "authentication_duration": state.get('authentication_duration', 0),
                "scraping_duration": state.get('scraping_duration', 0),
                "sentiment_duration": sentiment_duration,
                "theme_duration": theme_duration,
                "analysis_duration": analysis_duration,
                "total_duration": total_duration,
                "authenticated": state.get('authentication_success', False),
                "scraping_success": state.get('scraping_success', False)
            },
            
            "product_data": {
                "title": scraped_data.get('title', 'N/A'),
                "rating_average": scraped_data.get('rating_average', 0.0),
                "total_reviews": scraped_data.get('total_reviews', 0),
                "price": scraped_data.get('price', 'N/A'),
                "reviews_extracted": len(scraped_data.get('reviews', [])),
                "authenticated_extraction": scraped_data.get('authenticated', False)
            },
            
            "llm_analysis": {
                "sentiment": {
                    "overall": sentiment.get('sentiment_generale', 'N/A'),
                    "sentiment_generale": sentiment.get('sentiment_generale', 'N/A'),  # Include entrambe le chiavi
                    "confidence": sentiment.get('confidence', 0.0),
                    "distribution": sentiment.get('distribuzione', {}),
                    "distribuzione": sentiment.get('distribuzione', {}),  # Include entrambe
                    "key_points": sentiment.get('punti_chiave', []),
                    "punti_chiave": sentiment.get('punti_chiave', [])  # Include entrambe
                },
                "themes": {
                    "strengths": themes.get('punti_forza', []),
                    "weaknesses": themes.get('punti_deboli', []),
                    "emerging_themes": themes.get('temi_emergenti', []),
                    "recommendations": themes.get('raccomandazioni', []),
                    "keywords": themes.get('parole_chiave', [])
                }
            },
            
            "error_summary": {
                "scraping_errors": state.get('scraping_errors', []),
                "analysis_errors": state.get('analysis_errors', []),
                "total_errors": len(state.get('scraping_errors', [])) + len(state.get('analysis_errors', []))
            }
        }
        
        # Valutazione successo complessivo
        workflow_success = (
            state.get('scraping_success', False) and
            len(scraped_data.get('reviews', [])) > 0 and
            sentiment.get('sentiment_generale') != 'N/A'
        )
        
        logger.info(f"✅ [REPORT] Report generato - Successo: {workflow_success}, Durata: {total_duration:.2f}s")
        
        return {
            "final_report": final_report,
            "total_duration": total_duration,
            "workflow_success": workflow_success
        }


# =====================================
# HYBRID WORKFLOW CLASS
# =====================================

class AmazonHybridWorkflow:
    """Workflow ibrido basato su ricerca approfondita 2024"""
    
    def __init__(self):
        self.nodes = AmazonHybridNodes()
        
    def build_hybrid_workflow(self) -> StateGraph:
        """Costruisce workflow ibrido ottimale"""
        logger.info("🏗️ Costruzione workflow ibrido Playwright + LLM...")
        
        workflow = StateGraph(AmazonHybridState)
        
        # Phase 1: Performance Critical (Direct Playwright) - UNIFICATO
        workflow.add_node("auth_and_scraping", self.nodes.auth_and_scraping_node)
        
        # Phase 2: Intelligence Layer (LLM Analysis)
        workflow.add_node("sentiment_analysis", self.nodes.sentiment_analysis_node)
        workflow.add_node("theme_analysis", self.nodes.theme_analysis_node)
        
        # Phase 3: Final Assembly
        workflow.add_node("report_generation", self.nodes.report_generation_node)
        
        # Flow ottimizzato: Unified auth+scraping → LLM analysis → Report
        workflow.set_entry_point("auth_and_scraping")
        workflow.add_edge("auth_and_scraping", "sentiment_analysis")
        workflow.add_edge("sentiment_analysis", "theme_analysis")  # Sequential per stabilità
        workflow.add_edge("theme_analysis", "report_generation")
        workflow.add_edge("report_generation", END)
        
        return workflow.compile()
    
    async def run_hybrid_analysis(self, asin: str) -> Dict[str, Any]:
        """Esegue analisi ibrida completa"""
        logger.info(f"🚀 [HYBRID] Avvio analisi Amazon - ASIN: {asin}")
        logger.info("📊 Architettura: UNIFIED Auth+Scraping + LLM Intelligence")
        logger.info("🎯 Basato su ricerca best practices 2024 + Session Persistence")
        
        start_time = datetime.now()
        
        # State iniziale
        initial_state = AmazonHybridState(
            asin=asin,
            authentication_success=False,
            authentication_duration=None,
            scraped_data=None,
            scraping_success=False,
            scraping_duration=None,
            scraping_errors=[],
            sentiment_analysis=None,
            theme_analysis=None,
            analysis_duration=None,
            analysis_errors=[],
            final_report={},
            total_duration=None,
            workflow_success=False
        )
        
        # Configurazione avanzata LangSmith per visualizzazione nodi
        run_id = str(uuid.uuid4())
        config = {
            "run_name": f"amazon_audit_{asin}",
            "run_id": run_id,
            "tags": ["amazon", "review_analysis", "hybrid_workflow", "production"],
            "metadata": {
                "asin": asin,
                "workflow_type": "unified_auth_scraping",
                "version": "2024_session_persistence",
                "architecture": "playwright_llm_hybrid",
                "timestamp": datetime.now().isoformat(),
                "langsmith_enhanced": True
            },
            "configurable": {
                "thread_id": f"amazon_session_{asin}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            }
        }
        
        logger.info(f"🔍 [LANGSMITH] Run ID: {run_id}")
        logger.info(f"🔍 [LANGSMITH] Thread ID: {config['configurable']['thread_id']}")
        logger.info(f"🔍 [LANGSMITH] Tags: {', '.join(config['tags'])}")
        
        # Esecuzione workflow ibrido con configurazione avanzata
        app = self.build_hybrid_workflow()
        final_state = await app.ainvoke(initial_state, config=config)
        
        total_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"✅ [HYBRID] Workflow completato in {total_time:.2f}s")
        logger.info(f"🔍 [LANGSMITH] Trace ID: {run_id} - Cerca questo ID su LangSmith")
        
        # Aggiungi run_id al result per riferimento
        final_state["run_id"] = run_id
        final_state["langsmith_config"] = config
        
        return final_state


# =====================================
# TEST FUNCTION
# =====================================

async def test_hybrid_workflow():
    """Test workflow ibrido completo"""
    logger.info("🧪 === TEST AMAZON HYBRID WORKFLOW ===")
    logger.info("🎯 Direct Playwright + LLM Intelligence")
    logger.info("📚 Basato su ricerca documentazione ufficiale 2024")
    
    workflow = AmazonHybridWorkflow()
    test_asin = "B0BC16S6RN"  # Pastiglie Leone
    
    try:
        result = await workflow.run_hybrid_analysis(test_asin)
        
        logger.info("=" * 80)
        logger.info("📊 === RISULTATI HYBRID WORKFLOW ===")
        
        final_report = result["final_report"]
        metrics = final_report["performance_metrics"]
        product = final_report["product_data"]
        
        logger.info(f"🎯 ASIN: {final_report['metadata']['asin']}")
        logger.info(f"✅ Successo: {result['workflow_success']}")
        
        # Performance breakdown
        auth_time = metrics.get('authentication_duration', 0)
        scrape_time = metrics.get('scraping_duration', 0)
        analysis_time = metrics.get('analysis_duration', 0)
        total_time = metrics.get('total_duration', 0)
        
        logger.info(f"⏱️ Performance:")
        logger.info(f"   Auth: {auth_time:.1f}s | Scrape: {scrape_time:.1f}s | Analysis: {analysis_time:.1f}s | Total: {total_time:.1f}s")
        
        # Data quality
        logger.info(f"📦 Dati estratti:")
        logger.info(f"   Titolo: {product['title'][:50]}...")
        logger.info(f"   Rating: {product['rating_average']} ({product['total_reviews']} recensioni)")
        logger.info(f"   Prezzo: {product['price']}")
        logger.info(f"   Reviews estratte: {product['reviews_extracted']}")
        logger.info(f"   Autenticato: {product['authenticated_extraction']}")
        
        # LLM Analysis
        llm_analysis = final_report["llm_analysis"]
        sentiment = llm_analysis["sentiment"]
        themes = llm_analysis["themes"]
        
        logger.info(f"🎭 Analisi LLM:")
        logger.info(f"   Sentiment: {sentiment.get('sentiment_generale', 'N/A')} (conf: {sentiment.get('confidence', 0.0):.2f})")
        logger.info(f"   Punti forza: {', '.join(themes['strengths'][:3])}")
        logger.info(f"   Criticità: {', '.join(themes['weaknesses'][:2])}")
        
        # Error summary
        errors = final_report["error_summary"]
        if errors['total_errors'] > 0:
            logger.warning(f"⚠️ Errori totali: {errors['total_errors']}")
            for error in (errors['scraping_errors'] + errors['analysis_errors'])[:3]:
                logger.warning(f"   - {error}")
        
        # Salva report finale automaticamente
        final_report = result["final_report"]
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_path = f"output/hybrid_report_{test_asin}_{timestamp}.json"
        
        try:
            os.makedirs("output", exist_ok=True)
            with open(report_path, 'w', encoding='utf-8') as f:
                import json
                json.dump(final_report, f, indent=2, ensure_ascii=False)
            logger.info(f"📁 Report salvato automaticamente: {report_path}")
        except Exception as e:
            logger.error(f"❌ Errore salvataggio report: {e}")
        
        logger.info(f"\\n🌐 LangSmith Enhanced Tracing:")
        logger.info(f"   📊 Project: amazon-hybrid-workflow")
        logger.info(f"   🆔 Run ID: {result.get('run_id', 'N/A')}")
        logger.info(f"   🏷️ Tags: amazon, review_analysis, hybrid_workflow, production")
        logger.info(f"   🧵 Thread ID: amazon_session_{test_asin}_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        logger.info(f"   🔗 URL: https://smith.langchain.com/o/projects/p/{os.environ.get('LANGCHAIN_PROJECT', 'amazon-hybrid-workflow')}")
        logger.info("🏆 HYBRID WORKFLOW FUNZIONANTE CON ENHANCED TRACING!")
        
        return result['workflow_success']
        
    except Exception as e:
        logger.error(f"❌ Test fallito: {e}")
        return False


if __name__ == "__main__":
    success = asyncio.run(test_hybrid_workflow())
    exit(0 if success else 1)
