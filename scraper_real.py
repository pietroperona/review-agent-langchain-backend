"""
Scraper REALE Amazon con Playwright
NO MOCK - Solo implementazioni vere per test concreti
"""

import asyncio
import os
import random
import time
from datetime import datetime
from typing import List, Dict, Optional, Any
from loguru import logger
from playwright.async_api import async_playwright, Page, Browser

# Carica variabili da .env se presente (non obbligatorio)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# Setup LangSmith REALE (non sovrascrivere se già impostate)
os.environ.setdefault("LANGCHAIN_TRACING_V2", "true")
os.environ.setdefault("LANGCHAIN_PROJECT", "amazon-review-audit")
os.environ.setdefault("LANGCHAIN_ENDPOINT", "https://api.smith.langchain.com")

from langchain_ollama import OllamaLLM
from pydantic import BaseModel, Field
from typing import List


def setup_logging():
    logger.remove()
    logger.add(
        lambda x: print(x, end=""),
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>\n",
        level="INFO"
    )


class ReviewData(BaseModel):
    """Struttura REALE per review Amazon"""
    rating: int = Field(description="Rating da 1 a 5")
    title: str = Field(description="Titolo recensione")
    text: str = Field(description="Testo completo recensione")
    author: str = Field(description="Nome autore")
    date: str = Field(description="Data recensione")
    verified: bool = Field(description="Acquisto verificato", default=False)
    helpful_votes: int = Field(description="Voti utili", default=0)


class ProductData(BaseModel):
    """Struttura REALE per prodotto Amazon"""
    asin: str
    title: str
    rating_average: float
    total_reviews: int
    price: Optional[str] = None
    availability: Optional[str] = None
    reviews: List[ReviewData] = []


class AmazonScraperReal:
    """Scraper REALE Amazon - NO MOCK"""
    
    def __init__(self, headless: bool = False, login_credentials: Optional[Dict[str, Any]] = None):
        self.headless = headless  # Default False per vedere il browser
        self.login_credentials = login_credentials or {}
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None
        self.user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        
    async def __aenter__(self):
        """Async context manager entry"""
        await self.start_browser()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close_browser()
        
    async def start_browser(self):
        """Avvia browser REALE con anti-detection"""
        logger.info("🚀 Avvio browser Playwright REALE...")
        
        playwright = await async_playwright().__aenter__()
        
        # Browser context REALE con OFFICIAL FLAGS per testing automatico
        # Fonte: Google Chrome Launcher + Chromium Project Documentation
        self.browser = await playwright.chromium.launch(
            headless=self.headless,
            args=[
                # CORE AUTOMATION FLAGS (Google Chrome Launcher Official)
                "--enable-automation",           # ✅ UFFICIALE: Automation mode 
                "--test-type",                   # ✅ UFFICIALE: Test mode behavior
                "--deny-permission-prompts",     # ✅ UFFICIALE: Deny all permission prompts
                "--noerrdialogs",               # ✅ UFFICIALE: Suppress all error dialogs
                "--no-first-run",               # ✅ UFFICIALE: Skip first run wizards
                
                # WEBAUTHN/PASSKEY DISABLING
                "--disable-features=WebAuthenticationAPI",  # ✅ Disabilita WebAuthn API nativamente
                
                # ANTI-DETECTION STANDARD
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--disable-extensions",
                "--disable-gpu",
                "--no-default-browser-check",
                "--disable-background-timer-throttling",
                "--disable-renderer-backgrounding",
                "--override-plugin-power-saver-for-testing=never",
                "--disable-backgrounding-occluded-windows",
                "--disable-web-security"
            ]
        )
        
        # Crea contesto con settings realistici
        context = await self.browser.new_context(
            viewport={"width": 1366, "height": 768},
            user_agent=self.user_agent,
            locale="it-IT",
            timezone_id="Europe/Rome",
            extra_http_headers={
                "Accept-Language": "it-IT,it;q=0.9,en;q=0.8",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            }
        )
        
        self.page = await context.new_page()
        
        # Iniezioni anti-detection + WEBAUTHN DISABLING
        await self.page.add_init_script("""
            // Anti-detection standard
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined,
            });
            
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5],
            });
            
            Object.defineProperty(navigator, 'languages', {
                get: () => ['it-IT', 'it', 'en'],
            });
            
            // DISABILITA COMPLETAMENTE WEBAUTHN/PASSKEY APIs
            // Questo dovrebbe prevenire il popup nativo
            if (typeof navigator.credentials !== 'undefined') {
                delete navigator.credentials;
            }
            navigator.credentials = undefined;
            
            if (typeof window.PublicKeyCredential !== 'undefined') {
                delete window.PublicKeyCredential;
            }
            window.PublicKeyCredential = undefined;
            
            if (typeof window.AuthenticatorAssertionResponse !== 'undefined') {
                delete window.AuthenticatorAssertionResponse;
            }
            window.AuthenticatorAssertionResponse = undefined;
            
            if (typeof window.AuthenticatorAttestationResponse !== 'undefined') {
                delete window.AuthenticatorAttestationResponse;
            }
            window.AuthenticatorAttestationResponse = undefined;
            
            // Override isUserVerifyingPlatformAuthenticatorAvailable per sicurezza
            if (typeof window.PublicKeyCredential !== 'undefined' && 
                typeof window.PublicKeyCredential.isUserVerifyingPlatformAuthenticatorAvailable === 'function') {
                window.PublicKeyCredential.isUserVerifyingPlatformAuthenticatorAvailable = () => Promise.resolve(false);
            }
            
            console.log('🔒 WebAuthn APIs completely disabled');
        """)
        
        logger.info("✅ Browser avviato con successo")
        
    async def perform_login(self) -> bool:
        """Esegue login Amazon REALE con 2FA"""
        assert self.page is not None, "Browser non inizializzato"
        
        if not self.login_credentials.get('email') or not self.login_credentials.get('password'):
            logger.warning("⚠️ Credenziali login non fornite, procedo senza login")
            return False
            
        logger.info("🔐 Inizio processo login Amazon REALE...")
        
        try:
            # Prima vai alla homepage per ottenere la sessione corretta
            logger.info("🏠 Caricamento homepage Amazon...")
            await self.page.goto("https://www.amazon.it", wait_until="domcontentloaded")
            await self.random_delay(2, 4)
            
            # Trova e clicca il link login (che genera gli OpenID params corretti)
            logger.info("🔍 Ricerca pulsante login...")
            login_selectors = [
                "#nav-link-accountList",
                "a[data-nav-role='signin']", 
                ".nav-sign-in",
                "#nav-signin-tooltip .nav-action-button",
                "a[href*='signin']"
            ]
            
            login_clicked = False
            for selector in login_selectors:
                try:
                    login_element = await self.page.wait_for_selector(selector, timeout=5000)
                    if login_element:
                        logger.info(f"✅ Click su login: {selector}")
                        await login_element.click()
                        await self.random_delay(3, 5)
                        login_clicked = True
                        break
                except:
                    continue
            
            if not login_clicked:
                logger.warning("⚠️ Pulsante login non trovato, provo URL diretta...")
                await self.page.goto("https://www.amazon.it/ap/signin?openid.pape.max_auth_age=0&openid.return_to=https%3A%2F%2Fwww.amazon.it%2F%3Fref_%3Dnav_signin&openid.identity=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0%2Fidentifier_select&openid.assoc_handle=itflex&openid.mode=checkid_setup&openid.claimed_id=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0%2Fidentifier_select&openid.ns=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0", wait_until="domcontentloaded")
                await self.random_delay(2, 4)
            
            # Email
            email_field = await self.page.wait_for_selector("#ap_email", timeout=10000)
            if email_field is None:
                raise RuntimeError("Campo email non trovato")
            await email_field.fill(self.login_credentials['email'])
            logger.info(f"✅ Email inserita: {self.login_credentials['email'][:3]}***")
            
            # Continue button
            continue_btn = await self.page.wait_for_selector("#continue", timeout=5000)
            if continue_btn is None:
                raise RuntimeError("Pulsante continue non trovato")
            await continue_btn.click()
            await self.random_delay(2, 3)
            
            # Password
            password_field = await self.page.wait_for_selector("#ap_password", timeout=10000)
            if password_field is None:
                raise RuntimeError("Campo password non trovato")
            await password_field.fill(self.login_credentials['password'])
            logger.info("✅ Password inserita")
            
            # Sign in button
            signin_btn = await self.page.wait_for_selector("#signInSubmit", timeout=5000)
            if signin_btn is None:
                raise RuntimeError("Pulsante signin non trovato")
            await signin_btn.click()
            await self.random_delay(3, 5)
            
            # Controlla se serve 2FA
            await self.handle_2fa_if_needed()
            
            # Verifica se login è riuscito
            try:
                # Cerca elementi che indicano login riuscito
                await self.page.wait_for_selector(
                    "#nav-link-accountList, .nav-line-2", 
                    timeout=10000
                )
                logger.info("✅ Login Amazon completato con successo!")
                return True
            except:
                # Controlla se ci sono errori
                error_selectors = [
                    ".a-alert-error", 
                    "#auth-error-message-box",
                    ".a-list-item .a-text-bold"
                ]
                
                for selector in error_selectors:
                    try:
                        error_element = await self.page.query_selector(selector)
                        if error_element:
                            error_text = await error_element.inner_text()
                            logger.error(f"❌ Errore login: {error_text}")
                            return False
                    except:
                        continue
                        
                logger.warning("⚠️ Stato login incerto, procedo comunque")
                return True
                
        except Exception as e:
            logger.error(f"❌ Errore durante login: {e}")
            return False
            
    async def handle_2fa_if_needed(self) -> bool:
        """Gestisce 2FA Amazon se richiesto"""
        assert self.page is not None, "Browser non inizializzato"
        logger.info("🔒 Verifica necessità 2FA...")
        
        try:
            # Controlla se siamo nella pagina 2FA
            otp_selectors = [
                "#auth-mfa-otpcode",
                "#ap_add_phone_number", 
                "#auth-mfa-remember-device",
                ".cvf-widget-input-code"
            ]
            
            otp_field = None
            for selector in otp_selectors:
                try:
                    otp_field = await self.page.wait_for_selector(selector, timeout=3000)
                    if otp_field:
                        logger.info(f"🎯 Campo 2FA trovato: {selector}")
                        break
                except:
                    continue
                    
            if not otp_field:
                logger.info("✅ Nessun 2FA richiesto")
                return True
                
            # 2FA richiesto
            logger.info("🔐 2FA richiesto! Modalità disponibili:")
            
            # Verifica se abbiamo TOTP secret
            if self.login_credentials.get('totp_secret'):
                logger.info("📱 Usando TOTP automatico...")
                return await self.handle_totp_2fa(otp_field)
            else:
                logger.info("📱 TOTP automatico non configurato")
                logger.info("⏸️ PAUSA - Inserisci manualmente il codice 2FA nel browser")
                logger.info("👀 Il browser è visibile - inserisci il codice e premi invio")
                
                # Aspetta che l'utente completi manualmente
                return await self.wait_for_manual_2fa_completion()
                
        except Exception as e:
            logger.error(f"❌ Errore gestione 2FA: {e}")
            return False
            
    async def handle_totp_2fa(self, otp_field) -> bool:
        """Gestisce 2FA con TOTP automatico"""
        assert self.page is not None, "Browser non inizializzato"
        try:
            import pyotp
            
            totp_secret = self.login_credentials['totp_secret']
            totp = pyotp.TOTP(totp_secret)
            current_code = totp.now()
            
            logger.info(f"🔢 Codice TOTP generato: {current_code}")
            
            # Inserisci il codice
            await otp_field.fill(current_code)
            await self.random_delay(1, 2)
            
            # Cerca e clicca submit
            submit_selectors = [
                "#auth-signin-button",
                "#signInSubmit", 
                "input[type='submit']",
                ".a-button-input"
            ]
            
            for selector in submit_selectors:
                try:
                    submit_btn = await self.page.query_selector(selector)
                    if submit_btn:
                        await submit_btn.click()
                        logger.info("✅ Codice TOTP inviato")
                        await self.random_delay(3, 5)
                        return True
                except:
                    continue
                    
            logger.warning("⚠️ Submit button non trovato per TOTP")
            return False
            
        except ImportError:
            logger.error("❌ pyotp non installato. Installa con: pip install pyotp")
            return False
        except Exception as e:
            logger.error(f"❌ Errore TOTP: {e}")
            return False
            
    async def wait_for_manual_2fa_completion(self) -> bool:
        """Aspetta che utente completi 2FA manualmente"""
        assert self.page is not None, "Browser non inizializzato"
        logger.info("⏳ Aspetto completamento manuale 2FA...")
        logger.info("🖱️ Inserisci il codice nel browser visibile e procedi")
        
        # Aspetta fino a 300 secondi (5 minuti)
        for i in range(60):  # 60 tentativi da 5 secondi = 5 minuti
            try:
                # Controlla se siamo usciti dalla pagina 2FA
                current_url = self.page.url
                if "signin" not in current_url or "amazon.it" in current_url and len(current_url) < 50:
                    logger.info("✅ 2FA completato manualmente!")
                    return True
                    
                # Controlla se siamo nella homepage/account
                try:
                    account_element = await self.page.query_selector("#nav-link-accountList, .nav-line-2")
                    if account_element:
                        logger.info("✅ 2FA completato - rilevato elemento account!")
                        return True
                except:
                    pass
                    
                if i % 12 == 0:  # Ogni minuto circa
                    minutes_left = 5 - (i // 12)
                    logger.info(f"⏳ Ancora {minutes_left} minuti per completare 2FA...")
                    
                await asyncio.sleep(5)
                
            except Exception as e:
                logger.warning(f"⚠️ Errore controllo 2FA: {e}")
                await asyncio.sleep(5)
                continue
                
        logger.error("⏰ Timeout 2FA - troppo tempo per completare")
        return False
        
    async def close_browser(self):
        """Chiude browser"""
        if self.browser:
            await self.browser.close()
            logger.info("🔒 Browser chiuso")
            
    async def random_delay(self, min_seconds: float = 1.0, max_seconds: float = 3.0):
        """Delay casuali umani"""
        delay = random.uniform(min_seconds, max_seconds)
        await asyncio.sleep(delay)
        
    async def human_scroll(self, page: Optional[Page], scroll_count: int = 3):
        """Scroll umano realistico"""
        if page is None:
            return
        for i in range(scroll_count):
            scroll_amount = random.randint(300, 800)
            await page.mouse.wheel(0, scroll_amount)
            await self.random_delay(0.5, 1.5)
            
    async def navigate_to_product(self, asin: str, with_login: bool = True) -> bool:
        """Naviga alla pagina prodotto REALE con login opzionale"""
        assert self.page is not None, "Browser non inizializzato"
            
        # Prima esegui login se richiesto e credenziali disponibili
        if with_login and self.login_credentials:
            logger.info("🔐 Esecuzione login prima dello scraping...")
            login_success = await self.perform_login()
            if login_success:
                logger.info("✅ Login completato - procedo con scraping autenticato")
            else:
                logger.warning("⚠️ Login fallito - procedo senza autenticazione")
        else:
            # Vai alla homepage senza login
            logger.info("🏠 Navigazione homepage senza login...")
            try:
                await self.page.goto("https://www.amazon.it", wait_until="domcontentloaded", timeout=30000)
                await self.random_delay(2, 4)
                
                # Gestione automatica cookie banner
                await self.handle_cookie_banner()
                
                # Simula comportamento umano sulla homepage (ridotto)
                await self.human_scroll(self.page, scroll_count=1)
                await self.random_delay(1, 2)
                
            except Exception as e:
                logger.warning(f"⚠️ Homepage non accessibile: {e}")
            
        # Poi naviga al prodotto
        url = f"https://www.amazon.it/dp/{asin}"
        logger.info(f"🧭 Navigazione REALE a: {url}")
        
        try:
            # Navigazione REALE
            await self.page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await self.random_delay(2, 3)  # Delay ridotto
            
            # Gestione cookie banner anche sulla pagina prodotto
            await self.handle_cookie_banner()
            
            # Debug: salva screenshot per capire cosa succede
            await self.page.screenshot(path=f"debug_page_{asin}.png")
            logger.info("📸 Screenshot salvato per debug")
            
            # Verifica se siamo bloccati da captcha
            captcha_selectors = [
                "img[alt*='captcha']",
                "[data-cy='captcha']",
                ".captcha-container"
            ]
            
            for selector in captcha_selectors:
                try:
                    captcha_el = await self.page.query_selector(selector)
                    if captcha_el:
                        logger.warning("🤖 CAPTCHA rilevato - Amazon ha bloccato l'accesso")
                        return False
                except:
                    continue
            
            # Verifica se la pagina è caricata - selettori più permissivi
            title_selectors = [
                "#productTitle",
                "[data-cy='title'] h1", 
                "h1.a-size-large",
                "h1",
                ".product-title",
                ".a-size-large"
            ]
            
            for selector in title_selectors:
                try:
                    title_el = await self.page.wait_for_selector(selector, timeout=5000)
                    if title_el:
                        title_text = await title_el.inner_text()
                        if title_text and len(title_text.strip()) > 0:
                            logger.info(f"✅ Pagina prodotto caricata: {title_text[:30]}...")
                            return True
                except:
                    continue
                    
            # Verifica generica presenza contenuto
            page_content = await self.page.content()
            if "amazon" in page_content.lower() and len(page_content) > 1000:
                logger.info("✅ Pagina Amazon caricata (verifica generica)")
                return True
            else:
                logger.warning("⚠️ Possibile blocco o redirect Amazon")
                return False
                
        except Exception as e:
            logger.error(f"❌ Errore navigazione: {e}")
            return False
            
    async def extract_product_info(self, asin: str) -> Dict:
        """Estrae info prodotto REALI"""
        assert self.page is not None, "Browser non inizializzato"
        logger.info(f"📦 Estrazione dati prodotto REALE per ASIN: {asin}")
        
        product_data = {
            "asin": asin,
            "title": "Prodotto non trovato",
            "rating_average": 0.0,
            "total_reviews": 0,
            "price": None,
            "availability": None
        }
        
        try:
            # Titolo prodotto
            title_selectors = [
                "#productTitle",
                "[data-cy='title'] h1", 
                "h1.a-size-large",
                ".product-title h1"
            ]
            
            for selector in title_selectors:
                try:
                    title_element = await self.page.wait_for_selector(selector, timeout=5000)
                    if title_element:
                        title = await title_element.inner_text()
                        product_data["title"] = title.strip()
                        logger.info(f"📝 Titolo: {product_data['title'][:50]}...")
                        break
                except:
                    continue
                    
            # Rating medio e numero recensioni
            rating_selectors = [
                "[data-hook='average-star-rating'] .a-icon-alt",
                ".a-icon-star .a-icon-alt",
                "[data-cy='reviews-block'] .a-icon-alt"
            ]
            
            for selector in rating_selectors:
                try:
                    rating_element = await self.page.wait_for_selector(selector, timeout=5000)
                    if rating_element:
                        rating_text = await rating_element.inner_text()
                        # Estrai rating numerico (es. "4,3 su 5 stelle")
                        import re
                        match = re.search(r'(\d+[,.]?\d*)', rating_text)
                        if match:
                            rating_str = match.group(1).replace(',', '.')
                            product_data["rating_average"] = float(rating_str)
                            logger.info(f"⭐ Rating: {product_data['rating_average']}")
                        break
                except:
                    continue
                    
            # Numero totale recensioni
            reviews_count_selectors = [
                "#acrCustomerReviewText",
                "[data-hook='total-review-count']",
                "a[href*='#customerReviews'] span"
            ]
            
            for selector in reviews_count_selectors:
                try:
                    count_element = await self.page.wait_for_selector(selector, timeout=5000)
                    if count_element:
                        count_text = await count_element.inner_text()
                        # Estrai numero (es. "1.234 valutazioni")
                        import re
                        match = re.search(r'([\d.,]+)', count_text)
                        if match:
                            count_str = match.group(1).replace('.', '').replace(',', '')
                            product_data["total_reviews"] = int(count_str)
                            logger.info(f"📊 Totale recensioni: {product_data['total_reviews']}")
                        break
                except:
                    continue
                    
            # Prezzo
            price_selectors = [
                ".a-price-whole",
                "#priceblock_dealprice", 
                "#priceblock_ourprice",
                "[data-cy='price'] .a-price"
            ]
            
            for selector in price_selectors:
                try:
                    price_element = await self.page.wait_for_selector(selector, timeout=3000)
                    if price_element:
                        price = await price_element.inner_text()
                        product_data["price"] = price.strip()
                        logger.info(f"💰 Prezzo: {product_data['price']}")
                        break
                except:
                    continue
                    
            return product_data
            
        except Exception as e:
            logger.error(f"❌ Errore estrazione dati prodotto: {e}")
            return product_data
            
    async def navigate_to_reviews(self) -> bool:
        """Naviga alla sezione recensioni REALE - priorità pagina corrente"""
        assert self.page is not None, "Browser non inizializzato"
        logger.info("📝 Accesso sezione recensioni...")
        
        try:
            # PRIORITÀ 1: Verifica recensioni già presenti sulla pagina prodotto
            logger.info("🔍 PRIORITÀ: verifica recensioni sulla pagina corrente...")
            await self.human_scroll(self.page, scroll_count=2)
            await self.random_delay(1, 2)
            
            # Cerca prima le recensioni dirette sulla pagina - dai risultati debug
            reviews_section_selectors = [
                "[data-hook='review']",  # DEBUG: 9 elementi trovati!
                "#customerReviews",      # DEBUG: trovato
                "#reviewsMedley",        # DEBUG: trovato  
                "[data-hook='reviews-medley-widget']",  # DEBUG: trovato
                ".review",               # DEBUG: 9 elementi trovati
                ".reviews-section"
            ]
            
            for section_selector in reviews_section_selectors:
                try:
                    # Prima verifica se esistono recensioni
                    review_elements = await self.page.query_selector_all(section_selector)
                    if review_elements and len(review_elements) > 0:
                        logger.info(f"✅ Trovate {len(review_elements)} recensioni direttamente sulla pagina con: {section_selector}")
                        # Scroll per renderle visibili
                        await review_elements[0].scroll_into_view_if_needed()
                        await self.random_delay(1, 2)
                        return True
                except Exception as e:
                    logger.debug(f"⚠️ Selector {section_selector} fallito: {e}")
                    continue
            
            # PRIORITÀ 2: Se non ci sono recensioni sulla pagina, tenta navigazione
            logger.info("🔗 Tentativo navigazione pagina recensioni dedicata...")
            await self.human_scroll(self.page, scroll_count=3)
            await self.random_delay(2, 4)
            
            # Link alle recensioni - selettori Amazon 2024 aggiornati
            reviews_link_selectors = [
                "a[data-hook='see-all-reviews-link-foot']",
                "a[data-hook='see-all-reviews-display-link']", 
                "a[href*='#customerReviews']",
                "[data-hook='total-review-count'] a",
                "a[href*='/product-reviews/']",
                "a[href*='customerReviews']",
                ".cr-widget-ACR a[href*='reviews']",
                "#dp-summary-see-all-reviews a",
                "a[aria-label*='reviews']",
                "a[aria-label*='recensioni']"
            ]
            
            for selector in reviews_link_selectors:
                try:
                    link_element = await self.page.wait_for_selector(selector, timeout=5000)
                    if link_element:
                        logger.info(f"🔗 Click su link recensioni: {selector}")
                        await link_element.click()
                        await self.random_delay(3, 5)
                        
                        # Verifica caricamento pagina recensioni
                        reviews_selectors = [
                            "[data-hook='review-body']",
                            ".review-text",
                            "[data-hook='review']"
                        ]
                        
                        for review_sel in reviews_selectors:
                            try:
                                await self.page.wait_for_selector(review_sel, timeout=10000)
                                logger.info("✅ Pagina recensioni caricata")
                                return True
                            except:
                                continue
                                
                        break
                except:
                    continue
            
            # Se arriviamo qui, nessun metodo ha funzionato
            logger.warning("⚠️ Nessun metodo di accesso recensioni funzionante")
            return False
            
        except Exception as e:
            logger.error(f"❌ Errore navigazione recensioni: {e}")
            return False
            
    async def extract_reviews(self, max_reviews: int = 50) -> List[Dict]:
        """Estrae recensioni REALI dalla pagina"""
        assert self.page is not None, "Browser non inizializzato"
        logger.info(f"📝 Estrazione fino a {max_reviews} recensioni REALI...")
        
        reviews = []
        pages_scraped = 0
        max_pages = 5  # Limite sicurezza
        
        try:
            while len(reviews) < max_reviews and pages_scraped < max_pages:
                pages_scraped += 1
                logger.info(f"📄 Scraping pagina {pages_scraped}...")
                
                # Scroll per caricare recensioni
                await self.human_scroll(self.page, scroll_count=2)
                await self.random_delay(1, 3)
                
                # Selettori per recensioni - Amazon 2024 aggiornati
                review_selectors = [
                    "[data-hook='review']",
                    ".review",
                    "[data-cy='review-item']",
                    "[data-hook='review-body-wrapper']",
                    ".cr-original-review-text",
                    "[data-hook='mobley-review-content']",
                    ".review-item-content",
                    ".a-section.review",
                    ".review-data"
                ]
                
                review_elements = []
                for selector in review_selectors:
                    try:
                        elements = await self.page.query_selector_all(selector)
                        if elements:
                            review_elements = elements
                            logger.info(f"📋 Trovate {len(elements)} recensioni con selector: {selector}")
                            break
                    except:
                        continue
                        
                if not review_elements:
                    logger.warning("⚠️ Nessuna recensione trovata su questa pagina")
                    break
                    
                # Estrai dati da ogni recensione
                for element in review_elements:
                    if len(reviews) >= max_reviews:
                        break
                        
                    try:
                        review_data = await self.extract_single_review(element)
                        if review_data and review_data["text"].strip():
                            reviews.append(review_data)
                            logger.info(f"✅ Recensione {len(reviews)}: {review_data['title'][:30]}...")
                    except Exception as e:
                        logger.warning(f"⚠️ Errore estrazione recensione: {e}")
                        continue
                        
                # Prova a passare alla pagina successiva
                if len(reviews) < max_reviews and pages_scraped < max_pages:
                    next_success = await self.click_next_page()
                    if not next_success:
                        logger.info("📄 Nessuna pagina successiva, terminazione")
                        break
                        
        except Exception as e:
            logger.error(f"❌ Errore estrazione recensioni: {e}")
            
        logger.info(f"✅ Estratte {len(reviews)} recensioni REALI")
        return reviews
        
    async def extract_single_review(self, review_element) -> Optional[Dict]:
        """Estrae dati da singola recensione REALE"""
        try:
            review_data = {
                "rating": 0,
                "title": "",
                "text": "",
                "author": "",
                "date": "",
                "verified": False,
                "helpful_votes": 0
            }
            
            # Rating
            try:
                rating_el = await review_element.query_selector(".a-icon-star .a-icon-alt, [data-hook='review-star-rating'] .a-icon-alt")
                if rating_el:
                    rating_text = await rating_el.inner_text()
                    import re
                    match = re.search(r'(\d+)', rating_text)
                    if match:
                        review_data["rating"] = int(match.group(1))
            except:
                pass
                
            # Titolo
            try:
                title_el = await review_element.query_selector("[data-hook='review-title'] span, .review-title span")
                if title_el:
                    review_data["title"] = await title_el.inner_text()
            except:
                pass
                
            # Testo recensione
            try:
                text_el = await review_element.query_selector("[data-hook='review-body'] span, .review-text span")
                if text_el:
                    review_data["text"] = await text_el.inner_text()
            except:
                pass
                
            # Autore
            try:
                author_el = await review_element.query_selector("[data-hook='review-author'] span, .a-profile-name")
                if author_el:
                    review_data["author"] = await author_el.inner_text()
            except:
                pass
                
            # Data
            try:
                date_el = await review_element.query_selector("[data-hook='review-date'] span, .review-date span")
                if date_el:
                    review_data["date"] = await date_el.inner_text()
            except:
                pass
                
            # Acquisto verificato
            try:
                verified_el = await review_element.query_selector("[data-hook='avp-badge']")
                review_data["verified"] = verified_el is not None
            except:
                pass
                
            return review_data
            
        except Exception as e:
            logger.warning(f"⚠️ Errore estrazione singola recensione: {e}")
            return None
            
    async def click_next_page(self) -> bool:
        """Click pagina successiva recensioni"""
        assert self.page is not None, "Browser non inizializzato"
        try:
            next_selectors = [
                "li.a-last:not(.a-disabled) a",
                ".a-pagination .a-next:not(.a-disabled)",
                "a[aria-label='Next page']"
            ]
            
            for selector in next_selectors:
                try:
                    next_button = await self.page.wait_for_selector(selector, timeout=5000)
                    if next_button:
                        logger.info("➡️ Click pagina successiva...")
                        await next_button.click()
                        await self.random_delay(3, 5)
                        return True
                except:
                    continue
                    
            return False
            
        except:
            return False

    async def handle_cookie_banner(self):
        """Gestisce automaticamente il banner cookie Amazon"""
        assert self.page is not None, "Browser non inizializzato"
        logger.info("🍪 Gestione automatica cookie banner...")
        
        # Selettori cookie Amazon 2024 (basati su ricerca documentazione)
        cookie_selectors = [
            "#sp-cc-accept",  # Amazon IT principale
            "#a-autoid-0",  # Button generico Amazon
            "[data-testid='cookie-accept']",  # Nuovo formato
            ".a-button[aria-labelledby*='accept']",  # Aria label
            "button[id*='accept']",  # ID contenente accept
            "input[value*='Accept']",  # Input con valore Accept
            "#cookieAcceptSubmit",  # Amazon US/Global
        ]
        
        try:
            # Aspetta un momento per il caricamento del banner
            await self.random_delay(1, 2)
            
            for i, selector in enumerate(cookie_selectors, 1):
                try:
                    logger.debug(f"🔍 Tentativo {i}: selector '{selector}'")
                    
                    # Prova a trovare l'elemento cookie
                    cookie_element = await self.page.wait_for_selector(selector, timeout=3000)
                    
                    if cookie_element:
                        # Verifica che sia visibile
                        is_visible = await cookie_element.is_visible()
                        if is_visible:
                            logger.info(f"✅ Cookie banner trovato: {selector}")
                            await cookie_element.click()
                            logger.info("🍪 Cookie banner accettato automaticamente")
                            
                            # Aspetta che il banner scompaia
                            await self.random_delay(1, 2)
                            return True
                        
                except Exception as e:
                    logger.debug(f"⚠️ Selector {selector} fallito: {e}")
                    continue
            
            logger.info("ℹ️ Nessun cookie banner trovato (già gestito o non presente)")
            return True  # Non è un errore se non c'è banner
            
        except Exception as e:
            logger.warning(f"⚠️ Errore gestione cookie banner: {e}")
            return False


async def test_real_scraper():
    """Test scraper REALE con ASIN vero"""
    logger.info("🧪 Test Amazon Scraper REALE")
    
    # ASIN reale per test (Echo Dot)
    test_asin = "B0BC16S6RN"
    
    # Credenziali per login (lascia vuote per test senza login)
    login_credentials = {
        'email': '',  # Inserisci la tua email Amazon
        'password': '',  # Inserisci la tua password Amazon
        # 'totp_secret': '',  # Opzionale: secret TOTP per 2FA automatico
    }
    
    logger.info("⚙️ Configurazione scraper:")
    logger.info(f"   🔍 ASIN test: {test_asin}")
    logger.info(f"   👀 Browser visibile: True")
    logger.info(f"   🔐 Login: {'Configurato' if login_credentials.get('email') else 'Non configurato'}")
    
    try:
        async with AmazonScraperReal(
            headless=False,  # Browser visibile per debug
            login_credentials=login_credentials
        ) as scraper:
            logger.info(f"🎯 Test scraping REALE per ASIN: {test_asin}")
            
            # Navigazione
            nav_success = await scraper.navigate_to_product(test_asin)
            if not nav_success:
                logger.error("❌ Navigazione fallita")
                return False
                
            # Estrazione dati prodotto
            product_info = await scraper.extract_product_info(test_asin)
            logger.info(f"📦 Prodotto: {product_info['title'][:50]}...")
            logger.info(f"⭐ Rating: {product_info['rating_average']}")
            logger.info(f"📊 Recensioni: {product_info['total_reviews']}")
            
            # Navigazione recensioni
            reviews_nav = await scraper.navigate_to_reviews()
            if not reviews_nav:
                logger.warning("⚠️ Sezione recensioni non accessibile")
                # Continua comunque con i dati prodotto
                
            # Estrazione recensioni (se possibile)
            reviews = []
            if reviews_nav:
                reviews = await scraper.extract_reviews(max_reviews=10)
                
            # Crea oggetto risultato
            result = ProductData(
                asin=test_asin,
                title=product_info["title"],
                rating_average=product_info["rating_average"],
                total_reviews=product_info["total_reviews"],
                price=product_info.get("price"),
                reviews=[ReviewData(**review) for review in reviews if review]
            )
            
            logger.info("✅ Scraper REALE completato!")
            logger.info(f"📊 Dati estratti: {len(result.reviews)} recensioni")
            
            return result
            
    except Exception as e:
        logger.error(f"❌ Test scraper REALE fallito: {e}")
        return None


async def main():
    """Main test function"""
    setup_logging()
    
    logger.info("🚀 === AMAZON SCRAPER REALE - NO MOCK ===")
    logger.info("🎯 Test con dati veri di Amazon")
    logger.info("=" * 50)
    
    start_time = datetime.now()
    result = await test_real_scraper()
    end_time = datetime.now()
    
    duration = (end_time - start_time).total_seconds()
    
    if result:
        logger.info("=" * 50)
        logger.info("📋 === RISULTATI SCRAPING REALE ===")
        logger.info(f"⏱️ Tempo totale: {duration:.2f}s")
        logger.info(f"🎯 ASIN: {result.asin}")
        logger.info(f"📦 Prodotto: {result.title[:80]}...")
        logger.info(f"⭐ Rating medio: {result.rating_average}")
        logger.info(f"📊 Totale recensioni: {result.total_reviews}")
        logger.info(f"📝 Recensioni estratte: {len(result.reviews)}")
        
        if result.reviews:
            logger.info("📋 Prime 3 recensioni:")
            for i, review in enumerate(result.reviews[:3], 1):
                logger.info(f"  {i}. [{review.rating}⭐] {review.title[:40]}...")
                logger.info(f"     {review.text[:80]}...")
                
        logger.info("🎉 SCRAPER REALE FUNZIONANTE!")
        return True
    else:
        logger.error("💥 SCRAPER REALE FALLITO")
        return False


if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)
