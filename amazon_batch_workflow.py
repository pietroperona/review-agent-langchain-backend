"""
Amazon Batch Workflow - Multi-ASIN con riuso sessione Playwright

Obiettivo: una singola sessione/browser, login una volta, iterazione ASIN,
analisi LLM e persistenza risultati, senza Google Sheets.
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

# LangSmith di default (non blocca se non configurato)
os.environ.setdefault("LANGCHAIN_TRACING_V2", "true")
os.environ.setdefault("LANGCHAIN_PROJECT", "amazon-batch-workflow")
os.environ.setdefault("LANGCHAIN_CALLBACKS_BACKGROUND", "true")

from langgraph.graph import StateGraph, END

from scraper_real import AmazonScraperReal
from amazon_credentials import AMAZON_CREDENTIALS
from amazon_hybrid_workflow import AmazonHybridNodes

# Fix event loop nested (utile in ambienti interattivi)
import nest_asyncio
nest_asyncio.apply()

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-8s | %(message)s')
logger = logging.getLogger(__name__)


class BatchState(TypedDict):
    """Stato per elaborazione batch multi‑ASIN"""
    # Input batch
    asins: List[str]
    i: int
    session_id: str

    # Session/auth
    auth_ok: bool

    # Corrente
    current_asin: Optional[str]
    scraped_data: Optional[Dict[str, Any]]
    scraping_success: bool
    scraping_errors: List[str]

    # Analisi LLM
    sentiment_analysis: Optional[Dict[str, Any]]
    theme_analysis: Optional[Dict[str, Any]]
    sentiment_duration: Optional[float]
    theme_duration: Optional[float]

    # Report corrente e cumulativo
    final_report: Optional[Dict[str, Any]]
    results: List[Dict[str, Any]]
    txt_paths: Dict[str, str]

    # Controllo flusso
    retries: int
    last_error: Optional[str]


class SessionManager:
    """Gestisce istanze AmazonScraperReal per sessione logica"""
    _sessions: Dict[str, AmazonScraperReal] = {}

    @classmethod
    async def get(cls, session_id: str) -> AmazonScraperReal:
        if session_id in cls._sessions:
            return cls._sessions[session_id]

        scraper = AmazonScraperReal(
            headless=(os.getenv("HEADLESS", "false").lower() == "true"),
            login_credentials=AMAZON_CREDENTIALS,
        )
        await scraper.start_browser()
        cls._sessions[session_id] = scraper
        return scraper

    @classmethod
    async def close(cls, session_id: str) -> None:
        scraper = cls._sessions.pop(session_id, None)
        if scraper:
            try:
                await scraper.close_browser()
            except Exception:
                pass

    @classmethod
    async def close_all(cls) -> None:
        keys = list(cls._sessions.keys())
        for sid in keys:
            try:
                await cls.close(sid)
            except Exception:
                pass


from typing import Callable, Awaitable


class AmazonBatchWorkflow:
    """Workflow LangGraph per processare più ASIN con una sola sessione"""

    def __init__(
        self,
        max_reviews: int = 15,
        delay_between_asins: float = 0.0,
        emitter: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
        job_id: Optional[str] = None,
        user_id: Optional[str] = None,
        user_email: Optional[str] = None,
    ):
        self.nodes = AmazonHybridNodes()  # riusa LLM e report node esistenti
        self.max_reviews = max_reviews
        # Delay opzionale tra ASIN per simulare comportamento umano
        try:
            self.delay_between_asins = float(os.getenv("DELAY_BETWEEN_ASINS", str(delay_between_asins)))
        except Exception:
            self.delay_between_asins = delay_between_asins
        self._emitter = emitter
        self._job_id = job_id or f"batch_{uuid.uuid4().hex[:8]}"
        self._user_id = user_id
        self._user_email = user_email

    async def emit(self, event: Dict[str, Any]):
        if self._emitter is not None:
            try:
                await self._emitter(event)
            except Exception:
                pass

    # === NODI ===
    async def open_session_and_login(self, state: BatchState) -> Dict[str, Any]:
        logger.info("🔐 [SESSION] Apertura sessione e login (una sola volta)")
        await self.emit({"step": "start_agent", "status": "running"})
        session_id = state.get("session_id") or f"sess_{uuid.uuid4().hex[:8]}"
        scraper = await SessionManager.get(session_id)

        auth_ok = False
        try:
            await self.emit({"step": "login", "status": "running"})
            auth_ok = await scraper.perform_login()
        except Exception as e:
            logger.warning(f"⚠️ [SESSION] Login fallito: {e}")

        await self.emit({"step": "login", "status": "done", "ok": bool(auth_ok)})
        # Chiudi lo step di avvio una volta completata la fase iniziale
        await self.emit({"step": "start_agent", "status": "done"})
        return {"session_id": session_id, "auth_ok": bool(auth_ok)}

    async def _detect_auth_block(self, scraper: AmazonScraperReal) -> bool:
        try:
            login_form = await scraper.page.query_selector("#ap_email, #signInSubmit")
            url = scraper.page.url.lower()
            content = (await scraper.page.content())[:2000].lower()
            if login_form:
                return True
            if "captcha" in url or "captcha" in content:
                return True
            if any(err in content for err in ["access denied", "403", "we're sorry"]):
                return True
        except Exception:
            pass
        return False

    async def _goto_product_with_retries(self, scraper: AmazonScraperReal, asin: str, max_attempts: int = 3) -> bool:
        from asyncio import sleep
        urls = [
            f"https://www.amazon.it/dp/{asin}",
            f"https://www.amazon.it/gp/product/{asin}",
        ]
        backoff = 2.0
        for attempt in range(1, max_attempts + 1):
            for url in urls:
                try:
                    await scraper.page.goto(url, wait_until="domcontentloaded", timeout=90000)
                    try:
                        await scraper.page.wait_for_selector("#productTitle, #dp-container", timeout=15000)
                        return True
                    except Exception:
                        pass
                except Exception:
                    pass
            await sleep(backoff)
            backoff *= 1.8
        return False

    async def pick_next_asin(self, state: BatchState) -> Dict[str, Any]:
        i = state.get("i", 0)
        asins = state.get("asins", [])
        if i >= len(asins):
            logger.info("✅ [BATCH] Tutti gli ASIN sono stati processati")
            return {}

        current = asins[i]
        logger.info(f"🎯 [BATCH] Prossimo ASIN: {current} ({i+1}/{len(asins)})")
        await self.emit({"step": "navigate_product", "asin": current, "status": "running"})
        return {
            "current_asin": current,
            "scraped_data": {},
            "scraping_success": False,
            "scraping_errors": [],
            "sentiment_analysis": None,
            "theme_analysis": None,
            "final_report": None,
            "retries": 0,
            "last_error": None,
        }

    async def scrape_product(self, state: BatchState) -> Dict[str, Any]:
        asin = state.get("current_asin")
        assert asin, "ASIN corrente non impostato"
        session_id = state["session_id"]
        scraper = await SessionManager.get(session_id)

        logger.info(f"🕷️ [SCRAPE] Estrazione prodotto: {asin}")
        await self.emit({"step": "scrape_reviews", "asin": asin, "status": "running"})
        from datetime import datetime as _dt
        _t0 = _dt.now()

        try:
            # Cookie banner: gestiscilo solo la prima volta in sessione
            try:
                if not getattr(scraper, "_cookie_handled", False):
                    await scraper.handle_cookie_banner()
                    setattr(scraper, "_cookie_handled", True)
            except Exception:
                pass

            # Naviga con retry/backoff e fallback URL
            ok = await self._goto_product_with_retries(scraper, asin, max_attempts=3)
            if not ok:
                # Se bloccati, re-login e un retry extra
                if await self._detect_auth_block(scraper):
                    logger.info("🔁 [SCRAPE] Blocco rilevato → re-login e retry")
                    try:
                        await scraper.perform_login()
                    except Exception:
                        pass
                    ok = await self._goto_product_with_retries(scraper, asin, max_attempts=2)
            if not ok:
                await self.emit({"step": "navigate_product", "asin": asin, "status": "error"})
                return {
                    "scraped_data": {},
                    "scraping_success": False,
                    "scraping_errors": ["Product navigation failed (after retries)"],
                }
            else:
                await self.emit({"step": "navigate_product", "asin": asin, "status": "done"})

            product_info = await scraper.extract_product_info(asin)
            scraped_data: Dict[str, Any] = {
                "asin": asin,
                "title": product_info.get("title", "N/A"),
                "rating_average": product_info.get("rating_average", 0.0),
                "total_reviews": product_info.get("total_reviews", 0),
                "price": product_info.get("price"),
                "reviews": [],
                "scraped_at": datetime.now().isoformat(),
                "authenticated": state.get("auth_ok", False),
            }

            # Recensioni (best-effort)
            reviews = []
            try:
                if await scraper.navigate_to_reviews():
                    reviews = await scraper.extract_reviews(max_reviews=self.max_reviews)
            except Exception as e:
                logger.warning(f"⚠️ [SCRAPE] Errore durante estrazione recensioni: {e}")

            scraped_data["reviews"] = reviews

            has_core = scraped_data.get("title") != "N/A" and (scraped_data.get("rating_average") or 0) > 0
            logger.info(
                f"✅ [SCRAPE] Core={'OK' if has_core else 'FAIL'} | Reviews={len(reviews)} | Rating={scraped_data.get('rating_average', 0)}"
            )

            result = {
                "scraped_data": scraped_data,
                "scraping_success": bool(has_core),
                "scraping_errors": [] if has_core else ["Core product data missing"],
                "retries": state.get("retries", 0) if has_core else (state.get("retries", 0) + 1),
                "scraping_duration": (_dt.now() - _t0).total_seconds(),
            }
            await self.emit({
                "step": "scrape_reviews",
                "asin": asin,
                "status": "done" if has_core else "error",
                "reviews_extracted": len(reviews),
            })
            return result

        except Exception as e:
            logger.error(f"❌ [SCRAPE] Errore: {e}")
            await self.emit({"step": "scrape_reviews", "asin": asin, "status": "error", "error": str(e)})
            return {
                "scraped_data": {},
                "scraping_success": False,
                "scraping_errors": [str(e)],
                "last_error": str(e),
                "retries": state.get("retries", 0) + 1,
                "scraping_duration": (_dt.now() - _t0).total_seconds(),
            }

    # Deleghe a nodi esistenti per LLM e report
    async def sentiment_node(self, state: BatchState) -> Dict[str, Any]:
        await self.emit({"step": "llm_sentiment", "asin": state.get("current_asin"), "status": "running"})
        res = await self.nodes.sentiment_analysis_node(state)  # type: ignore[arg-type]
        await self.emit({"step": "llm_sentiment", "asin": state.get("current_asin"), "status": "done"})
        return res

    async def theme_node(self, state: BatchState) -> Dict[str, Any]:
        await self.emit({"step": "llm_themes", "asin": state.get("current_asin"), "status": "running"})
        res = await self.nodes.theme_analysis_node(state)  # type: ignore[arg-type]
        await self.emit({"step": "llm_themes", "asin": state.get("current_asin"), "status": "done"})
        return res

    async def build_report(self, state: BatchState) -> Dict[str, Any]:
        # Adatta lo state batch allo schema del report ibrido
        shim_state = {
            "asin": state.get("current_asin"),
            "authentication_success": state.get("auth_ok", False),
            "authentication_duration": state.get("authentication_duration", 0),
            "scraped_data": state.get("scraped_data", {}),
            "scraping_success": state.get("scraping_success", False),
            "scraping_duration": state.get("scraping_duration", 0),
            "sentiment_analysis": state.get("sentiment_analysis", {}) or {},
            "sentiment_duration": state.get("sentiment_duration", 0),
            "theme_analysis": state.get("theme_analysis", {}) or {},
            "theme_duration": state.get("theme_duration", 0),
            "analysis_errors": state.get("analysis_errors", []) or [],
            "scraping_errors": state.get("scraping_errors", []) or [],
        }
        res = await self.nodes.report_generation_node(shim_state)  # type: ignore[arg-type]
        await self.emit({"step": "build_report", "asin": state.get("current_asin"), "status": "done"})
        return res

    async def persist_result(self, state: BatchState) -> Dict[str, Any]:
        asin = state.get("current_asin") or state.get("scraped_data", {}).get("asin")
        report = state.get("final_report") or {}
        # Accumula entry anche in caso di fallimento scraping (senza report)
        if not asin:
            return {}

        path = None
        if report:
            os.makedirs("output", exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            path = f"output/batch_report_{asin}_{timestamp}.json"
            try:
                import json
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(report, f, indent=2, ensure_ascii=False)
                logger.info(f"📁 [SAVE] Report salvato: {path}")
                await self.emit({"step": "report_saved", "asin": asin, "status": "done", "path": path})
                # Generate TXT and upload to Supabase Storage
                try:
                    from api.utils import report_json_to_txt  # type: ignore
                    from api.supabase_upload import upload_file  # type: ignore
                    txt = report_json_to_txt(report)
                    txt_name = f"report_{asin}.txt"
                    storage_path = upload_file(
                        user_id=self._user_id,
                        job_id=self._job_id,
                        file_bytes=txt.encode("utf-8"),
                        filename=txt_name,
                        content_type="text/plain; charset=utf-8",
                    )
                    if storage_path:
                        logger.info(f"☁️  [SAVE] TXT caricato: {storage_path}")
                        current_txt = dict(state.get("txt_paths") or {})
                        current_txt[str(asin)] = storage_path
                        # Do NOT return early; allow results to be appended below
                        state["txt_paths"] = current_txt  # type: ignore[index]
                except Exception as e:
                    logger.warning(f"⚠️  [SAVE] TXT upload skipped: {e}")
            except Exception as e:
                logger.error(f"❌ [SAVE] Errore salvataggio report: {e}")
                path = None

        # Accumula sommario risultati
        product = report.get("product_data", {}) if report else {}
        entry = {
            "asin": asin,
            "title": product.get("title", "N/A"),
            "rating_average": product.get("rating_average", 0.0),
            "total_reviews": product.get("total_reviews", 0),
            "reviews_extracted": product.get("reviews_extracted", 0),
            "path": path,
            "success": bool(state.get("scraping_success", False)),
            "errors": list(state.get("scraping_errors") or []),
        }

        current_results = list(state.get("results") or [])
        current_results.append(entry)
        # Include any updated txt_paths from above in the return
        updated = {"results": current_results}
        try:
            if state.get("txt_paths"):
                updated["txt_paths"] = state.get("txt_paths")  # type: ignore[assignment]
        except Exception:
            pass
        return updated

    async def advance_index(self, state: BatchState) -> Dict[str, Any]:
        # Delay opzionale tra ASIN per comportamento umano
        if self.delay_between_asins and self.delay_between_asins > 0:
            try:
                await asyncio.sleep(self.delay_between_asins)
            except Exception:
                pass

        return {"i": state.get("i", 0) + 1}

    async def finalize(self, state: BatchState) -> Dict[str, Any]:
        session_id = state.get("session_id")
        if session_id:
            await SessionManager.close(session_id)

        # Genera sommario batch
        os.makedirs("output", exist_ok=True)
        batch_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        summary_path = f"output/batch_summary_{batch_id}.json"
        try:
            import json
            summary = {
                "batch_id": batch_id,
                "timestamp": datetime.now().isoformat(),
                "count": len(state.get("results") or []),
                "results": state.get("results") or [],
            }
            with open(summary_path, "w", encoding="utf-8") as f:
                json.dump(summary, f, indent=2, ensure_ascii=False)
            logger.info(f"📦 [BATCH] Sommario salvato: {summary_path}")
            await self.emit({"step": "done", "status": "done", "summary_path": summary_path})
        except Exception as e:
            logger.error(f"❌ [BATCH] Errore salvataggio sommario: {e}")

        # Build and upload a single CSV to Supabase (best effort)
        try:
            import csv, io
            from api import supabase_upload  # local module

            results = list(state.get("results") or [])
            if results:
                sio = io.StringIO()
                w = csv.writer(sio)
                w.writerow(["asin", "title", "rating_average", "total_reviews", "reviews_extracted", "success", "errors"])
                for r in results:
                    w.writerow([
                        r.get("asin", ""),
                        r.get("title", ""),
                        r.get("rating_average", 0.0),
                        r.get("total_reviews", 0),
                        r.get("reviews_extracted", 0),
                        bool(r.get("success", False)),
                        "; ".join(r.get("errors", []) or []),
                    ])
                csv_bytes = sio.getvalue().encode("utf-8")
                filename = f"summary_{batch_id}.csv"
                # include txt paths in meta so frontend can present TXT links
                meta = {"batch_id": batch_id, "count": len(results), "user_email": self._user_email}
                try:
                    txt_paths = state.get("txt_paths") or {}
                    if txt_paths:
                        meta["txt_paths"] = txt_paths
                        meta["asins"] = list(txt_paths.keys())
                    else:
                        meta["asins"] = [r.get("asin", "") for r in results if r.get("asin")]
                except Exception:
                    meta["asins"] = [r.get("asin", "") for r in results if r.get("asin")]

                path = supabase_upload.upload_csv_and_record(
                    user_id=self._user_id,
                    job_id=self._job_id,
                    csv_bytes=csv_bytes,
                    filename=filename,
                    asins=[r.get("asin", "") for r in results if r.get("asin")],
                    meta=meta,
                )
                if path:
                    logger.info(f"☁️  [BATCH] CSV caricato su Supabase: {path}")
                else:
                    logger.warning("⚠️  [BATCH] Upload CSV su Supabase non eseguito (verifica env/policy)")
        except Exception as e:
            logger.error(f"❌ [BATCH] Errore upload CSV Supabase: {e}")

        return {}

    # === GRAFO ===
    def build_graph(self) -> StateGraph:
        graph = StateGraph(BatchState)

        graph.add_node("open_session_and_login", self.open_session_and_login)
        graph.add_node("pick_next_asin", self.pick_next_asin)
        graph.add_node("scrape_product", self.scrape_product)
        graph.add_node("sentiment_analysis", self.sentiment_node)
        graph.add_node("theme_analysis", self.theme_node)
        graph.add_node("build_report", self.build_report)
        graph.add_node("persist_result", self.persist_result)
        graph.add_node("advance_index", self.advance_index)
        graph.add_node("finalize", self.finalize)

        graph.set_entry_point("open_session_and_login")
        graph.add_edge("open_session_and_login", "pick_next_asin")

        def pick_router(state: BatchState) -> str:
            i = state.get("i", 0)
            asins = state.get("asins", [])
            return "finalize" if i >= len(asins) else "scrape_product"

        graph.add_conditional_edges(
            "pick_next_asin",
            pick_router,
            {"scrape_product": "scrape_product", "finalize": "finalize"},
        )

        def scrape_router(state: BatchState) -> str:
            if not state.get("scraping_success"):
                # Retry semplice una volta
                if (state.get("retries", 0) or 0) < 1:
                    return "scrape_product"
                else:
                    # Salta analisi se scraping fallito anche al retry
                    return "persist_result"
            return "sentiment_analysis"

        graph.add_conditional_edges(
            "scrape_product",
            scrape_router,
            {
                "scrape_product": "scrape_product",
                "sentiment_analysis": "sentiment_analysis",
                "persist_result": "persist_result",
            },
        )

        graph.add_edge("sentiment_analysis", "theme_analysis")
        graph.add_edge("theme_analysis", "build_report")
        graph.add_edge("build_report", "persist_result")
        graph.add_edge("persist_result", "advance_index")
        graph.add_edge("advance_index", "pick_next_asin")

        return graph.compile()

    async def run(self, asins: List[str]) -> Dict[str, Any]:
        if not asins:
            raise ValueError("Lista ASIN vuota")

        app = self.build_graph()

        # Stato iniziale
        initial_state: BatchState = {
            "asins": asins,
            "i": 0,
            "session_id": f"sess_{uuid.uuid4().hex[:8]}",
            "auth_ok": False,
            "current_asin": None,
            "scraped_data": {},
            "scraping_success": False,
            "scraping_errors": [],
            "sentiment_analysis": None,
            "theme_analysis": None,
            "final_report": None,
            "results": [],
            "txt_paths": {},
            "retries": 0,
            "last_error": None,
        }

        # Config per tracing/coerenza
        run_id = f"batch_{uuid.uuid4().hex[:8]}"
        config = {
            "configurable": {"thread_id": run_id},
            "recursion_limit": 100,
            "tags": ["amazon", "batch", "hybrid", "playwright", "langgraph"],
        }

        logger.info(f"🚀 [BATCH] Avvio batch: {', '.join(asins)}")
        final_state = await app.ainvoke(initial_state, config=config)
        logger.info("🏁 [BATCH] Completato")
        return final_state


async def _main_cli():
    import argparse

    parser = argparse.ArgumentParser(description="Amazon Batch Workflow")
    parser.add_argument("--asins", type=str, help="Lista ASIN separati da virgola", required=False)
    parser.add_argument("--max-reviews", type=int, default=int(os.getenv("MAX_REVIEWS", "15")))
    parser.add_argument("--delay-between-asins", type=float, default=float(os.getenv("DELAY_BETWEEN_ASINS", "0")))
    args = parser.parse_args()

    if args.asins:
        asins = [a.strip() for a in args.asins.split(",") if a.strip()]
    else:
        # fallback a due esempi comuni
        asins = ["B08N5WRWNW", "B07XJ8C8F5"]

    wf = AmazonBatchWorkflow(max_reviews=args.max_reviews, delay_between_asins=args.delay_between_asins)
    await wf.run(asins)


if __name__ == "__main__":
    try:
        asyncio.run(_main_cli())
    except KeyboardInterrupt:
        pass
