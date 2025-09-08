import asyncio
import json
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, PlainTextResponse
from sse_starlette.sse import EventSourceResponse

from .jobs import job_manager
from .utils import report_json_to_txt
from .supabase_upload import upload_csv_and_record
import os
from datetime import datetime
try:
    from importlib.metadata import version as _pkg_version  # Python 3.8+
except Exception:  # pragma: no cover
    _pkg_version = None

app = FastAPI(title="Amazon Review Agent API")

# CORS per frontend su localhost:3000
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/where")
async def where():
    import inspect
    return {"file": inspect.getsourcefile(where)}


@app.post("/jobs")
async def create_job(
    asins: str,
    headless: bool = True,
    max_reviews: int = 15,
    delay_between_asins: float = 0.0,
    user_id: Optional[str] = None,
    user_email: Optional[str] = None,
):
    asin_list: List[str] = [a.strip() for a in asins.split(",") if a.strip()]
    if not asin_list:
        raise HTTPException(status_code=400, detail="Lista ASIN vuota")
    # Enforce headless in production or when FORCE_HEADLESS=true
    env = (os.getenv("ENV", "") or os.getenv("APP_ENV", "")).lower()
    force_headless = os.getenv("FORCE_HEADLESS", "").lower() == "true" or env == "production"
    effective_headless = True if force_headless else headless

    # Debug log for diagnostics
    try:
        print(f"[create_job] asins={asin_list} user_id={user_id} user_email={user_email} headless={effective_headless} max_reviews={max_reviews}")
    except Exception:
        pass

    job = job_manager.create(asin_list, {
        "headless": effective_headless,
        "max_reviews": max_reviews,
        "delay_between_asins": delay_between_asins,
        "user_id": user_id,
        "user_email": user_email,
    })
    job.task = asyncio.create_task(job_manager.run_job(job))
    return {"job_id": job.id}


@app.get("/jobs/{job_id}/events")
async def stream_events(job_id: str):
    job = job_manager.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job non trovato")

    async def event_generator():
        while True:
            event = await job.queue.get()
            if event.get("step") == "_stream_end":
                yield {"event": "end", "data": json.dumps({"job_id": job_id})}
                break
            yield {"event": "message", "data": json.dumps(event)}

    return EventSourceResponse(event_generator())


@app.get("/jobs/{job_id}/report", response_class=PlainTextResponse)
async def get_report_txt(job_id: str, asin: Optional[str] = None, format: str = Query("txt")):
    job = job_manager.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job non trovato")
    if job.status not in ("done", "error"):
        raise HTTPException(status_code=409, detail="Job non terminato")

    import glob
    import os
    import json as pyjson

    if format == "txt":
        # Cerca il report più recente per ASIN
        if not asin:
            raise HTTPException(status_code=400, detail="Parametro asin richiesto per format=txt")
        paths = sorted(glob.glob(f"output/batch_report_{asin}_*.json"))
        if not paths:
            raise HTTPException(status_code=404, detail="Report non trovato per questo ASIN")
        path = paths[-1]
        with open(path, "r", encoding="utf-8") as f:
            data = pyjson.load(f)
        txt = report_json_to_txt(data)
        return PlainTextResponse(txt, headers={"Content-Disposition": f"attachment; filename=report_{asin}.txt"})
    else:
        raise HTTPException(status_code=400, detail="Formato non supportato")


@app.get("/healthz")
async def health():
    # Enrich health with versions and LLM info to avoid extra endpoint
    try:
        from importlib.metadata import version as _ver
    except Exception:
        _ver = None
    def pv(n: str):
        try:
            return _ver(n) if _ver else None
        except Exception:
            return None
    provider = (os.getenv("LLM_PROVIDER", "ollama") or "ollama").lower()
    model = os.getenv("OPENAI_MODEL") if provider == "openai" else os.getenv("OLLAMA_MODEL")
    return {
        "ok": True,
        "versions": {
            "langchain": pv("langchain"),
            "langgraph": pv("langgraph"),
        },
        "llm": {"provider": provider, "model": model},
    }


@app.delete("/jobs/{job_id}")
async def cancel_job(job_id: str):
    ok = await job_manager.cancel(job_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Job non trovato")
    return {"cancelled": True}


@app.post("/debug/supabase-upload")
async def debug_supabase_upload(user_id: Optional[str] = None, user_email: Optional[str] = None):
    """Uploads a tiny CSV to Supabase Storage and inserts recent_reports (if user available)."""
    try:
        import io, csv, time
        job_id = f"job_debug_{int(time.time())}"
        sio = io.StringIO()
        w = csv.writer(sio)
        w.writerow(["asin","title","rating_average","total_reviews","reviews_extracted","success","errors"])
        w.writerow(["TESTASIN","Debug Item",4.5,100,10,True,""])
        path = upload_csv_and_record(
            user_id=user_id,
            job_id=job_id,
            csv_bytes=sio.getvalue().encode("utf-8"),
            filename=f"summary_{job_id}.csv",
            asins=["TESTASIN"],
            meta={"user_email": user_email or None, "batch_id": job_id, "count": 1},
        )
        return {"ok": bool(path), "path": path}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/status")
@app.get("/api/status")
async def get_status():
    def pkg_ver(name: str):
        try:
            if _pkg_version:
                return _pkg_version(name)
        except Exception:
            return None
        return None

    provider = (os.getenv("LLM_PROVIDER", "ollama") or "ollama").lower()
    if provider == "openai":
        model = os.getenv("OPENAI_MODEL", os.getenv("OPENAI_DEFAULT_MODEL", "gpt-4o-mini"))
    else:
        model = os.getenv("OLLAMA_MODEL", "llama3.1:8b")

    return {
        "backend": "ok",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "versions": {
            "langchain": pkg_ver("langchain"),
            "langgraph": pkg_ver("langgraph"),
        },
        "llm": {
            "provider": provider,
            "model": model,
        },
    }
