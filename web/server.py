import asyncio
import json
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, PlainTextResponse
from sse_starlette.sse import EventSourceResponse

from .jobs import job_manager
from .utils import report_json_to_txt

app = FastAPI(title="Amazon Review Agent API")

# CORS per frontend su localhost:3000
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/jobs")
async def create_job(
    asins: str,
    headless: bool = True,
    max_reviews: int = 15,
    delay_between_asins: float = 0.0,
):
    asin_list: List[str] = [a.strip() for a in asins.split(",") if a.strip()]
    if not asin_list:
        raise HTTPException(status_code=400, detail="Lista ASIN vuota")
    job = job_manager.create(asin_list, {
        "headless": headless,
        "max_reviews": max_reviews,
        "delay_between_asins": delay_between_asins,
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
    return {"ok": True}


@app.delete("/jobs/{job_id}")
async def cancel_job(job_id: str):
    ok = await job_manager.cancel(job_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Job non trovato")
    return {"cancelled": True}
