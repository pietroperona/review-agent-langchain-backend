# Review Agent Backend (FastAPI + LangGraph)

Backend API for Amazon review analysis. Orchestrates Playwright scraping and LLM analysis via LangGraph and streams job status via Server‑Sent Events (SSE).

## Features
- Single browser session reused per batch (one login)
- Robust scraping with retry/backoff and URL fallback
- SSE progress events (start_agent, login, navigate_product, scrape_reviews, llm_sentiment, llm_themes, build_report, report_saved, done)
- Report JSON per ASIN and batch summary

## Requirements
- Python 3.9+
- Playwright Chromium
- OpenAI API key (or Ollama local)

## Setup
```bash
pip install -r requirements.txt
python -m playwright install chromium
```

Create a `.env` (optional) from `.env.example` or export variables:
- `AMAZON_EMAIL`, `AMAZON_PASSWORD`, `AMAZON_TOTP_SECRET`
- `LLM_PROVIDER` = `openai` | `ollama`
- `OPENAI_API_KEY` if `openai`
- `HEADLESS`, `DELAY_BETWEEN_ASINS`, `MAX_REVIEWS`

## Run API
```bash
uvicorn web.server:app --host 0.0.0.0 --port 8000
```

## API
- `POST /jobs?asins=B0...,B0...&headless=true&max_reviews=5&delay_between_asins=2` → `{ job_id }`
- `GET /jobs/{job_id}/events` (SSE) → live progress
- `GET /jobs/{job_id}/report?asin=B0...&format=txt` → TXT report download
- `DELETE /jobs/{job_id}` → cancel job

## Deploy (DigitalOcean App Platform)
- Build Command:
  ```
  pip install -r requirements.txt && \
  python -m playwright install chromium && \
  python -m playwright install-deps chromium
  ```
- Run Command:
  ```
  uvicorn web.server:app --host 0.0.0.0 --port $PORT
  ```
- Set env vars as above. Ensure CORS `allow_origins` in `web/server.py` includes your frontend domain.
