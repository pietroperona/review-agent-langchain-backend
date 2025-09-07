"""
Amazon Parallel Batch Workflow (opzionale)

Esegue più ASIN in parallelo con limiti di concorrenza, creando
una sessione Playwright indipendente per ciascun worker. Mantiene
amazon_batch_workflow.py come percorso sequenziale a sessione unica.
"""

import asyncio
from typing import List, Dict, Any
import os
from datetime import datetime

from amazon_batch_workflow import AmazonBatchWorkflow


class AmazonParallelBatchWorkflow:
    def __init__(self, max_parallel: int = 2, max_reviews: int = 15, delay_between_asins: float = 0.0):
        self.max_parallel = max_parallel
        self.max_reviews = max_reviews
        self.delay_between_asins = delay_between_asins

    async def _run_single(self, asin: str, sem: asyncio.Semaphore) -> Dict[str, Any]:
        async with sem:
            wf = AmazonBatchWorkflow(max_reviews=self.max_reviews, delay_between_asins=self.delay_between_asins)
            return await wf.run([asin])

    async def run(self, asins: List[str]) -> Dict[str, Any]:
        if not asins:
            raise ValueError("Lista ASIN vuota")
        sem = asyncio.Semaphore(self.max_parallel)
        tasks = [self._run_single(a, sem) for a in asins]
        results = await asyncio.gather(*tasks, return_exceptions=False)

        # Aggrega un sommario batch
        import json
        os.makedirs("output", exist_ok=True)
        batch_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        summary_path = f"output/parallel_batch_summary_{batch_id}.json"
        flattened: List[Dict[str, Any]] = []
        for st in results:
            flattened.extend(st.get("results", []))
        summary = {"batch_id": batch_id, "parallel": True, "results": flattened}
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        return summary


async def _main_cli():
    import argparse
    parser = argparse.ArgumentParser(description="Amazon Parallel Batch Workflow")
    parser.add_argument("--asins", type=str, required=True, help="Lista ASIN separati da virgola")
    parser.add_argument("--max-parallel", type=int, default=int(os.getenv("MAX_PARALLEL", "2")))
    parser.add_argument("--max-reviews", type=int, default=int(os.getenv("MAX_REVIEWS", "15")))
    parser.add_argument("--delay-between-asins", type=float, default=float(os.getenv("DELAY_BETWEEN_ASINS", "0")))
    args = parser.parse_args()

    asins = [a.strip() for a in args.asins.split(",") if a.strip()]
    wf = AmazonParallelBatchWorkflow(args.max_parallel, args.max_reviews, args.delay_between_asins)
    await wf.run(asins)


if __name__ == "__main__":
    asyncio.run(_main_cli())

