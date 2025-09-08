import asyncio
import uuid
from typing import Dict, Any, List, Optional

from amazon_batch_workflow import AmazonBatchWorkflow, SessionManager


class Job:
    def __init__(self, asins: List[str], options: Dict[str, Any]):
        self.id = f"job_{uuid.uuid4().hex[:8]}"
        self.queue: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue()
        self.asins = asins
        self.options = options
        self.task: Optional[asyncio.Task] = None
        self.results: List[Dict[str, Any]] = []
        self.status: str = "pending"

    async def emitter(self, event: Dict[str, Any]):
        event = {"job_id": self.id, **event}
        await self.queue.put(event)


class JobManager:
    def __init__(self):
        self.jobs: Dict[str, Job] = {}

    def create(self, asins: List[str], options: Dict[str, Any]) -> Job:
        job = Job(asins, options)
        self.jobs[job.id] = job
        return job

    def get(self, job_id: str) -> Optional[Job]:
        return self.jobs.get(job_id)

    async def run_job(self, job: Job):
        job.status = "running"
        # Passa emitter al workflow
        try:
            print(f"[JobManager] run_job id={job.id} options={job.options}")
        except Exception:
            pass
        wf = AmazonBatchWorkflow(
            max_reviews=job.options.get("max_reviews", 15),
            delay_between_asins=job.options.get("delay_between_asins", 0.0),
            emitter=job.emitter,
            job_id=job.id,
            user_id=job.options.get("user_id"),
            user_email=job.options.get("user_email"),
        )
        # Headless toggle via env
        if "headless" in job.options:
            import os
            os.environ["HEADLESS"] = "true" if job.options["headless"] else "false"

        try:
            state = await wf.run(job.asins)
            job.results = state.get("results", []) if isinstance(state, dict) else []
            job.status = "done"
        except Exception as e:
            await job.emitter({"step": "error", "status": "error", "error": str(e)})
            job.status = "error"
        finally:
            await job.queue.put({"job_id": job.id, "step": "_stream_end"})

    async def cancel(self, job_id: str) -> bool:
        job = self.get(job_id)
        if not job:
            return False
        if job.status in ("done", "error", "cancelled"):
            return True
        try:
            if job.task and not job.task.done():
                job.task.cancel()
        except Exception:
            pass
        # Chiudi eventuali sessioni Playwright aperte
        try:
            await SessionManager.close_all()
        except Exception:
            pass
        job.status = "cancelled"
        await job.queue.put({"job_id": job.id, "step": "cancelled", "status": "done"})
        await job.queue.put({"job_id": job.id, "step": "_stream_end"})
        return True


job_manager = JobManager()
