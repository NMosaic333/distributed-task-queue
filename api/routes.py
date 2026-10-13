from fastapi import APIRouter
from db.models import Task, DeadLetterTask, TaskExecution
from db.session import SessionLocal
from redis_queue.redis_queue import RedisQueue
from sqlalchemy import func

router = APIRouter()
queue = RedisQueue()

@router.post("/tasks")
def enqueue_task(task: dict):
    db = SessionLocal()
    try:
        idem_key = task.get("idempotency_key")

        if idem_key:
            existing = (
                db.query(Task)
                .filter(Task.idempotency_key == idem_key)
                .first()
            )
            if existing:
                return {"task_id": existing.id, "status": "duplicate"}

        new_task = Task(
            type=task["type"],
            payload=task["payload"],
            status="PENDING",
            idempotency_key=idem_key,
        )

        db.add(new_task)
        db.commit()
        db.refresh(new_task)

        queue.enqueue(new_task.id)

        return {"task_id": new_task.id, "status": "created"}
    finally:
        db.close()

@router.get("/metrics")
def metrics():
    db = SessionLocal()
    try:
        ready = queue.client.llen(queue.ready_queue)
        delayed = queue.client.zcard(queue.delayed_zset)
        processing = queue.client.zcard(queue.processing_zset)

        dlq_count = db.query(DeadLetterTask).count()

        latencies = db.query(TaskExecution.latency_seconds).all()
        values = sorted([l[0] for l in latencies if l[0] is not None])

        if values:
            p50 = values[int(0.5 * len(values))]
            p99 = values[int(0.99 * len(values))]
        else:
            p50 = p99 = None

        return {
            "ready_queue": ready,
            "processing": processing,
            "delayed": delayed,
            "dead_letter_count": dlq_count,
            "p99": p99,
            "p50": p50,
        }
    finally:
        db.close()
