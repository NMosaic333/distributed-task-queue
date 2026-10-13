from redis_queue.redis_queue import RedisQueue
from db.session import SessionLocal
from db.models import Task,TaskExecution
import time
from datetime import datetime, timezone

queue = RedisQueue()

def move_to_dlq(db, task):
    from db.models import DeadLetterTask

    dlq_task = DeadLetterTask(
        original_task_id=task.id,
        type=task.type,
        payload=task.payload,
        retries=task.retries,
    )

    db.add(dlq_task)
    db.delete(task)
    db.commit()


while True:
    queue.requeue_expired_tasks()
    queue.promote_delayed_tasks()

    task_id = queue.claim_task()
    if not task_id:
        time.sleep(5)
        continue

    db = SessionLocal()
    task = db.query(Task).get(task_id)

    if not task:
        queue.ack_task(task_id)
        db.close()
        continue

    try:
        task.status = "IN_PROGRESS"
        db.commit()

        print(f"Processing task {task_id}")
        time.sleep(10)

        task.status = "COMPLETED"

        latency = (datetime.utcnow() - task.created_at).total_seconds()

        execution = TaskExecution(
            task_id=task.id,
            latency_seconds=latency
        )

        db.add(execution)
        db.commit()

        queue.ack_task(task.id)

        print(f"Task {task_id} completed")
        print(f"Latency: {latency}s")

    except Exception as e:
        db.rollback()
        task.retries += 1

        if task.retries >= 5:
            print("ERROR:", e)
            print(f"Task {task.id} moved to DLQ")
            move_to_dlq(db, task)
            db.commit()
            queue.ack_task(task.id)
        else:
            task.status = "PENDING"
            db.commit()
            queue.schedule_retry(task.id, task.retries)
            print(f"Retrying task {task.id} (attempt {task.retries})")

    finally:
        db.close()
