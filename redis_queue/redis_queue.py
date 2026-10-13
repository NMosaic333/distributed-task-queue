import redis
import time
import math


class RedisQueue:
    def __init__(self):
        self.client = redis.Redis(host="localhost", port=6379, decode_responses=True)
        self.ready_queue = "task_queue"
        self.processing_zset = "processing_tasks"
        self.delayed_zset = "delayed_tasks"
        self.visibility_timeout = 10
        self.claim_script = self.client.register_script("""
            local task_id = redis.call('RPOP', KEYS[1])
            if not task_id then
                return nil
            end

            local visibility_deadline = tonumber(ARGV[1])
            redis.call('ZADD', KEYS[2], visibility_deadline, task_id)

            return task_id
        """)
        self.ack_script = self.client.register_script("""
            if redis.call('ZSCORE', KEYS[1], ARGV[1]) then
                return redis.call('ZREM', KEYS[1], ARGV[1])
            end
            return 0
        """)


    def enqueue(self, task_id: int):
        self.client.lpush(self.ready_queue, task_id)

    def dequeue(self):
        return self.client.brpop(self.queue_name, timeout=5)

    def claim_task(self):
        visibility_deadline = time.time() + self.visibility_timeout

        task_id = self.claim_script(
            keys=[self.ready_queue, self.processing_zset],
            args=[visibility_deadline],
        )

        return task_id
    
    def requeue_expired_tasks(self):
        now = time.time()
        expired_tasks = self.client.zrangebyscore(
            self.processing_zset, 0, now
        )

        for task_id in expired_tasks:
            self.client.lpush(self.ready_queue, task_id)
            self.client.zrem(self.processing_zset, task_id)

    def ack_task(self, task_id):
        self.ack_script(keys=[self.processing_zset], args=[task_id])


    def schedule_retry(self, task_id, retry_count):
        delay = min(2 ** retry_count, 60)
        run_at = time.time() + delay

        self.client.zadd(self.delayed_zset, {task_id: run_at})
        self.client.zrem(self.processing_zset, task_id)

    def promote_delayed_tasks(self):
        now = time.time()
        ready = self.client.zrangebyscore(self.delayed_zset, 0, now)

        for task_id in ready:
            self.client.lpush(self.ready_queue, task_id)
            self.client.zrem(self.delayed_zset, task_id)

