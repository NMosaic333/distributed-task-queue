from sqlalchemy import Column, Integer, String, JSON, DateTime
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class Task(Base):
    __tablename__ = "tasks"

    id = Column(Integer, primary_key=True)
    type = Column(String, nullable=False)
    payload = Column(JSON, nullable=False)
    status = Column(String, default="PENDING")
    retries = Column(Integer, default=0)
    idempotency_key = Column(String, unique=True, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

class DeadLetterTask(Base):
    __tablename__ = "dead_letter_tasks"

    id = Column(Integer, primary_key=True)
    original_task_id = Column(Integer)
    type = Column(String)
    payload = Column(JSON)
    retries = Column(Integer)
    failed_at = Column(DateTime, default=datetime.utcnow)

class TaskExecution(Base):
    __tablename__ = "task_executions"

    id = Column(Integer, primary_key=True)
    task_id = Column(Integer, unique=True)
    executed_at = Column(DateTime, default=datetime.utcnow)
    latency_seconds = Column(Integer)
