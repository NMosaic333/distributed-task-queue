from fastapi import FastAPI
from api.routes import router

app = FastAPI(title="Distributed Task Queue")
app.include_router(router)
