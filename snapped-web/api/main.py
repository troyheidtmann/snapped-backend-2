from fastapi import FastAPI
from routers import cdn

app = FastAPI()
app.include_router(cdn.router) 