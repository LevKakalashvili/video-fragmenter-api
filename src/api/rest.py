from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.endpoints import api_router
from src.core.settings import settings


def create_app(lifespan_) -> FastAPI:
    app = FastAPI(
        title=settings.app.project_name,
        version=settings.app.version,
        docs_url=f"{settings.app.api}/docs",
        openapi_url=f"{settings.app.api}/openapi.json",
        debug=settings.app.debug,
        lifespan=lifespan_,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.app.backend_cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(api_router, prefix=settings.app.api)
    return app
