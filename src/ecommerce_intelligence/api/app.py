import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from pydantic import BaseModel, ConfigDict, Field

from ecommerce_intelligence.ml.predict_purchase import (
    load_candidate_models,
    score_feature_row,
)


class FeatureSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    events_previous_day: int = Field(ge=0)
    page_visits_previous_day: int = Field(ge=0)
    search_previous_day: int = Field(ge=0)
    client_cart_additions_previous_day: int = Field(ge=0)
    client_cart_removals_previous_day: int = Field(ge=0)
    client_purchases_previous_day: int = Field(ge=0)
    distinct_skus_previous_day: int = Field(ge=0)
    sku_interactions_previous_day: int = Field(ge=0)
    sku_cart_additions_previous_day: int = Field(ge=0)
    sku_cart_removals_previous_day: int = Field(ge=0)
    sku_purchases_previous_day: int = Field(ge=0)
    hours_since_last_client_event: float = Field(ge=0)
    hours_since_last_sku_event: float = Field(ge=0)


@asynccontextmanager
async def lifespan(app: FastAPI):
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI")
    if not tracking_uri:
        raise RuntimeError("MLFLOW_TRACKING_URI is required")

    app.state.purchase_model, app.state.timing_model = load_candidate_models(
        tracking_uri
    )
    yield


app = FastAPI(title="E-commerce purchase scoring", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ready"}


@app.post("/predict")
def predict(features: FeatureSnapshot, request: Request):
    return score_feature_row(
        features.model_dump(),
        request.app.state.purchase_model,
        request.app.state.timing_model,
    )