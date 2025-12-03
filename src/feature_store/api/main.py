from fastapi import FastAPI, HTTPException
from feature_store.core.manager import FeatureStore
from feature_store.api.schemas import OnlineFeatureRequest, OnlineFeatureResponse

app = FastAPI(title="Feature Store API", version="1.0.0")

# Initialize store (In prod, this might connect to Redis)
fs = FeatureStore()

@app.get("/health")
def health_check():
    """Health check endpoint for Kubernetes/Docker"""
    return {"status": "ok"}

@app.post("/features/online", response_model=OnlineFeatureResponse)
def get_online_feature(request: OnlineFeatureRequest):
    """
    Retrieve the latest feature values for a specific entity (e.g., User ID).
    """
    try:
        data = fs.get_online_value(
            feature_name=request.feature_name,
            entity_id=request.entity_id,
            entity_key=request.entity_key
        )
        
        if data is None:
             return OnlineFeatureResponse(
                feature_name=request.feature_name,
                entity_id=request.entity_id,
                error="Entity not found in latest version"
            )

        return OnlineFeatureResponse(
            feature_name=request.feature_name,
            entity_id=request.entity_id,
            data=data
        )
    except Exception as e:
        # Log the error here
        raise HTTPException(status_code=400, detail=str(e))