from pydantic import BaseModel
from typing import Dict, Any, Optional, Union

class OnlineFeatureRequest(BaseModel):
    feature_name: str
    entity_id: Union[int, str]
    entity_key: str = "user_id"

class OnlineFeatureResponse(BaseModel):
    feature_name: str
    entity_id: Union[int, str]
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None