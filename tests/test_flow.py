import pytest
import pandas as pd
import os
from feature_store import FeatureStore
# Import the database initialization function
from feature_store.core.registry.db import init_db

def test_full_flow():
    # 0. Setup Environment (Crucial for CI/CD)
    # This creates the 'data/' directory and the SQLite tables
    init_db()

    # 1. Initialize
    fs = FeatureStore()
    feat_name = "ci_test_feature"
    
    # 2. Register
    try:
        fs.register_feature(feat_name, "Test for CI/CD", "ci_bot")
    except:
        pass # Already exists is fine
        
    # 3. Ingest Data
    df = pd.DataFrame({"id": [1, 2], "val": [10, 20]})
    version = fs.ingest_feature_data(feat_name, df)
    
    assert version.version.startswith("v")
    assert os.path.exists(version.path)
    
    # 4. Read Back
    df_read = fs.get_feature_data(feat_name)
    assert len(df_read) == 2
    assert df_read.iloc[0]["val"] == 10