import pandas as pd
from feature_store import FeatureStore

fs = FeatureStore()

# 1. Register Feature
feat_name = "user_login_counts"
fs.register_feature(feat_name, "Number of logins per user", "harshith")

# 2. Create Dummy Data (Batch 1)
df1 = pd.DataFrame({
    "user_id": [101, 102, 103],
    "login_count": [5, 12, 0],
    "timestamp": pd.Timestamp.now()
})

# 3. Ingest Batch 1 (Should be v1)
print("--- Ingesting Batch 1 ---")
fs.ingest_feature_data(feat_name, df1)

# 4. Create Dummy Data (Batch 2 - Updated data)
df2 = pd.DataFrame({
    "user_id": [101, 102, 103, 104],
    "login_count": [6, 15, 1, 3], # Counts increased
    "timestamp": pd.Timestamp.now()
})

# 5. Ingest Batch 2 (Should be v2)
print("\n--- Ingesting Batch 2 ---")
fs.ingest_feature_data(feat_name, df2)

# 6. Check Registry
print("\n--- Registry Status ---")
feat = fs.get_feature(feat_name)
for v in feat.versions:
    print(f"Version: {v.version} | Path: {v.path}")