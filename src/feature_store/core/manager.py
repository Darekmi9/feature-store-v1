import os
import pandas as pd
from typing import List, Optional
from datetime import datetime
import json
from feature_store.core.quality.profiler import calculate_statistics
from sqlalchemy.orm import Session, joinedload
from feature_store.config import settings
from feature_store.core.registry.db import SessionLocal
from feature_store.core.registry.models import Feature, FeatureVersion
from feature_store.core.storage import get_artifact_store

class FeatureStore:
    def __init__(self):
        """Initialize the Feature Store Manager"""
        pass

    def register_feature(self, name: str, description: str = "", owner: str = "system") -> Feature:
        """
        Register a new feature definition in the metadata store.
        """
        session: Session = SessionLocal()
        try:
            # Check if feature exists
            existing = session.query(Feature).filter(Feature.name == name).first()
            if existing:
                print(f"Feature '{name}' already exists. Updating metadata...")
                existing.description = description
                existing.owner = owner
                session.commit()
                session.refresh(existing)
                return existing
            
            # Create new feature
            new_feature = Feature(name=name, description=description, owner=owner)
            session.add(new_feature)
            session.commit()
            session.refresh(new_feature)
            print(f"Successfully registered feature: {name}")
            return new_feature
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def list_features(self) -> List[dict]:
        """List all registered features"""
        session: Session = SessionLocal()
        try:
            features = session.query(Feature).all()
            return [
                {
                    "id": f.id,
                    "name": f.name,
                    "owner": f.owner,
                    "versions": len(f.versions)
                } 
                for f in features
            ]
        finally:
            session.close()

    def get_feature(self, name: str) -> Optional[Feature]:
        """Retrieve full feature details by name"""
        session: Session = SessionLocal()
        try:
            # joinedload ensures 'versions' are fetched before session closes
            return session.query(Feature)\
                .options(joinedload(Feature.versions))\
                .filter(Feature.name == name)\
                .first()
        finally:
            session.close()
            
    def ingest_feature_data(self, feature_name: str, df: pd.DataFrame, commit_hash: str = None) -> FeatureVersion:
        """
        Version and save a feature dataframe with automated profiling.
        """
        session: Session = SessionLocal()
        store = get_artifact_store()
        
        try:
            # 1. Get Feature
            feature = session.query(Feature).filter(Feature.name == feature_name).first()
            if not feature:
                raise ValueError(f"Feature '{feature_name}' not found. Register it first.")

            # 2. Determine Version
            latest_ver = session.query(FeatureVersion)\
                .filter(FeatureVersion.feature_id == feature.id)\
                .order_by(FeatureVersion.id.desc())\
                .first()
            
            if latest_ver:
                curr_num = int(latest_ver.version.replace("v", ""))
                new_version_str = f"v{curr_num + 1}"
            else:
                new_version_str = "v1"

            # 3. Define Paths
            base_rel_path = f"{feature_name}/{new_version_str}"
            parquet_path = str(settings.feature_store_path / f"{base_rel_path}.parquet")
            stats_path = str(settings.feature_store_path / f"{base_rel_path}_stats.json")

            # 4. Calculate Statistics (The new Quality Step)
            print(f"Profiling data for {feature_name}...")
            stats_profile = calculate_statistics(df)
            
            # 5. Write Data & Stats
            print(f"Writing data to {parquet_path}...")
            store.write_dataset(df, parquet_path)
            
            # Write stats JSON (Using standard IO for now)
            with open(stats_path, 'w') as f:
                json.dump(stats_profile, f, indent=4)

            # 6. Register Version in DB
            new_version = FeatureVersion(
                feature_id=feature.id,
                version=new_version_str,
                path=parquet_path,
                git_commit_hash=commit_hash,
                computed_at=datetime.utcnow()
            )
            session.add(new_version)
            session.commit()
            session.refresh(new_version)
            
            print(f"Successfully ingested {feature_name} version {new_version_str}")
            return new_version

        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def get_feature_data(self, feature_name: str, version: str = None) -> pd.DataFrame:
        """
        Retrieve data for a specific feature.
        If version is None, fetches the latest version.
        """
        session: Session = SessionLocal()
        store = get_artifact_store()
        try:
            # 1. Find Feature
            feature = session.query(Feature).filter(Feature.name == feature_name).first()
            if not feature:
                raise ValueError(f"Feature '{feature_name}' not found.")

            # 2. Determine Version
            if version:
                ver_obj = session.query(FeatureVersion)\
                    .filter(FeatureVersion.feature_id == feature.id, FeatureVersion.version == version)\
                    .first()
            else:
                # Get latest
                ver_obj = session.query(FeatureVersion)\
                    .filter(FeatureVersion.feature_id == feature.id)\
                    .order_by(FeatureVersion.id.desc())\
                    .first()
            
            if not ver_obj:
                raise ValueError(f"No data found for feature '{feature_name}' (Version: {version or 'Latest'})")

            # 3. Read Data
            print(f"Loading data from {ver_obj.path}...")
            return store.read_dataset(ver_obj.path)

        finally:
            session.close()

    def get_online_value(self, feature_name: str, entity_id: any, entity_key: str = "user_id") -> dict:
        """
        Simulates online retrieval by fetching the latest value for a specific entity.
        Note: In production, this would usually hit a low-latency store like Redis.
        """
        # Load latest dataframe (Cached implementation would be better here for prod)
        df = self.get_feature_data(feature_name)
        
        # Filter for the entity
        if entity_key not in df.columns:
            raise ValueError(f"Entity key '{entity_key}' not found in feature data.")

        record = df[df[entity_key] == entity_id]
        
        if record.empty:
            return None
        
        # Return as dictionary (JSON-like for API)
        return record.iloc[0].to_dict()
    
    def get_feature_stats(self, feature_name: str, version: str) -> dict:
        """Retrieve the statistical profile for a specific feature version"""
        session: Session = SessionLocal()
        try:
            # Find the version
            feature = session.query(Feature).filter(Feature.name == feature_name).first()
            if not feature: raise ValueError("Feature not found")
            
            ver_obj = session.query(FeatureVersion)\
                .filter(FeatureVersion.feature_id == feature.id, FeatureVersion.version == version)\
                .first()
            
            if not ver_obj: raise ValueError("Version not found")
            
            # Derive stats path from parquet path
            # e.g., .../v1.parquet -> .../v1_stats.json
            stats_path = ver_obj.path.replace(".parquet", "_stats.json")
            
            if not os.path.exists(stats_path):
                return {"error": "Stats file not found"}
                
            with open(stats_path, 'r') as f:
                return json.load(f)
        finally:
            session.close()