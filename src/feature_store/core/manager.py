import os
import pandas as pd
from typing import List, Optional
from datetime import datetime
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
        Version and save a feature dataframe.
        1. Determines next version number (v1 -> v2)
        2. Writes Parquet file to storage
        3. Updates metadata registry
        """
        session: Session = SessionLocal()
        store = get_artifact_store()
        
        try:
            # 1. Get Feature
            feature = session.query(Feature).filter(Feature.name == feature_name).first()
            if not feature:
                raise ValueError(f"Feature '{feature_name}' not found. Register it first.")

            # 2. Determine Version
            # Get latest version for this feature
            latest_ver = session.query(FeatureVersion)\
                .filter(FeatureVersion.feature_id == feature.id)\
                .order_by(FeatureVersion.id.desc())\
                .first()
            
            if latest_ver:
                # Extract number from "v1", "v2" -> increment
                curr_num = int(latest_ver.version.replace("v", ""))
                new_version_str = f"v{curr_num + 1}"
            else:
                new_version_str = "v1"

            # 3. Define Storage Path
            # data/features/feature_name/v1.parquet
            relative_path = f"{feature_name}/{new_version_str}.parquet"
            full_path = str(settings.feature_store_path / relative_path)

            # 4. Write Data (Parquet)
            print(f"Writing data to {full_path}...")
            store.write_dataset(df, full_path)

            # 5. Register Version in DB
            new_version = FeatureVersion(
                feature_id=feature.id,
                version=new_version_str,
                path=full_path,
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