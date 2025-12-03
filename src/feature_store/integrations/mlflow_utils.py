import mlflow
import os
from typing import Dict, Any

class MLflowLogger:
    def __init__(self, experiment_name: str = "feature_store_ingestion"):
        self.enabled = True
        try:
            mlflow.set_experiment(experiment_name)
        except Exception as e:
            print(f"MLflow not available or failed to configure: {e}")
            self.enabled = False

    def log_feature_version(self, 
                          feature_name: str, 
                          version: str, 
                          params: Dict[str, Any], 
                          metrics: Dict[str, Any] = None):
        """
        Log a feature ingestion run to MLflow.
        """
        if not self.enabled:
            return

        with mlflow.start_run(run_name=f"{feature_name}_{version}"):
            # Log Identifiers
            mlflow.log_param("feature_name", feature_name)
            mlflow.log_param("version", version)
            
            # Log Metadata (Path, Owner, etc.)
            for k, v in params.items():
                mlflow.log_param(k, v)
            
            # Log Statistics (Mean, Null Counts)
            if metrics:
                # Flatten the stats dictionary for logging
                # stats structure is {'columns': {'col_name': {'mean': 10...}}}
                for col, stats in metrics.get("columns", {}).items():
                    for stat_name, val in stats.items():
                        if val is not None and isinstance(val, (int, float)):
                            mlflow.log_metric(f"{col}_{stat_name}", val)
            
            print(f"[MLflow] Logged run for {feature_name} {version}")