import pandas as pd
import numpy as np
from typing import Dict, Any

def calculate_statistics(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Generates a statistical profile of the dataframe.
    Tracks: Row count, per-column nulls, mean, std, min, max (for numerics).
    """
    stats = {
        "row_count": len(df),
        "columns": {}
    }

    for col in df.columns:
        col_stats = {
            "dtype": str(df[col].dtype),
            "null_count": int(df[col].isnull().sum()),
            "null_percentage": float(df[col].isnull().sum() / len(df))
        }

        # Numeric stats
        if pd.api.types.is_numeric_dtype(df[col]):
            col_stats.update({
                "mean": float(df[col].mean()) if not df[col].isnull().all() else None,
                "std": float(df[col].std()) if not df[col].isnull().all() else None,
                "min": float(df[col].min()) if not df[col].isnull().all() else None,
                "max": float(df[col].max()) if not df[col].isnull().all() else None,
            })
        
        stats["columns"][col] = col_stats

    return stats