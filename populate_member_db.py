from typing import Dict, Optional
import pandas as pd
from typing import Dict
from sqlalchemy import create_engine
from config import db_config
from fastapi import FastAPI, File, HTTPException


app = FastAPI(
    title="Data Transfer API",
    description="API for transferring data between tables with column mapping",
    version="1.0.0"
)

def get_db_connection():
    """Create and return a database connection."""
    try:
        connection = create_engine(
            f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
        )
        return connection
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection error: {str(e)}")

def validate_member_id(df: pd.DataFrame, mapping: Dict[str, str]) -> bool:
    """
    Validate if Member ID is present in the source data.
    
    Args:
        df: Source DataFrame
        mapping: Column mapping dictionary
    
    Returns:
        bool: True if Member ID is present and valid
    """
    source_member_id = next((v for k, v in mapping.items() if k == "Member ID"), None)
    if not source_member_id or source_member_id not in df.columns:
        return False
    
    # Check if any Member ID is null or empty
    return not df[source_member_id].isna().any() and not (df[source_member_id] == '').any()

def transform_data(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    """
    Transform the source DataFrame using the provided mapping.
    
    Args:
        df: Source DataFrame
        mapping: Column mapping dictionary
    
    Returns:
        pd.DataFrame: Transformed DataFrame with destination column names
    """
    transformed_df = pd.DataFrame()
    
    for dest_col, source_col in mapping.items():
        if source_col in df.columns:
            transformed_df[dest_col] = df[source_col]
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Source column '{source_col}' not found in source table"
            )
    
    return transformed_df

