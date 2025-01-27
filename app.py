import uvicorn
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Dict
from config import db_config
from azure_openai import LLM_Azure
from source_key_mapping import ColumnMatchAgent
from member_data_reconciliation import CSVIntegrityAgent, compare_csv_integrity
import pandas as pd
from populate_member_db import get_db_connection, validate_member_id, transform_data


# FastAPI initialization
app = FastAPI()

# Pydantic model for request validation
class MapSourceKeysRequest(BaseModel):
    tables: List[str]

class MapSourceKeysResponse(BaseModel):
    mappings: Dict[str, Dict[str, str]]

class DataTransferRequest(BaseModel):
    source_table: str = Field(..., description="Name of the source table")
    destination_table: str = Field(..., description="Name of the destination table")
    mapping: Dict[str, str] = Field(..., description="Mapping of source columns to destination columns")

class DataTransferResponse(BaseModel):
    status: str
    rows_transferred: int

class ErrorResponse(BaseModel):
    error: str

@app.get("/")
async def root():
    return {"message": "Hello, World!"}


@app.get("/health")
async def health_check():
    return {"status": "ok"}

# Endpoint implementation
@app.post("/member-merge/map-source-keys-withtestdata", response_model=MapSourceKeysResponse)
def map_source_keys(request: MapSourceKeysRequest):
    try:
        # Extract data from request
        tables = request.tables

        # Standard columns to match
        standard_columns = ["Member ID", "First Name", "Last Name", "DOB", "Address", "City", "State", "Zip"]

        # Initialize LLM and agent
        llm = LLM_Azure()
        agent = ColumnMatchAgent(llm)
        agent.connect_db(db_config)

        # Perform column matching
        mappings = agent.match_columns_for_tables(standard_columns, tables)

        # Return final mappings
        return {"mappings": mappings}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@app.post("/member-merge/reconcile-member-data-test")
async def reconcile_member_data_test(
    file1: UploadFile = File(...), 
    file2: UploadFile = File(...)
):
    try:
        # Read the uploaded files into pandas DataFrames
        df1 = pd.read_csv(file1.file)
        df2 = pd.read_csv(file2.file)

        # Initialize the agent and process the files
        agent = CSVIntegrityAgent()

        # Get missing and altered rows
        missing_rows, altered_rows = compare_csv_integrity(file1.filename, file2.filename)

        # Analyze and get the results
        analysis_result, _ = agent.analyze_and_update_changes(
            df1, df2, missing_rows, altered_rows
        )

        return JSONResponse(content={"analysis_result": analysis_result}, status_code=200)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    


@app.post(
    "/transfer-memberdata-to-db",
    response_model=DataTransferResponse,
    responses={
        400: {"model": ErrorResponse},
        500: {"model": ErrorResponse}
    }
)
async def transfer_data(request: DataTransferRequest):
    """
    Transfer data between tables with column mapping.
    
    - Validates the presence of Member ID
    - Transforms data according to provided mapping
    - Inserts transformed data into destination table
    """
    try:
        # Connect to database
        conn = get_db_connection()
        
        # Read source table
        query = f"SELECT * FROM {request.source_table}"
        try:
            df = pd.read_sql(query, conn)
            print(df.head(2))
        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=f"Error reading source table: {str(e)}"
            )
        
        # Validate Member ID
        if not validate_member_id(df, request.mapping):
            raise HTTPException(
                status_code=400,
                detail="Invalid or missing Member ID in source data"
            )
        
        # Transform data according to mapping
        try:
            transformed_df = transform_data(df, request.mapping)
        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=f"Error transforming data: {str(e)}"
            )
        
        # Insert into destination table
        print(transformed_df.head(2))
        try:
            print("Adding data to :", request.destination_table)
            transformed_df.to_sql(
                request.destination_table,
                conn,
                if_exists='append',
                index=False
            )
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Error inserting data into destination table: {str(e)}"
            )
        
        query = f"SELECT * FROM {request.destination_table}"
        try:
            df = pd.read_sql(query, conn)
            print(df.head(2))
        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=f"Error reading destination table: {str(e)}"
            )

        return DataTransferResponse(
            status="success",
            rows_transferred=len(transformed_df)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run(app, loop="asyncio", host="0.0.0.0", port=8000)
