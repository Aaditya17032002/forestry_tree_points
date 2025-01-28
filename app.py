from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, StreamingResponse
import pandas as pd
import os
import io

app = FastAPI()

# Directory containing the CSV files
CSV_DIRECTORY = "csv_files"

# Global variable for the unified dataframe
dataframe = None

# Load all CSV files on server startup
@app.on_event("startup")
async def load_csvs():
    global dataframe
    csv_files = [
        os.path.join(CSV_DIRECTORY, file)
        for file in os.listdir(CSV_DIRECTORY)
        if file.endswith(".csv")
    ]

    if not csv_files:
        print(f"No CSV files found in {CSV_DIRECTORY}.")
        return

    # Load and concatenate all CSV files
    try:
        dataframes = [pd.read_csv(file) for file in csv_files]
        dataframe = pd.concat(dataframes, ignore_index=True)
        print(f"Loaded {len(dataframe)} records from {len(csv_files)} CSV files.")
    except Exception as e:
        print(f"Error loading CSV files: {e}")


@app.get("/fetch-data/")
async def fetch_data(
    page: int = Query(1, ge=1, description="Page number to fetch"),
    page_size: int = Query(100, ge=1, le=1000, description="Number of records per page"),
):
    """
    Paginated endpoint to fetch data.
    """
    if dataframe is None:
        return {"error": "No CSV files found or loaded."}

    # Pagination logic
    start_idx = (page - 1) * page_size
    end_idx = start_idx + page_size

    if start_idx >= len(dataframe):
        return {"error": "Page number out of range."}

    # Slice the dataframe for the current page
    data = dataframe.iloc[start_idx:end_idx].to_dict(orient="records")
    return {
        "page": page,
        "page_size": page_size,
        "total_records": len(dataframe),
        "total_pages": (len(dataframe) // page_size) + (1 if len(dataframe) % page_size else 0),
        "data": data,
    }


@app.get("/fetch-all-data/")
async def fetch_all_data():
    """
    Streaming endpoint to fetch all data as CSV.
    """
    if dataframe is None:
        return {"error": "No CSV files found or loaded."}

    stream = io.StringIO()
    dataframe.to_csv(stream, index=False)
    stream.seek(0)
    return StreamingResponse(stream, media_type="text/csv")


@app.get("/fetch-summary/")
async def fetch_summary():
    """
    Endpoint to fetch summary statistics of the data.
    """
    if dataframe is None:
        return {"error": "No CSV files found or loaded."}

    summary = {
        "total_records": len(dataframe),
        "columns": list(dataframe.columns),
        "sample": dataframe.head(5).to_dict(orient="records"),
    }
    return JSONResponse(content=summary)
