from fastapi import FastAPI, Query, HTTPException
import pandas as pd
import itertools

app = FastAPI(title="Chunked Data API")

FILE_PATH = "data/customers.csv"
CHUNK_SIZE = 5000  # each chunk = 5000 rows (~5% of dataset)

@app.get("/")
def root():
    return {"message": "Welcome to the Chunked Data API!"}

@app.get("/customers")
def get_customers(chunk: int = Query(1, ge=1)):
    try:
        # Skip directly to the requested chunk
        chunk_iter = pd.read_csv(FILE_PATH, chunksize=CHUNK_SIZE)
        chunk_df = next(itertools.islice(chunk_iter, chunk - 1, None))
    except StopIteration:
        raise HTTPException(status_code=404, detail="Chunk out of range")

    return {
        "chunk": chunk,
        "chunk_size": CHUNK_SIZE,
        "data": chunk_df.to_dict(orient="records")
    }
