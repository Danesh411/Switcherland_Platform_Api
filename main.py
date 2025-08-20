import time
import uvicorn
from pymongo import MongoClient
from fastapi import FastAPI, Query
from datetime import datetime, timezone
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from aldi import aldi_main
from coop import coop_main
from migros import migros_main


def log_to_mongodb(log):
    required_fields = {
        "endpoint": str,
        "request_url": str,
        "status_code": int,
        "request_time": datetime,
        "elapsed": (int, float),
        "params": dict,
        "payload": dict,
        "data": dict,
        "response_path": str
    }

    for field, field_type in required_fields.items():
        if field not in log or not isinstance(log[field], field_type):
            raise ValueError(f"Missing or invalid field: {field}")

    optional_fields = {
        "error_message": (str, type(None)),
        "proxy": (str, type(None)),
        "cost": str
    }

    for field, field_type in optional_fields.items():
        if field in log and not isinstance(log[field], field_type):
            raise ValueError(f"Invalid type for optional field: {field}")

    try:
        client = MongoClient("mongodb://actowiz:tvvL4n=33=*_@51.222.244.92:27017/admin?authSource=admin")
        db = client["switcherland_ecommerce_api"]
        collection = db["logs"]
        result = collection.insert_one(log)
        print(f"Log inserted with ID: {result.inserted_id}")
        return result.inserted_id
    except Exception as e:
        print(f"Error inserting log into MongoDB: {e}")
        return None


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

VALID_API_KEYS = ["T80492751Q", "T80492751QTT"]
VALID_PLATFORMS = ['migros', 'coop', 'aldi', 'mano']

@app.get("/switcherland_ecommerce/data")
async def get_data(
    platform: str = Query(..., description="Choose from migros, coop, aldi, mano"),
    keyword: str = Query(..., description="Choose keyword like book, pen, and etc"),
    apikey: str = Query(..., description="Your API key")
):
    start_time = time.time()
    log_base = {
        "endpoint": "http://51.222.241.206:1937/switcherland_ecommerce/data",
        "request_url": f"/switcherland_ecommerce/data?platform={platform}&apikey={apikey}&keyword={keyword}",
        "request_time": datetime.now(timezone.utc),
        "elapsed": 0,
        "params": {"platform": platform, "key": apikey, "keyword": keyword},
        "payload": {},
        "data": {},
        "response_path": "",
        "error_message": None,
    }
    # Validate API key
    if apikey not in VALID_API_KEYS:
        log_base["status_code"] = 401
        log_base["elapsed"] = time.time() - start_time
        log_base["error_message"] = "Invalid API Key"
        log_to_mongodb(log_base)
        return JSONResponse(status_code=401, content={"status": 401, "message": "Invalid API Key"})

    # Validate platform
    if platform.lower() not in VALID_PLATFORMS:
        log_base["status_code"] = 400
        log_base["elapsed"] = time.time() - start_time
        log_base["error_message"] = "Invalid platform"
        log_to_mongodb(log_base)
        return JSONResponse(status_code=400, content={"status": 400, "message": "Invalid platform"})

    try:
        if platform == 'coop':
            result = coop_main(keyword)
        elif platform == 'migros':
            result = migros_main(keyword)
        elif platform == "aldi":
            result = aldi_main(keyword)
        else:
            result = ''
        if result == "Something missing.." or result == "No Matches Available" or result == []:
            log_base["status_code"] = 400 if result == "Something missing.." else 404
            log_base["elapsed"] = time.time() - start_time
            log_base["error_message"] = result if result != [] else "No Matches Available"
            log_to_mongodb(log_base)
            return JSONResponse(status_code=log_base["status_code"], content={"status": log_base["status_code"], "platform": platform, 'time_taken':time.time() - start_time, "data": log_base["error_message"]})
        log_base["status_code"] = 200
        log_base["elapsed"] = time.time() - start_time
        log_to_mongodb(log_base)
        return JSONResponse(status_code=200, content={"status": 200, "platform": platform, 'keyword':keyword, 'time_taken':time.time() - start_time, "data": result})
    except Exception as e:
        log_base["status_code"] = 500
        log_base["error_message"] = str(e)
        log_base["elapsed"] = time.time() - start_time
        log_to_mongodb(log_base)
        return JSONResponse(status_code=500, content={"status": 500, "message": f"Server Error: {str(e)}"})

@app.get("/")
async def root():
    return JSONResponse(content={"status": 200, "message": "API is running!"})

if __name__ == "__main__":
    uvicorn.run("main:app", host="51.222.241.206", port=1937, reload=True, workers=8)
