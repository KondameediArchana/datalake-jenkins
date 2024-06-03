import requests
import uuid
from pymongo import MongoClient
import logging
from logging.handlers import TimedRotatingFileHandler
import os
from fastapi import HTTPException
from dotenv import load_dotenv



# Setting up logging
base_path = os.path.dirname(os.path.abspath(__file__))

# Define the log file path within the "logs" directory
log_file = os.path.join(base_path, "logs", "api_server.log")

# Ensure the "logs" folder exists
log_folder = os.path.dirname(log_file)
if not os.path.exists(log_folder):
    os.makedirs(log_folder)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
 

logger.setLevel(logging.DEBUG)


# Add a rotating file handler
file_handler = TimedRotatingFileHandler(log_file, when="midnight", interval=1, backupCount=5)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(console_handler)
 





load_dotenv()

mongo_host = os.getenv("MONGO_HOST","mongodb+srv://gopikrishna2934:12345@agent.hlvw3u9.mongodb.net/")

# mongo_host = "mongodb://localhost:27017"

trino_url = os.getenv("TRINO_URL", "http://192.168.56.179:8080/v1/statement")





from pydantic import BaseModel


class TrinoConfig(BaseModel):
    data_schema: list
    t_schema: str
    table_name: str
    host: str
    port: str
    username: str
    catalog: str
    file_format: str
    # query: str    


def get_mongo_collection():
    client = MongoClient(mongo_host)
    db = client.trino_db
    collection = db.token_nexturi
    return collection

def forward_request_to_trino(query):
    try:
        # trino_url = "http://localhost:8080/v1/statement"
        data = query
        trino_user = "admin"
        headers = {"X-Trino-User": trino_user}
        response = requests.post(trino_url, headers=headers, data=data).json()
        logger.info(f"Forwarded request to Trino. Query: {query}, Response: {response}")
        return response
    except requests.RequestException as e:
        logger.error(f"Error forwarding URI to Trino: {str(e)}")
        raise HTTPException(status_code=200, detail=f"Error forwarding URI to Trino: {str(e)}")
 
def forward_next_uri_to_trino(next_uri):
    try:
        if next_uri:
            response = requests.get(next_uri).json()
            logger.info(f"Forwarded next URI to Trino. Next URI: {next_uri}, Response: {response}")
            return response
    except requests.RequestException as e:
        logger.error(f"Error forwarding next URI to Trino: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error forwarding next URI to Trino: {str(e)}")
 
def store_next_uri(next_uri):
    try:
        token = str(uuid.uuid4())
        collection = get_mongo_collection()
        data_to_insert = {"token": token, "next_uri": next_uri, "tokenUsed": False}
        result = collection.insert_one(data_to_insert)
        logger.info(f"Stored next URI in MongoDB. Token: {token}, Next URI: {next_uri}")
        return token
    except Exception as e:
        logger.error(f"Error storing next URI in MongoDB: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error storing next URI in MongoDB: {str(e)}")
 
def get_next_uri_from_db(token):
    try:
        collection = get_mongo_collection()
        data = collection.find_one({"token": token})
        next_uri = None
        is_used = data["tokenUsed"]
        if not is_used:
            next_uri = data["next_uri"]
        return next_uri
    except Exception as e:
        logger.error(f"Error retrieving next URI from MongoDB: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error retrieving next URI from MongoDB: {str(e)}")