# Author Name : Chidarapu Gopi Krishna

from confluent_kafka import Producer
import os
from dotenv import load_dotenv
from pymongo import MongoClient, errors
import logging
from logging.handlers import TimedRotatingFileHandler
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get the base directory where the script is located
base_path = os.path.dirname(os.path.abspath(__file__))

# Define the log file path within the "logs" directory
log_file = os.path.join(base_path, "logs", "api_server.log")

# Ensure the "logs" folder exists
log_folder = os.path.dirname(log_file)
if not os.path.exists(log_folder):
    os.makedirs(log_folder)
    
# Add a rotating file handler
file_handler = TimedRotatingFileHandler(log_file, when="midnight", interval=1, backupCount=5)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(console_handler)


load_dotenv()

mongo_host = os.getenv("MONGO_HOST","mongodb+srv://gopikrishna2934:12345@agent.hlvw3u9.mongodb.net/")
db_name = os.getenv("DB_NAME","DataLake")
collection_name = os.getenv("COLLECTION_NAME","Job_Status")

def get_mongo_collection(mongo_host,db_name,collection_name):
    try:
        client = MongoClient(mongo_host)
        # client.server_info()  
        client.list_database_names()
        db = client[db_name]
        users_collection = db[collection_name]
        logger.info(f"Successfully connected to MongoDB")
        return users_collection
    except errors.ConnectionFailure as e:
        print("Connection failed:", e)
    except errors.ConfigurationError as e:
        print("Configuration error:", e)
    except errors.OperationFailure as e:
        print("Operation failure:", e)
    except Exception as e:
        print("Unexpected error:", e)

kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVER", "192.168.63.128:9093")
print(kafka_bootstrap_servers)
producer_conf = {'bootstrap.servers': kafka_bootstrap_servers}

def get_kafka_producer():
    try:
        producer = Producer(producer_conf)
        logger.info("Producer created successfully")
        return producer
    except Exception as e:
        logger.error(f"Error initializing Kafka producer: {e}")

