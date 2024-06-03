# Author Name : Chidarapu Gopi Krishna

import orjson
from fastapi import Depends, HTTPException, status, Path, Header
from pydantic import BaseModel
import os
import uuid
from dotenv import load_dotenv
import requests
from keycloak import KeycloakOpenID
from keycloak.exceptions import KeycloakAuthenticationError, KeycloakError, KeycloakConnectionError, KeycloakGetError
import jose
import json
import logging
from logging.handlers import TimedRotatingFileHandler
import os
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pymongo import MongoClient, errors
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, EndpointConnectionError
import paramiko
from connections import get_mongo_collection, get_kafka_producer
from trino_utilities import TrinoConfig
security = HTTPBasic()

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
db_name = os.getenv("DB_NAME","DataLake_Airflow")
collection_name = os.getenv("COLLECTION_NAME","Dags_01")

realm_name = os.getenv("REALM_NAME", "test_realm")
client_id = os.getenv("CLIENT_ID", "test-client")
client_secret_key = os.getenv("CLIENT_SECRET_KEY", "NlIttBCcv3GHMzB3vedF78qEkGxx0N1w")
server_url = os.getenv("KEYCLOAK_SERVER_URL", "http://localhost:8082")

try:
    keycloak_openid = KeycloakOpenID(server_url=server_url,
                                    client_id=client_id,
                                    realm_name=realm_name,
                                    client_secret_key=client_secret_key)
    logger.info("KeycloakOpenID object seems valid.")
    config_well_known = keycloak_openid.well_known()
    
except KeycloakConnectionError as KCE:
    keycloak_openid = None
    logger.info(f"Error verifying KeycloakOpenID: {KCE}")
    
except KeycloakGetError as KGE:
    keycloak_openid = None
    logger.info(f"Error verifying KeycloakOpenID: {KGE}")

except Exception as e:
    keycloak_openid = None
    logger.info(f"Error verifying KeycloakOpenID: {e}")


users_collection = get_mongo_collection(mongo_host,db_name,collection_name)
user_status_collection = get_mongo_collection(mongo_host,db_name,collection_name)
producer = get_kafka_producer()

kafka_topic_in_airflow = os.getenv("KAFKA_TOPIC", "data-movement-jobs")
print(kafka_topic_in_airflow)


class Source(BaseModel):
    type: str
    config: dict

class Destination(BaseModel):
    type: str
    config: dict
    
class KafkaConfig(BaseModel):
    topic_name: str

class ConnectorRequest(BaseModel):
    connector_name: str
    source: Source
    destination: Destination
    # kafka: KafkaConfig
    trino: TrinoConfig
    
class Status(BaseModel):
    request_id: str

class TokenData(BaseModel):
    access_token: str
    
def get_token(credentials: HTTPBasicCredentials = Depends(security)):
    try:
        if keycloak_openid is not None:
            data = keycloak_openid.token(credentials.username,credentials.password)
            return data
        else:
            error_message = "Internal Server Error"
            logger.error("Error occured while connecting to keycloak there might be a problem with Keycloak configurations")
            raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error_message,
        ) 
    except requests.exceptions.RequestException as req_ex:
        logger.error(f"Request Exception: {req_ex}")
        return {"error":f"Request Exception: {req_ex}"}
    except KeycloakAuthenticationError as auth_ex:
        logger.error(f"Authentication Error: {auth_ex}")
        error_message = json.loads(auth_ex.error_message)
        return {"error":error_message}
    except KeycloakError as kc_ex:
        logger.error(f"Keycloak Error: {kc_ex}")
        return {"error":f"Keycloak Error: {kc_ex}"}
    except Exception as e:
        logger.error(f"Unexpected Exception: {e}")
        return {"error":f"Unexpected Exception: {e}"}


def delivery_report(err, msg):
    """Callback function to be called on successful or failed message delivery."""
    if err is not None:
        error_message = f"Error occured which sending request to kafka {err}"
        logger.error(f"{error_message}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error_message,
        )
    else:
        logger.info('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

    
def validate_token(authorization: str = Header(...)):
    try:
        if "Bearer " not in authorization:
            logger.error("Invalid token")
            raise HTTPException(status_code=401, detail="Invalid token")
        access_token = authorization.replace("Bearer ", "")
        logger.info("access_token received.....")
        KEYCLOAK_PUBLIC_KEY = "-----BEGIN PUBLIC KEY-----\n" + keycloak_openid.public_key() + "\n-----END PUBLIC KEY-----"
        logger.info("Keycloak public key received successfully")
        options = {"verify_signature": True, "verify_aud": True, "verify_exp": True}
        token_info = keycloak_openid.decode_token(access_token, key=KEYCLOAK_PUBLIC_KEY, options=options)
        if token_info:
            logger.info("access_token signature verified by keycloak successfully.")
            return token_info
    except jose.exceptions.ExpiredSignatureError as SE:
        logger.error(f"Token has expired {SE}")
        raise HTTPException(status_code=403, detail="Token has expired")
    except jose.exceptions.JWTError as JWTE:
        logger.error(f"Please enter the valid token. {JWTE}")
        raise HTTPException(status_code=403, detail="Please enter the valid token.")
    except Exception as e:
        logger.error(f"An unexpected error occurred {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    
def connector_checker(connector_name):
    connector_names = ["nifi_file_to_minio","nifi_mongo_to_minio","log_data_to_minio","mongo_to_minio"]
    if connector_name in connector_names:
        return True
    else:
        return False
    
def check_mongo_config(host,port,chunk_size,db_name,multi_collection_names,store_key):
    if host and port and chunk_size and db_name and multi_collection_names and store_key:
        mongo_config = {"host":host,"port":port,"chunk_size":chunk_size,"db_name":db_name,"multi_collection_names":multi_collection_names,"store_key":store_key}
        return mongo_config
    else:
        return None
    
def check_minio_config(endpoint,access_key,secret_key,bucket_name):
    if endpoint and access_key and secret_key and bucket_name:
        minio_config = {"endpoint":endpoint,"access_key":access_key,"secret_key":secret_key,"bucket_name":bucket_name}
        return minio_config
    else:
        return None
    
def check_log_config(remote_server,server_port,remote_filepath,username,password,store_key,micro_service_name):
    if remote_server and server_port and remote_filepath and username and password and store_key and micro_service_name:
        log_config = {"remote_server":remote_server,"server_port":server_port,"remote_filepath":remote_filepath,"username":username,"password":password,"store_key":store_key,"micro_service_name":micro_service_name}
        return log_config
    else:
        return None
    
def vaildate_source_type(source_type):
    sources = ["mongo","log"]
    if source_type in sources:
        return True, source_type
    else:
        return False, None
    
def validate_destination_type(destination_type):
    destinations = ["minio"]
    if destination_type in destinations:
        return True, destination_type
    else:
        return False, None
    
def validate_mongo_credentials(host,port,chunk_size,db_name,multi_collection_names,store_key):
    valid_mongo_config = check_mongo_config(host,port,chunk_size,db_name,multi_collection_names,store_key)
    if valid_mongo_config:
        host = valid_mongo_config.get("host")
        port = valid_mongo_config.get("port")
        db_name = valid_mongo_config.get("db_name")
        multi_collection_names = valid_mongo_config.get("multi_collection_names")
        if multi_collection_names:
            collections_list = multi_collection_names.split(',')
        else:
            collections_list = []
        try:
            client = MongoClient(host,port)
            client.server_info()  
            data_base_names = client.list_database_names()
            if db_name in data_base_names:
                db = client[db_name]
                collection_names = db.list_collection_names()
                collections_present = all(value in collection_names for value in collections_list)
                if collections_present:
                    return True
                else:
                    return False  # here we can return like these collections are not present in the data_base you have provided
            else:
                return False # here we can return data_base name you have provided is not present the mongo_host you have provided
        except errors.ConnectionFailure as e:
            print("Connection failed:", e)
            return False # here we can return like you have provide the wrong host or port
        except errors.ConfigurationError as e:
            print("Configuration error:", e)
            return False # here we can return like you have provide the wrong host or port
        except errors.OperationFailure as e:
            print("Operation failure:", e)
            return False # here we can return like you have provide the wrong host or port
        except Exception as e:
            print("Unexpected error:", e)
            return False # here we can return like you have provide the wrong host or port
    else:
        return False # here we can return like you have provided some null values
    
def validate_minio_credentials(endpoint,access_key,secret_key,bucket_name):
    valid_minio_config = check_minio_config(endpoint,access_key,secret_key,bucket_name)
    if valid_minio_config:
        minio_url = valid_minio_config.get("endpoint")
        access_key= valid_minio_config.get("access_key")
        secret_key = valid_minio_config.get("secret_key")
        try:
            s3_client = boto3.client(
                "s3",
                endpoint_url=minio_url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                verify=False
            )
            s3_client.list_buckets()
            return True
        except NoCredentialsError:
            print("Credentials not available.")
        except PartialCredentialsError:
            print("Incomplete credentials provided.")
        except EndpointConnectionError:
            print("Unable to connect to the provided endpoint.")
        except Exception as e:
            print(f"An unexpected error occurred: {str(e)}")

def validate_kafka_topic(kafka_topic):
    if kafka_topic:
        kafka_topic = kafka_topic.strip()
        kafka_topic = kafka_topic.replace(" ", "_")
        return kafka_topic
    else:
        return None

def validate_log_credentials(remote_server,server_port,remote_filepath,username,password,store_key,micro_service_name):
    valid_log_config = check_log_config(remote_server,server_port,remote_filepath,username,password,store_key,micro_service_name)
    if valid_log_config:
        try:
            sftp = None
            key_file = None
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            if password:
                ssh_client.connect(remote_server, port=server_port, username=username, password=password)
            else:
                key = paramiko.RSAKey.from_private_key_file(key_file)
                ssh_client.connect(remote_server, port=server_port, username=username, pkey=key)
            sftp = ssh_client.open_sftp()
            sftp.stat(remote_filepath)
            return True
        except paramiko.AuthenticationException as auth_error:
            print(f"Authentication failed: {auth_error}")
            return None
        except paramiko.SSHException as ssh_error:
            print(f"SSH connection failed: {ssh_error}")
            return None
        except FileNotFoundError:
            print(f"Path '{remote_filepath}' does not exist.")
            return None 
        except Exception as e:
            print(f"Failed to create SSH client: {e}")
            return None
        # finally:
        #     sftp.close()
    else:
        return None
    

def validate_connector_credentials(source, destination, source_credentials, destination_credentials):
    # success = False
    # message = ""

    # if source == 'mongo':
    #     if not validate_mongo_credentials(**source_credentials):
    #         message = "Mongo credentials validation failed"
    #         return False, message
    #     else:
    #         success = True

    # elif source == 'log':
    #     if not validate_log_credentials(**source_credentials):
    #         message = "Log credentials validation failed"
    #         return False, message
    #     else:
    #         success = True

    # else:
    #     message = f"Unsupported source: {source}"
    #     return False, message

    # if destination == 'minio':
    #     if not validate_minio_credentials(**destination_credentials):
    #         message = "Minio credentials validation failed"
    #         return False, message
    #     else:
    #         success = True

    # else:
    #     message = f"Unsupported destination: {destination}"
    #     return False, message

    # return success, "Credentials validation successful"
    return True, "Credentials validation successful"



async def kafka_producer(connector_request: ConnectorRequest, token_data: TokenData = Depends(validate_token)):
    if token_data:
        try: 
            roles = token_data["realm_access"]["roles"]
            logger.info("roles of the user received successfully")
            if "user" not in roles:
                logger.error("User dont have permission to make this request")
                raise HTTPException(status_code=403, detail="You Dont have permission to make a request")
            else:
                # sources = ["mongo","log"]
                connector_name = connector_request.connector_name
                source_type = connector_request.source.type
                source_config = dict(connector_request.source.config)
                destination_type = connector_request.destination.type
                destination_config = dict(connector_request.destination.config)
                trino = dict(connector_request.trino)
                
                valid_connector = connector_checker(connector_name)
              
                if valid_connector:
                    valid_source, source_type = vaildate_source_type(source_type)    
                    if valid_source :
                        valid_destination, destination_type = validate_destination_type(destination_type)
                        if valid_destination:
                            success, validation_message = validate_connector_credentials(source_type, destination_type, source_config, destination_config)
                            if not success:
                                return {"message": validation_message}
                            else: 
                                kafka_data = {
                                    "connector_name":connector_name,
                                    "source_type":source_type,
                                    "source":source_config,
                                    "destination_type":destination_type,
                                    "destination":destination_config,
                                    # "kafka_topic":kafka_topic,
                                    "request_id":generate_uuid(),
                                    "trino": trino
                                }
                                data_json = orjson.dumps(kafka_data)
                                producer.produce(kafka_topic_in_airflow, value=data_json, callback=delivery_report)
                                producer.flush()
                                return {"request_id":kafka_data["request_id"]}
                        else:
                            return {"message": "You have provided invalid destination type "}
                    else:
                        return {"message": "You have provided ivalid source type"}
                else:
                    return {"message": "You have provided ivalid connector type"}
                                
        except KeyError as key:
            error_message = f"Key {key} not found in the decoded access_key"
            logger.error(error_message)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=error_message,
            )
        except Exception as e:
            logger.error(f"An unexpected error occured {e}")
            error_message = f"some thing happend {e}"
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=error_message,
            )
    else:
        logger.info("Received nothing by doecoding the access_token")
        
def get_user_status(request_id: str):
    user_data =  users_collection.find_one({"request_id": request_id})
    return user_data

def get_dag_data(request_id: str = Path(...),token_data: TokenData = Depends(validate_token)):
    if token_data:
        try: 
            roles = token_data["realm_access"]["roles"]
            logger.info("roles of the user received successfully")
            if "user" not in roles:
                logger.error("User dont have permission to make this request")
                raise HTTPException(status_code=403, detail="You Dont have permission to make a request")
            else:
                request_id = request_id
                airflow_dag_data = user_status_collection.find_one({"request_id":request_id})
                if airflow_dag_data is not None:
                    dag_id = airflow_dag_data["dag_id"]
                    run_id = airflow_dag_data["run_id"]
                    if dag_id and run_id:
                        data_to_stop_dag = {"dag_id":dag_id,"run_id":run_id}
                        return data_to_stop_dag
                    else:
                        return {"message":"The data movement is not yet started in order to stop or know the status"}
                else:
                    return {"message":"You have provided wrong request id to stop the job or to know the status"}
        except KeyError as key:
            error_message = f"Key {key} not found in the decoded access_key"
            logger.error(error_message)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=error_message,
            )
        except Exception as e:
            logger.error(f"An unexpected error occured {e}")
            error_message = f"some thing happend {e}"
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=error_message,
            )
    else:
        logger.info("Received nothing by doecoding the access_token")
            
    
def generate_uuid():
    """
    Generate a UUID and return it as a string.
    """
    new_uuid = uuid.uuid4()
    return str(new_uuid)