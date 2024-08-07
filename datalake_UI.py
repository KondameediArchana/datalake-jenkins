import streamlit as st
import requests
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import warnings
from urllib3.exceptions import InsecureRequestWarning

warnings.simplefilter('ignore', InsecureRequestWarning)

# Set page configuration
st.set_page_config(page_title="User Login", page_icon="🔐", layout="centered")

# Initialize session state variables
if "logged_in" not in st.session_state:
    st.session_state["logged_in"] = False
if "page" not in st.session_state:
    st.session_state["page"] = "login"
if "token" not in st.session_state:
    st.session_state["token"] = None
if "service_selected" not in st.session_state:
    st.session_state["service_selected"] = None

# Credentials and URLs
KAFKA_BOOTSTRAP_SERVER = "192.168.63.186:30751"
AIRFLOW_API_URL = "http://192.168.63.186:31151/api/v1/dags"
AIRFLOW_USERNAME = "admin"
AIRFLOW_PASSWORD = "admin"
NIFI_API_URL = "https://192.168.63.186:30555"
NIFI_USERNAME = "admin"
NIFI_PASSWORD = "ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB"
USER_LOGIN_URL = "http://192.168.63.186:32221/user-login"

# NiFi functions
def get_access_token(nifi_host, username, password, verify_ssl=False):
    token_endpoint = f"{nifi_host}/nifi-api/access/token"
    data = {'username': username, 'password': password}
    
    try:
        response = requests.post(token_endpoint, data=data, verify=verify_ssl)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        st.error(f"Error obtaining access token: {e}")
        return None
    
def get_root_process_group_id(nifi_host, token, verify_ssl=False):
    controller_url = f"{nifi_host}/nifi-api/flow/process-groups/root"
    headers = {'Authorization': f'Bearer {token}'}
    
    try:
        response = requests.get(controller_url, headers=headers, verify=verify_ssl)
        response.raise_for_status()
        data = response.json()
        return data['processGroupFlow']['id']
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to get root process group ID: {e}")
        return None

def get_process_groups_by_id(nifi_host, token, process_group_id, verify_ssl=False):
    process_group_endpoint = f"{nifi_host}/nifi-api/flow/process-groups/{process_group_id}"
    headers = {'Authorization': f'Bearer {token}'}

    try:
        response = requests.get(process_group_endpoint, headers=headers, verify=verify_ssl)
        response.raise_for_status()
        process_group = response.json()

        # Extract process groups from the response
        process_groups = process_group.get('processGroupFlow', {}).get('flow', {}).get('processGroups', [])
        return process_groups
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching process groups: {e}")
        return []

def get_number_of_processors(nifi_host, token, process_group_id, verify_ssl=False):
    process_group_endpoint = f"{nifi_host}/nifi-api/flow/process-groups/{process_group_id}"
    headers = {'Authorization': f'Bearer {token}'}

    try:
        response = requests.get(process_group_endpoint, headers=headers, verify=verify_ssl)
        response.raise_for_status()
        process_group = response.json()

        # Extract processors from the response
        processors = process_group.get('processGroupFlow', {}).get('flow', {}).get('processors', [])
        num_processors = len(processors)
        
        return num_processors
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching number of processors: {e}")
        return 0

# Login function
def login(username, password):
    try:
        response = requests.post(USER_LOGIN_URL, auth=(username, password))
        if response.status_code == 200:
            response_data = response.json()
            if 'access_token' in response_data:
                st.session_state["token"] = response_data['access_token']
                st.session_state["logged_in"] = True
                st.session_state["page"] = "main"
            else:
                st.error("No access token received")
        else:
            st.error("Invalid username or password")
    except Exception as e:
        st.error(f"An error occurred: {e}")

def check_kafka_status():
    try:
        consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER, client_id='status-check')
        topics = consumer.topics()
        consumer.close()
        return "Running" if topics else "Not Running"
    except KafkaError as e:
        return f"Error: {e}"
    except Exception as e:
        return f"Exception: {e}"

def get_kafka_topics():
    try:
        consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER, client_id='status-check')
        topics = consumer.topics()
        consumer.close()
        return topics
    except KafkaError as e:
        return f"Error: {e}"
    except Exception as e:
        return f"Exception: {e}"

def get_topic_records(topic, num_records=10):
    try:
        consumer = KafkaConsumer(topic, bootstrap_servers=KAFKA_BOOTSTRAP_SERVER, auto_offset_reset='earliest', consumer_timeout_ms=1000)
        records = []
        for message in consumer:
            records.append(message.value.decode('utf-8'))
            if len(records) >= num_records:
                break
        consumer.close()
        return records
    except KafkaError as e:
        return [f"Error: {e}"]
    except Exception as e:
        return [f"Exception: {e}"]

def check_airflow_status():
    try:
        response = requests.get(AIRFLOW_API_URL, auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD))
        if response.status_code == 200:
            return "Running"
        else:
            return "Not Running"
    except Exception as e:
        return str(e)

def get_airflow_dags():
    try:
        response = requests.get(AIRFLOW_API_URL, auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD))
        if response.status_code == 200:
            try:
                dags = response.json()
                return dags['dags'] if 'dags' in dags else []
            except ValueError:
                return "Error: Response is not in JSON format."
        else:
            return f"Error: Unable to fetch DAGs, status code: {response.status_code}"
    except Exception as e:
        return f"Exception: {e}"
    
def check_nifi_status():
    try:
        response = requests.get(NIFI_API_URL, auth=(NIFI_USERNAME, NIFI_PASSWORD), verify=False)
        if response.status_code == 200:
            return "Running"
        else:
            return "Not Running"
    except Exception as e:
        return str(e)

# Login page
def login_page():
    st.markdown('<div class="login-container">', unsafe_allow_html=True)
    st.markdown('<div class="login-header">User Login</div>', unsafe_allow_html=True)
    
    username = st.text_input("Username", key="username", placeholder="Username", help="Enter your username here")
    password = st.text_input("Password", key="password", type="password", placeholder="Password", help="Enter your password here")
    
    if st.button("Login", key="login"):
        login(username, password)
    
    st.markdown('</div>', unsafe_allow_html=True)

# Main application page
def main_page():
    st.title("Data Processing")
    st.write("Choose an option below:")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("Batch Data", key= "batch_data"):
            st.session_state["page"] = "batch_data"
            # batch_data_page()

    
    with col2:
        if st.button("Streaming Data", key="streaming_data"):
            st.write("You chose Streaming Data")

# Batch data page
def batch_data_page():
    st.title("Batch Data Jobs Status")
    
    # Sidebar for service selection
    with st.sidebar:
        st.subheader("Services")
        if st.button("Kafka", key="kafka"):
            st.session_state["service_selected"] = "kafka"
        if st.button("Airflow", key="Airflow"):
            st.session_state["service_selected"] = "airflow"
        if st.button("NiFi", key="NiFi"):
            st.session_state["service_selected"] = "nifi"

    if st.session_state["service_selected"] == "kafka":
        st.subheader("Kafka Status")
        kafka_status = check_kafka_status()
        st.write(f"Kafka Server Status: {kafka_status}")

        st.subheader("Kafka Topics")
        topics = get_kafka_topics()
        if isinstance(topics, str):
            st.write(topics)
        else:
            selected_topic = st.selectbox("Select a Kafka Topic", options=topics)

            if selected_topic:
                num_records = st.slider("Number of Records", min_value=1, max_value=2000, value=10)
                records = get_topic_records(selected_topic, num_records)
                for record in records:
                    st.write(record)

    elif st.session_state["service_selected"] == "airflow":
        st.subheader("Airflow DAGs")
        dags = get_airflow_dags()
        if isinstance(dags, str):
            st.write(dags)
        else:
            if not dags:
                st.write("No DAGs found or an error occurred.")
            else:
                st.write("Available DAGs:")
                for dag in dags:
                    st.write(f"- {dag['dag_id']}")

    elif st.session_state["service_selected"] == "nifi":
        st.subheader("NiFi Status")
        nifi_status = check_nifi_status()
        st.write(f"NiFi Server Status: {nifi_status}")

        st.subheader("NiFi Process Groups")
        token = get_access_token(NIFI_API_URL, NIFI_USERNAME, NIFI_PASSWORD, verify_ssl=False)
        if token:
            root_process_id = get_root_process_group_id(NIFI_API_URL, token, verify_ssl=False)
            if root_process_id:
                process_groups = get_process_groups_by_id(NIFI_API_URL, token, root_process_id, verify_ssl=False)
                if process_groups:
                    for group in process_groups:
                        st.write(f"- {group['component']['name']} (ID: {group['component']['id']})")
                        num_processors = get_number_of_processors(NIFI_API_URL, token, group['id'], verify_ssl=False)
                        st.write(f"Number of processors in Process Group {group['component']['name']}: {num_processors}")
                else:
                    st.write("No process groups found or an error occurred.")
            else:
                st.write("Failed to obtain root process group ID.")
        else:
            st.write("Failed to obtain access token.")

    if st.button("Back", key="back"):
        st.session_state["page"] = "main"

# Display the appropriate page based on the session state
if not st.session_state["logged_in"]:
    login_page()
else:
    if st.session_state["page"] == "main":
        main_page()
    elif st.session_state["page"] == "batch_data":
        batch_data_page()



