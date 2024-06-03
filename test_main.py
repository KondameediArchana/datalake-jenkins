# Author Name : Chidarapu Gopi Krishna

from fastapi.testclient import TestClient
from main import app
from utilities import get_token, UserData

client = TestClient(app)

def test_valid_login():
    response = client.post("/user-login",json={"username":"gopi","password":"gopi"},)
    assert response.status_code == 200
    access_token = response.json()
    assert access_token is not None
    
def test_invalid_login():
    response = client.post("/user-login",json={"username":"invalid_user","password":"invalid_password"},)
    assert response.status_code == 401
    data = response.json()
    assert data['detail']['error_description'] == "Invalid user credentials"
    
def test_valid_submit_job():
    response = client.post("/user-login",json={"username":"gopi","password":"gopi"},)    
    access_token = response.json()
    headers = {"Authorization": f"Bearer {access_token}"}
    json_data={
        "connector_name": "mongo-minio",
        "source": {
            "type": "mongo",
            "config": {
                "config_key":"config_values"
            }
        },
        "destination": {
            "type": "minio",
            "config": {
                "config_key":"config_values"
            }
        }
        }
    response = client.post("/submit-job", headers=headers, json=json_data)
    assert response.status_code == 200
    assert "request_id" in response.json()
    
def test_invalid_submit_job():
    response = client.post("/user-login",json={"username":"gopi","password":"gopi"},)
    access_token = response.json()
    headers = {"Authorization": f"Bearer {access_token}"}
    json_data={
        "connector_name": "mongo-minio",
        "source": {
            "type": "mongo"
        },
        "destination": {
            "type": "minio",
            "config": {
                "config_key":"config_values"
            }
        }
        }
    response = client.post("/submit-job", headers=headers, json=json_data)
    assert response.status_code == 422
    
def test_valid_get_token_func():
    user_data = UserData(username="gopi",
                    password="gopi")
    data = get_token(user_data)
    assert "access_token" in data
    assert "refresh_token" in data
    assert "session_state" in data
    
def test_invalid_get_token_func():
    user_data = UserData(username="",
                    password="")
    data = get_token(user_data)
    assert "error" in data
    

        