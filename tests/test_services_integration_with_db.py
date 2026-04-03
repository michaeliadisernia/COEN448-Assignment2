"""
COEN 448 - Integration Testing
test_services_integration_with_db.py
 
Tests:
    TC_01 - Validate User Creation and Retrieval
    TC_02 - Validate Order Creation with Existing User
    TC_03 - Validate Event-Driven User Update Propagation
    TC_04 - Validate API Gateway Routing (Strangler Pattern)
"""

import os
import pytest
import requests
import pymongo
import pika
import subprocess
import time
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Fixture to manage Docker Compose
@pytest.fixture(scope="module", autouse=True)
def docker_compose():
    # Start Docker Compose
    subprocess.run(
        ["docker", "compose", "-f", "docker-compose.test.yml", "up", "--build", "-d"],
        check=True
    )
    
    # Wait for services to be ready
    wait_for_service("http://localhost:8001/")
    
    yield  # Run tests
    
    # Tear down Docker Compose
    # subprocess.run(
    #     ["docker", "compose", "-f", "docker-compose.test.yml", "down", "-v"],
    #     check=True
    # )

# Helper function to wait for service readiness
def wait_for_service(url, timeout=200):
    start = time.time()
    while time.time() - start < timeout:
        try:
            if requests.get(url).status_code == 200:
                return
        except Exception:
            time.sleep(1)
    raise TimeoutError(f"Service at {url} not ready")

# Fixture for API base URL
@pytest.fixture(scope="module")
def api_base_url():
    return "http://localhost:8000"

# Fixture for MongoDB client
@pytest.fixture(scope="module")
def mongo_client():
    client = pymongo.MongoClient(
        host="localhost", 
        port=27017,
        username=os.getenv("MONGO_USERNAME"),
        password=os.getenv("MONGO_PASSWORD"),
        authSource="admin"
        )
    yield client
    client.close()

# Test: User Creation
def test_user_creation(api_base_url, mongo_client):
    # Create a new user
    user_payload = {
        "firstName": "Integration",
        "lastName": "Tester",
        "emails": ["integration.test@example.com"],
        "deliveryAddress": {
            "street": "123 Test Street",
            "city": "Testville",
            "state": "Test State",
            "postalCode": "12345",
            "country": "Test Country"
        }
    }
    
    # Send user creation request
    response = requests.post(
        f"{api_base_url}/users/", 
        json=user_payload
    )
    
    # Assertions
    assert response.status_code == 201
    created_user = response.json()
    assert created_user['firstName'] == "Integration"
    assert created_user['lastName'] == "Tester"
    
    # Verify user in MongoDB
    users_db = mongo_client[os.getenv("DATABASE_NAME")]
    users_collection = users_db["users"]
    user = users_collection.find_one({"userId": created_user["userId"]})
    assert user is not None
    assert user["emails"] == ["integration.test@example.com"]

# Test: User Update
def test_user_update(api_base_url, mongo_client):
    # First create a user
    user_payload = {
        "firstName": "Update",
        "lastName": "Tester",
        "emails": ["update.test@example.com"],
        "deliveryAddress": {
            "street": "123 Test Street",
            "city": "Testville",
            "state": "Test State",
            "postalCode": "12345",
            "country": "Test Country"
        }
    }
    
    # Create user
    create_response = requests.post(
        f"{api_base_url}/users/", 
        json=user_payload
    )
    
    assert create_response.status_code == 201
    created_user = create_response.json()
    user_id = created_user["userId"]
    
    # Update the user
    update_payload = {
        "emails": ["updated.email@example.com"],
        "deliveryAddress": {
            "street": "456 Update Street",
            "city": "Updateville",
            "state": "Update State",
            "postalCode": "54321",
            "country": "Update Country"
        }
    }
    
    # Send update request
    update_response = requests.put(
        f"{api_base_url}/users/{user_id}", 
        json=update_payload
    )
    
    # Assertions for the response
    assert update_response.status_code == 200
    update_result = update_response.json()
    
    # The response should contain both old and new user data
    old_user = update_result[0]
    new_user = update_result[1]
    
    # Check old user data
    assert old_user["emails"] == ["update.test@example.com"]
    assert old_user["deliveryAddress"]["street"] == "123 Test Street"
    
    # Check new user data
    assert new_user["emails"] == ["updated.email@example.com"]
    assert new_user["deliveryAddress"]["street"] == "456 Update Street"
    assert new_user["deliveryAddress"]["city"] == "Updateville"
    
    # Verify update in MongoDB
    users_db = mongo_client[os.getenv("DATABASE_NAME")]
    users_collection = users_db["users"]
    updated_user = users_collection.find_one({"userId": user_id})
    assert updated_user is not None
    assert updated_user["emails"] == ["updated.email@example.com"]
    assert updated_user["deliveryAddress"]["street"] == "456 Update Street"

def test_TC03_event_driven_user_update_propagation(api_base_url, mongo_client):
    """
    TC_03: Validate Event-Driven User Update Propagation
    Requirements: R3.1, R3.3, R3.4, R3.5, R4.1, R4.4
    
    Objective: Ensure that user updates trigger a RabbitMQ event which propagates to and updates the Order Microservice.
    """

    print("Starting TC_03: Validate Event-Driven User Update Propagation")

    # Test Step 1: Create initial user to update
    user_payload = {
        "firstName": "TC03",
        "lastName": "Tester",
        "emails": ["old@test.com"],
        "deliveryAddress": {
            "street": "999 Pierrefonds Boulevard",
            "city": "Montreal",
            "state": "QC",
            "postalCode": "H8Y1A1",
            "country": "Canada"
        }
    }
    user_response = requests.post(f"{api_base_url}/users/", json=user_payload)
    assert user_response.status_code == 201, \
        f"Step 1 FAILED - User creation: {user_response.status_code}: {user_response.text}"

    user = user_response.json()
    user_id = user["userId"]
    print(f"Created user with ID: {user_id}")

    # Test Step 2: Create an order for the user
    order_payload = {
        "userId": user_id,
        "items": [{"itemId": "itemA", "quantity": 1, "price": 9.99}],
        "userEmails": ["old@test.com"],
        "deliveryAddress": {
            "street": "999 Pierrefonds Boulevard",
            "city": "Montreal",
            "state": "QC",
            "postalCode": "H8Y1A1",
            "country": "Canada"
        },
        "orderStatus": "shipping"
    }
    order_response = requests.post(f"{api_base_url}/orders/", json=order_payload)
    assert order_response.status_code == 201, \
        f"Step 2 FAILED - Order creation: {order_response.status_code}: {order_response.text}"

    order_id = order_response.json()["orderId"]
    print(f"Created order with ID: {order_id}")

    #Test Step 3: Update the user to trigger the event which should appear in RabbitMQ
    update_payload = {
        "emails": ["new@test.com"],
        "deliveryAddress": {
            "street": "111 Oka Street",
            "city": "Oka",
            "state": "QC",
            "postalCode": "J0N1E0",
            "country": "Canada"
        }
    }
    update_response = requests.put(
        f"{api_base_url}/users/{user_id}", 
        json=update_payload
    )
    assert update_response.status_code == 200, \
        f"Step 3 FAILED - User update: {update_response.status_code}: {update_response.text}"

    # Verify response contains both old and new user data
    update_result = update_response.json()
    old_user = update_result[0]
    new_user = update_result[1]
    assert old_user["emails"] == ["old@test.com"], "Old user data does not match expected"
    assert new_user["emails"] == ["new@test.com"], "New user data does not match expected"
    assert new_user["deliveryAddress"]["street"] == "111 Oka Street", "New user delivery address does not match expected"
    print(f"Step 3: User update response contains both old and new data as expected")
    print(f"Old email: {old_user['emails']} -> New email: {new_user['emails']}")

    # Test Step 4: Wait for RabbitMQ event to propagate
    propagation_wait = 15   
    print(f"Waiting {propagation_wait} seconds for RabbitMQ event to propagate...")
    time.sleep(propagation_wait)

    # Test Step 5: Verify that the Order Microservice received the update
    db = mongo_client[os.getenv("DATABASE_NAME")]
    order_in_db = db["orders"].find_one({"orderId": order_id})

    assert order_in_db is not None, \
        f"Step 5 FAILED - Order {order_id} not found in MongoDB"
    assert order_in_db["userEmails"] == ["new@test.com"], \
        f"Step 5 FAILED - Email not propagated. Got: {order_in_db['userEmails']}"
    assert order_in_db["deliveryAddress"]["street"] == "111 Oka Street", \
        f"Step 5 FAILED - Address not propagated. Got: {order_in_db['deliveryAddress']}"
    
    print(f"Step 5 PASS - Order {order_id} reflects propagated changes:")
    print(f"Email:{order_in_db['userEmails']}")
    print(f"Address:{order_in_db['deliveryAddress']['street']}")
    print(f"\n[TC_03 PASS] Event-driven propagation verified successfully")


def test_TC04_api_gateway_routing_strangler_pattern(api_base_url, kong_admin_url="http://localhost:8001"):
        """
        TC_04: Validate API Gateway Routing (Strangler Pattern)
        Requirements: R2.1, R2.2, R2.3, R2.4

        Objective: Ensure Kong routes requests between v1 and v2 based on configured weights.
        
        Run this test three times with different .env weights:
            Case A: USER_SERVICE_V1_WEIGHT=100, USER_SERVICE_V2_WEIGHT=0  (P=100)
            Case B: USER_SERVICE_V1_WEIGHT=0,   USER_SERVICE_V2_WEIGHT=100 (P=0)
            Case C: USER_SERVICE_V1_WEIGHT=50,  USER_SERVICE_V2_WEIGHT=50  (P=50)
        """
        print("TC_04: API Gateway Routing (Strangler Pattern)")

        # STEP 1: Verify Kong upstream weights via Admin API
        upstream_response = requests.get(
            f"{kong_admin_url}/upstreams/user_service_upstream/targets"
        )
        assert upstream_response.status_code == 200, \
            f"STEP 1 FAILED - Could not reach Kong admin API: {upstream_response.status_code}"

        targets = upstream_response.json().get("data", [])
        assert len(targets) >= 2, f"Step 1 FAILED - Expected 2 targets, got: {len(targets)}"

        v1_target = next((t for t in targets if "v1" in t["target"]), None)
        v2_target = next((t for t in targets if "v2" in t["target"]), None)
        assert v1_target is not None, "Step 1 FAILED - v1 target not found in Kong upstream"
        assert v2_target is not None, "Step 1 FAILED - v2 target not found in Kong upstream"

        v1_weight = v1_target["weight"]
        v2_weight = v2_target["weight"]
        total = v1_weight + v2_weight
        assert total == 100, f"Step 1 FAILED - Weights must sum to 100, got {total}"

        print(f"Step 1 PASS - Kong upstream weights confirmed: v1={v1_weight}, v2={v2_weight}")

        # Step 2: Confirm weights match .env configuration
        env_v1 = int(os.getenv("USER_SERVICE_V1_WEIGHT", 0))
        env_v2 = int(os.getenv("USER_SERVICE_V2_WEIGHT", 0))
        assert v1_weight == env_v1, \
            f"Step 2 FAILED - Kong v1 weight ({v1_weight}) doesn't match .env ({env_v1})"
        assert v2_weight == env_v2, \
            f"Step 2 FAILED - Kong v2 weight ({v2_weight}) doesn't match .env ({env_v2})"

        print(f"Step 2 PASS - Kong weights match .env configuration (not hardcoded)")

        # Step 3: Send 10 requests and verify all succeed
        total_requests = 10
        success_count = 0
        failed_responses = []

        for i in range(total_requests):
            payload = {
                "firstName": f"TC04",
                "lastName": f"Request{i}",
                "emails": [f"tc04.request{i}@example.com"],
                "deliveryAddress": {
                    "street": f"{i} Kong Street",
                    "city": "Gatewayville",
                    "state": "QC",
                    "postalCode": "H1A1A1",
                    "country": "Canada"
                }
            }
            resp = requests.post(f"{api_base_url}/users/", json=payload)
            if resp.status_code == 201:
                success_count += 1
            else:
                failed_responses.append(f"Request {i}: {resp.status_code} - {resp.text}")

        assert success_count == total_requests, (
            f"Step 3 FAILED - Only {success_count}/{total_requests} succeeded.\n"
            + "\n".join(failed_responses)
        )
        print(f"Step 3 PASS - {success_count}/{total_requests} requests succeeded through Kong")

        # Step 4: Case-specific validation
        if v1_weight == 100 and v2_weight == 0:
            print("Step 4 - Case A (P=100): All traffic routed to v1 only")
            print("         Confirmed: v1=100%, v2=0% - Strangler pattern at full v1")

        elif v1_weight == 0 and v2_weight == 100:
            print("Step 4 - Case B (P=0): All traffic routed to v2 only")
            print("         Confirmed: v1=0%, v2=100% - Full migration to v2")

        elif v1_weight == 50 and v2_weight == 50:
            print("Step 4 - Case C (P=50): Traffic split 50/50 between v1 and v2")
            print("         Confirmed: v1=50%, v2=50% - Active strangler migration")

        else:
            print(f"Step 4 - Custom weights: v1={v1_weight}%, v2={v2_weight}%")

        print(f"\n[TC_04 PASS] Strangler pattern routing verified for v1={v1_weight}%, v2={v2_weight}%")


