import socket
import threading
import json
import time
from hash_ring import HashRing
import docker

# Define the KV stores and their addresses
kv_stores = {
    "kvstore1": "127.0.0.1:5001",
    "kvstore2": "127.0.0.1:5002",
    "kvstore3": "127.0.0.1:5003",
}

# Create a consistent hash ring
ring = HashRing(kv_stores.values())

# Docker client
docker_client = docker.from_env()

# Define the KV store container image (assumed to be available)
kv_store_image = "your-kv-store-image:latest"

# Initialize the data stores for each KV store
data_stores = {store: {} for store in kv_stores}

# Periodically save data to a file in each KV store (e.g., every 10 minutes)
def save_data_to_file(kv_store_name):
    while True:
        time.sleep(600)  # 10 minutes
        with open(f"{kv_store_name}_data.json", "w") as file:
            json.dump(data_stores[kv_store_name], file)

# Start data-saving threads for each KV store
data_saver_threads = {}
for kv_store_name in kv_stores:
    data_saver_thread = threading.Thread(target=save_data_to_file, args=(kv_store_name,))
    data_saver_thread.daemon = True
    data_saver_thread.start()
    data_saver_threads[kv_store_name] = data_saver_thread

# Lock for controlling access to the data dictionaries of each KV store
data_locks = {store: threading.Lock() for store in kv_stores}

# PUT operation
def put(key, value):
    store = ring.get_node(key)
    with data_locks[store]:
        data_stores[store][key] = value

# DEL operation
def delete(key):
    store = ring.get_node(key)
    with data_locks[store]:
        if key in data_stores[store]:
            del data_stores[store][key]

# GET operation
def get(key):
    store = ring.get_node(key)
    with data_locks[store]:
        if key in data_stores[store]:
            return data_stores[store][key]
        else:
            return "Key not found"

# Main server logic
def handle_client(client_socket):
    while True:
        request = client_socket.recv(1024).decode("utf-8")
        if not request:
            break

        # Parse the request (e.g., "GET key", "PUT key value", "DEL key")
        parts = request.split()
        command = parts[0]

        if command == "GET":
            key = parts[1]
            response = get(key)
        elif command == "PUT":
            key = parts[1]
            value = " ".join(parts[2:])
            put(key, value)
            response = "OK"
        elif command == "DEL":
            key = parts[1]
            delete(key)
            response = "OK"
        else:
            response = "Invalid command"

        client_socket.send(response.encode("utf-8")

# Create a socket, bind it to a port, and listen for incoming connections
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(("0.0.0.0", 12345))  # Use the desired port
server.listen(5)

print("Server listening on port 12345...")

while True:
    client, addr = server.accept()
    client_handler = threading.Thread(target=handle_client, args=(client,))
    client_handler.start()
