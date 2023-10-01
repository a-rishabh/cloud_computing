import socket
import threading
import json
import time

data = {}  # In-memory data store

# Load data from a file (if it exists) during server startup
try:
    with open("data.json", "r") as file:
        data = json.load(file)
except FileNotFoundError:
    print("File does not exist")
    

# Periodically save data to a file (e.g., every 10 minutes)
def save_data_to_file():
    while True:
        time.sleep(600)  # 10 minutes
        with open("data.json", "w") as file:
            json.dump(data, file)

# Start the data-saving thread
data_saver_thread = threading.Thread(target=save_data_to_file)
data_saver_thread.daemon = True
data_saver_thread.start()


# Lock for controlling access to the data dictionary
data_lock = threading.Lock()

# PUT operation
def put(key, value):
    with data_lock:
        data[key] = value

# DEL operation
def delete(key):
    with data_lock:
        if key in data:
            del data[key]

# GET operation
def get(key):
    with data_lock:
        if key in data:
            return data[key]
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

        client_socket.send(response.encode("utf-8"))

# Create a socket, bind it to a port, and listen for incoming connections
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(("0.0.0.0", 12345))  # Use the desired port
server.listen(5)

print("Server listening on port 12345...")

while True:
    client, addr = server.accept()
    client_handler = threading.Thread(target=handle_client, args=(client,))
    client_handler.start()
