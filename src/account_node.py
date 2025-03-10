import socket
import json
import threading
import time
import os

class AccountNode:
    def __init__(self, node_id, port, coordinator_port=5010):
        self.node_id = node_id
        self.port = port
        self.coordinator_port = coordinator_port
        self.balance = 0
        self.transaction_history = []
        self.lock = threading.Lock()
        self.data_file = f"{self.node_id}_data.json"
        
        # Load data if exists
        self.load_data()
        
        # Start server
        self.server_thread = threading.Thread(target=self.start_server)
        self.server_thread.daemon = True
        self.server_thread.start()
        
        # Start heartbeat
        self.heartbeat_thread = threading.Thread(target=self.send_heartbeat)
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()
        
        print(f"Account Node {self.node_id} started on port {self.port} with balance {self.balance}")
    
    def load_data(self):
        if os.path.exists(self.data_file):
            try:
                with open(self.data_file, 'r') as f:
                    data = json.load(f)
                    self.balance = data.get('balance', 0)
                    self.transaction_history = data.get('transaction_history', [])
            except Exception as e:
                print(f"Error loading data: {e}")
    
    def save_data(self):
        data = {
            'balance': self.balance,
            'transaction_history': self.transaction_history
        }
        with open(self.data_file, 'w') as f:
            json.dump(data, f, indent=2)
    
    def start_server(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(('localhost', self.port))
        server.listen(5)
        
        while True:
            client, addr = server.accept()
            client_thread = threading.Thread(target=self.handle_request, args=(client,))
            client_thread.daemon = True
            client_thread.start()
    
    def handle_request(self, client):
        try:
            data = client.recv(4096)
            if not data:
                return
            
            request = json.loads(data.decode('utf-8'))
            command = request.get('command')
            
            response = {'status': 'error', 'message': 'Unknown command'}
            
            if command == 'get_balance':
                with self.lock:
                    response = {
                        'status': 'success', 
                        'balance': self.balance
                    }
            
            elif command == 'prepare_transfer':
                # Phase 1 of 2PC
                amount = request.get('amount', 0)
                is_sender = request.get('is_sender', False)
                
                with self.lock:
                    if is_sender and self.balance < amount:
                        response = {
                            'status': 'error',
                            'message': 'Insufficient funds'
                        }
                    else:
                        response = {
                            'status': 'success',
                            'message': 'Ready to transfer'
                        }
            
            elif command == 'execute_transfer':
                # Phase 2 of 2PC
                transaction_id = request.get('transaction_id')
                amount = request.get('amount', 0)
                is_sender = request.get('is_sender', False)
                
                with self.lock:
                    if is_sender:
                        self.balance -= amount
                    else:
                        self.balance += amount
                    
                    # Record transaction
                    self.transaction_history.append({
                        'transaction_id': transaction_id,
                        'amount': amount if not is_sender else -amount,
                        'timestamp': time.time()
                    })
                    
                    # Save data
                    self.save_data()
                    
                    response = {
                        'status': 'success',
                        'message': 'Transfer executed',
                        'new_balance': self.balance
                    }
            
            elif command == 'heartbeat':
                response = {
                    'status': 'success',
                    'node_id': self.node_id
                }
            
            elif command == 'init_balance':
                amount = request.get('amount', 0)
                with self.lock:
                    self.balance = amount
                    self.save_data()
                    response = {
                        'status': 'success',
                        'message': f'Balance initialized to {amount}',
                        'balance': self.balance
                    }
            
            client.send(json.dumps(response).encode('utf-8'))
        
        except Exception as e:
            print(f"Error handling request: {e}")
        finally:
            client.close()
    
    def send_heartbeat(self):
        while True:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect(('localhost', self.coordinator_port))
                    heartbeat = {
                        'command': 'heartbeat',
                        'node_id': self.node_id,
                        'node_type': 'account',
                        'port': self.port
                    }
                    s.send(json.dumps(heartbeat).encode('utf-8'))
                    # No need to wait for response
            except Exception as e:
                print(f"Failed to send heartbeat: {e}")
            
            time.sleep(5)  # Send heartbeat every 5 seconds

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 3:
        print("Usage: python account_node.py <node_id> <port> [coordinator_port]")
        sys.exit(1)
    
    node_id = sys.argv[1]
    port = int(sys.argv[2])
    coordinator_port = int(sys.argv[3]) if len(sys.argv) > 3 else 5000
    
    node = AccountNode(node_id, port, coordinator_port)
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"Account Node {node_id} shutting down...")
