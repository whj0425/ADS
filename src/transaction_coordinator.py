import socket
import json
import threading
import time
import uuid
import os

class TransactionCoordinator:
    def __init__(self, port=5010, coordinator_id="c1"):
        self.coordinator_id = coordinator_id
        self.port = port
        self.account_nodes = {}  # {node_id: {'port': port, 'last_heartbeat': timestamp}}
        self.transactions = {}  # {transaction_id: {'status': status, 'from': node_id, 'to': node_id, 'amount': amount}}
        self.lock = threading.Lock()
        self.data_file = "coordinator_data.json"
        
        # Load data if exists
        self.load_data()
        
        # Start server
        self.server_thread = threading.Thread(target=self.start_server)
        self.server_thread.daemon = True
        self.server_thread.start()
        
        # Start monitoring thread
        self.monitor_thread = threading.Thread(target=self.monitor_nodes)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        
        print(f"Transaction Coordinator {self.coordinator_id} started on port {self.port}")
        print(f"Registered account nodes: {list(self.account_nodes.keys())}")
    
    def load_data(self):
        if os.path.exists(self.data_file):
            try:
                with open(self.data_file, 'r') as f:
                    data = json.load(f)
                    self.account_nodes = data.get('account_nodes', {})
                    self.transactions = data.get('transactions', {})
            except Exception as e:
                print(f"Error loading coordinator data: {e}")
    
    def save_data(self):
        data = {
            'account_nodes': self.account_nodes,
            'transactions': self.transactions
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
            
            if command == 'heartbeat':
                # Handle node heartbeat
                node_id = request.get('node_id')
                node_type = request.get('node_type')
                port = request.get('port')
                
                if node_type == 'account':
                    with self.lock:
                        self.account_nodes[node_id] = {
                            'port': port,
                            'last_heartbeat': time.time()
                        }
                        self.save_data()
                
                response = {
                    'status': 'success',
                    'message': 'Heartbeat received'
                }
            
            elif command == 'list_accounts':
                with self.lock:
                    response = {
                        'status': 'success',
                        'accounts': list(self.account_nodes.keys())
                    }
            
            elif command == 'transfer':
                # Handle transfer request
                from_account = request.get('from')
                to_account = request.get('to')
                amount = request.get('amount')
                
                if from_account not in self.account_nodes or to_account not in self.account_nodes:
                    response = {
                        'status': 'error',
                        'message': 'One or both accounts not found'
                    }
                else:
                    # Start 2PC protocol
                    transaction_id = str(uuid.uuid4())
                    success = self.execute_two_phase_commit(transaction_id, from_account, to_account, amount)
                    
                    if success:
                        response = {
                            'status': 'success',
                            'message': f'Transfer of {amount} from {from_account} to {to_account} completed',
                            'transaction_id': transaction_id
                        }
                    else:
                        response = {
                            'status': 'error',
                            'message': 'Transfer failed during two-phase commit'
                        }
            
            elif command == 'init_accounts':
                # Initialize accounts with initial balance
                amount = request.get('amount', 10000)
                success = True
                
                for node_id, node_info in self.account_nodes.items():
                    try:
                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                            s.connect(('localhost', node_info['port']))
                            init_request = {
                                'command': 'init_balance',
                                'amount': amount
                            }
                            s.send(json.dumps(init_request).encode('utf-8'))
                            init_response = json.loads(s.recv(4096).decode('utf-8'))
                            
                            if init_response.get('status') != 'success':
                                success = False
                                break
                    
                    except Exception as e:
                        print(f"Failed to initialize account {node_id}: {e}")
                        success = False
                        break
                
                if success:
                    response = {
                        'status': 'success',
                        'message': f'All accounts initialized with {amount}'
                    }
                else:
                    response = {
                        'status': 'error',
                        'message': 'Failed to initialize all accounts'
                    }
            
            client.send(json.dumps(response).encode('utf-8'))
        
        except Exception as e:
            print(f"Error handling request: {e}")
        finally:
            client.close()
    
    def execute_two_phase_commit(self, transaction_id, from_account, to_account, amount):
        # Phase 1: Preparation
        try:
            # Record the transaction
            with self.lock:
                self.transactions[transaction_id] = {
                    'status': 'preparing',
                    'from': from_account,
                    'to': to_account,
                    'amount': amount,
                    'timestamp': time.time()
                }
                self.save_data()
            
            # Prepare the sender
            sender_ready = self.prepare_transfer(from_account, amount, True)
            if not sender_ready:
                with self.lock:
                    self.transactions[transaction_id]['status'] = 'aborted'
                    self.save_data()
                return False
            
            # Prepare the receiver
            receiver_ready = self.prepare_transfer(to_account, amount, False)
            if not receiver_ready:
                with self.lock:
                    self.transactions[transaction_id]['status'] = 'aborted'
                    self.save_data()
                return False
            
            # Phase 2: Execution
            sender_success = self.execute_transfer(transaction_id, from_account, amount, True)
            if not sender_success:
                with self.lock:
                    self.transactions[transaction_id]['status'] = 'failed'
                    self.save_data()
                return False
            
            receiver_success = self.execute_transfer(transaction_id, to_account, amount, False)
            if not receiver_success:
                # This is a critical failure state. Money has been deducted but not added.
                # In a real system, this would require recovery mechanisms.
                with self.lock:
                    self.transactions[transaction_id]['status'] = 'inconsistent'
                    self.save_data()
                print(f"CRITICAL ERROR: Transaction {transaction_id} in inconsistent state")
                return False
            
            # Transaction completed successfully
            with self.lock:
                self.transactions[transaction_id]['status'] = 'completed'
                self.save_data()
            return True
        
        except Exception as e:
            print(f"Error in two-phase commit: {e}")
            with self.lock:
                self.transactions[transaction_id]['status'] = 'error'
                self.transactions[transaction_id]['error'] = str(e)
                self.save_data()
            return False
    
    def prepare_transfer(self, account_id, amount, is_sender):
        try:
            node_info = self.account_nodes.get(account_id)
            if not node_info:
                return False
            
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(('localhost', node_info['port']))
                prepare_request = {
                    'command': 'prepare_transfer',
                    'amount': amount,
                    'is_sender': is_sender
                }
                s.send(json.dumps(prepare_request).encode('utf-8'))
                prepare_response = json.loads(s.recv(4096).decode('utf-8'))
                
                return prepare_response.get('status') == 'success'
        
        except Exception as e:
            print(f"Error preparing transfer for {account_id}: {e}")
            return False
    
    def execute_transfer(self, transaction_id, account_id, amount, is_sender):
        try:
            node_info = self.account_nodes.get(account_id)
            if not node_info:
                return False
            
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(('localhost', node_info['port']))
                execute_request = {
                    'command': 'execute_transfer',
                    'transaction_id': transaction_id,
                    'amount': amount,
                    'is_sender': is_sender
                }
                s.send(json.dumps(execute_request).encode('utf-8'))
                execute_response = json.loads(s.recv(4096).decode('utf-8'))
                
                return execute_response.get('status') == 'success'
        
        except Exception as e:
            print(f"Error executing transfer for {account_id}: {e}")
            return False
    
    def monitor_nodes(self):
        # Check node health periodically
        while True:
            with self.lock:
                current_time = time.time()
                dead_nodes = []
                
                for node_id, node_info in self.account_nodes.items():
                    # Check if node hasn't sent a heartbeat in 15 seconds
                    if current_time - node_info.get('last_heartbeat', 0) > 15:
                        dead_nodes.append(node_id)
                
                # Remove dead nodes
                for node_id in dead_nodes:
                    print(f"Node {node_id} appears to be dead, removing from registry")
                    self.account_nodes.pop(node_id, None)
                
                if dead_nodes:
                    self.save_data()
            
            time.sleep(5)  # Check every 5 seconds

if __name__ == "__main__":
    import sys
    
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5000
    
    coordinator = TransactionCoordinator(port)
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Transaction Coordinator shutting down...")
