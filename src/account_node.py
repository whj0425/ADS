import socket
import json
import threading
import time
import os
import uuid

class AccountNode:
    def __init__(self, node_id, port, coordinator_port=5010, role='primary'):
        self.node_id = node_id
        self.port = port
        self.coordinator_port = coordinator_port
        self.balance = 0
        self.transaction_history = []
        self.lock = threading.Lock()
        self.data_file = f"{self.node_id}_data.json"
        
        # Replication related attributes
        self.role = role  # 'primary' or 'backup'
        self.backup_node = None  # Information about backup node if this is primary
        self.primary_node = None  # Information about primary node if this is backup
        self.last_sync_time = None  # Last time data was synchronized
        self.sync_interval = 5  # Seconds between synchronizations
        
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
        
        # Start synchronization if backup is assigned
        self.sync_thread = threading.Thread(target=self.sync_with_partner)
        self.sync_thread.daemon = True
        self.sync_thread.start()
        
        print(f"Account Node {self.node_id} started on port {self.port} with balance {self.balance} as {self.role}")
    
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
                        'balance': self.balance,
                        'role': self.role
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
                    # 只有当节点角色为primary时才执行实际的转账操作
                    if self.role == 'primary':
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
                        
                        # Sync with backup
                        if self.backup_node:
                            self.sync_to_backup()
                    # 如果是backup节点，记录交易但不修改余额（由primary同步过来）
                    elif self.role == 'backup':
                        # 仅记录交易历史
                        self.transaction_history.append({
                            'transaction_id': transaction_id,
                            'amount': amount if not is_sender else -amount,
                            'timestamp': time.time(),
                            'note': 'recorded_at_backup'
                        })
                        self.save_data()
                    
                    response = {
                        'status': 'success',
                        'message': 'Transfer executed',
                        'new_balance': self.balance,
                        'role': self.role
                    }
            
            elif command == 'heartbeat':
                response = {
                    'status': 'success',
                    'node_id': self.node_id,
                    'role': self.role
                }
            
            elif command == 'init_balance':
                amount = request.get('amount', 0)
                with self.lock:
                    self.balance = amount
                    self.save_data()
                    
                    # If this is a primary node, sync with backup
                    if self.role == 'primary' and self.backup_node:
                        self.sync_to_backup()
                    
                    response = {
                        'status': 'success',
                        'message': f'Balance initialized to {amount}',
                        'balance': self.balance
                    }
            
            elif command == 'sync_data':
                # Handle sync request from primary
                if self.role == 'backup':
                    primary_balance = request.get('balance')
                    primary_history = request.get('transaction_history')
                    
                    with self.lock:
                        self.balance = primary_balance
                        self.transaction_history = primary_history
                        self.save_data()
                        self.last_sync_time = time.time()
                    
                    response = {
                        'status': 'success',
                        'message': 'Data synchronized with primary',
                        'sync_time': self.last_sync_time
                    }
                else:
                    response = {
                        'status': 'error',
                        'message': 'Only backup nodes can receive sync data'
                    }
            
            elif command == 'become_primary':
                # Promotion request from coordinator when primary fails
                if self.role == 'backup':
                    self.role = 'primary'
                    print(f"Node {self.node_id} promoted from backup to primary!")
                    response = {
                        'status': 'success',
                        'message': f'Node {self.node_id} promoted to primary',
                        'new_role': 'primary'
                    }
                else:
                    response = {
                        'status': 'error',
                        'message': 'Only backup nodes can be promoted to primary'
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
                        'port': self.port,
                        'role': self.role,
                        'backup_node': self.backup_node,
                        'primary_node': self.primary_node
                    }
                    s.send(json.dumps(heartbeat).encode('utf-8'))
                    response = json.loads(s.recv(4096).decode('utf-8'))
                    
                    # Check if coordinator assigned a backup for this node (if primary)
                    if self.role == 'primary' and response.get('status') == 'success':
                        if response.get('backup_assigned'):
                            self.backup_node = response.get('backup_info')
                            print(f"Backup node {self.backup_node['node_id']} assigned to primary {self.node_id}")
                    
                    # Check if coordinator assigned a primary for this node (if backup)
                    if self.role == 'backup' and response.get('status') == 'success':
                        if response.get('primary_assigned'):
                            self.primary_node = response.get('primary_info')
                            print(f"Primary node {self.primary_node['node_id']} assigned to backup {self.node_id}")
                    
            except Exception as e:
                print(f"Failed to send heartbeat: {e}")
            
            time.sleep(5)  # Send heartbeat every 5 seconds

    def sync_with_partner(self):
        """Periodically synchronize data with partner node"""
        while True:
            try:
                # If primary, sync data to backup
                if self.role == 'primary' and self.backup_node:
                    self.sync_to_backup()
                
                # If backup, check if primary is alive
                elif self.role == 'backup' and self.primary_node:
                    self.check_primary_health()
            
            except Exception as e:
                print(f"Error during synchronization: {e}")
            
            time.sleep(self.sync_interval)
    
    def sync_to_backup(self):
        """Send current state to backup node"""
        if not self.backup_node:
            return
        
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(('localhost', self.backup_node['port']))
                sync_data = {
                    'command': 'sync_data',
                    'balance': self.balance,
                    'transaction_history': self.transaction_history
                }
                s.send(json.dumps(sync_data).encode('utf-8'))
                response = json.loads(s.recv(4096).decode('utf-8'))
                
                if response.get('status') == 'success':
                    print(f"Synchronized data with backup node {self.backup_node['node_id']}")
                    self.last_sync_time = time.time()
                else:
                    print(f"Failed to sync with backup: {response.get('message')}")
        
        except Exception as e:
            print(f"Error syncing to backup: {e}")
    
    def check_primary_health(self):
        """Check if primary node is still alive"""
        if not self.primary_node:
            return
        
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(2)  # Short timeout for health check
                s.connect(('localhost', self.primary_node['port']))
                health_check = {
                    'command': 'heartbeat'
                }
                s.send(json.dumps(health_check).encode('utf-8'))
                response = json.loads(s.recv(4096).decode('utf-8'))
                
                if response.get('status') == 'success':
                    # Primary is still alive
                    pass
                else:
                    print(f"Unhealthy response from primary: {response.get('message')}")
                    self.notify_coordinator_of_primary_failure()
        
        except Exception as e:
            print(f"Primary node may be down: {e}")
            self.notify_coordinator_of_primary_failure()
    
    def notify_coordinator_of_primary_failure(self):
        """Notify coordinator that primary appears to be down"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(('localhost', self.coordinator_port))
                failure_report = {
                    'command': 'report_node_failure',
                    'reporter': self.node_id,
                    'failed_node': self.primary_node['node_id'],
                    'reporter_role': 'backup'
                }
                s.send(json.dumps(failure_report).encode('utf-8'))
                # No need to wait for response
                print(f"Reported primary {self.primary_node['node_id']} failure to coordinator")
        
        except Exception as e:
            print(f"Failed to notify coordinator about primary failure: {e}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 3:
        print("Usage: python account_node.py <node_id> <port> [coordinator_port] [role]")
        sys.exit(1)
    
    node_id = sys.argv[1]
    port = int(sys.argv[2])
    coordinator_port = int(sys.argv[3]) if len(sys.argv) > 3 else 5000
    role = sys.argv[4] if len(sys.argv) > 4 else 'primary'
    
    node = AccountNode(node_id, port, coordinator_port, role)
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"Account Node {node_id} shutting down...")
