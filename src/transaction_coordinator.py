import socket
import json
import threading
import time
import uuid
import os
import sys
from pathlib import Path

# Add project root to system path
sys.path.append(str(Path(__file__).parent.parent))
from config.network_config import COORDINATOR_PORT

class TransactionCoordinator:
    def __init__(self, port=COORDINATOR_PORT, coordinator_id="c1"):
        """
        Initialize the transaction coordinator that manages distributed transactions.
        
        Args:
            port: Port number for the coordinator to listen on
            coordinator_id: Unique identifier for this coordinator
        """
        self.coordinator_id = coordinator_id
        self.port = port
        self.account_nodes = {}  # {node_id: {'port': port, 'last_heartbeat': timestamp, 'role': role, 'backup': backup_node_id}}
        self.node_hosts = {}  # {node_id: host_ip} - Track the host address for each node
        self.transactions = {}  # {transaction_id: {'status': status, 'from': node_id, 'to': node_id, 'amount': amount}}
        self.lock = threading.Lock()
        self.data_file = "data/coordinator_data.json"
        self.node_pairs = {}  # {primary_id: backup_id} - tracks primary-backup relationships
        
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
        """
        Load coordinator data from persistent storage.
        Restores account nodes, transactions, and node pairing information.
        """
        if os.path.exists(self.data_file):
            try:
                with open(self.data_file, 'r') as f:
                    data = json.load(f)
                    self.account_nodes = data.get('account_nodes', {})
                    self.transactions = data.get('transactions', {})
                    self.node_pairs = data.get('node_pairs', {})
                    self.node_hosts = data.get('node_hosts', {})
                    
                    # Debug information
                    print(f"Data loading completed. Node status:")
                    for node_id, node_info in self.account_nodes.items():
                        status = node_info.get('status', 'active')
                        role = node_info.get('role', 'primary')
                        print(f"  - Node {node_id}: status={status}, role={role}")
            except Exception as e:
                print(f"Error loading coordinator data: {e}")
    
    def save_data(self):
        """
        Save coordinator data to persistent storage.
        Writes account nodes, transactions, and node pairing information to a JSON file.
        """
        # Debug information
        print(f"Saving node status information:")
        for node_id, node_info in self.account_nodes.items():
            status = node_info.get('status', 'active')
            role = node_info.get('role', 'primary')
            print(f"  - Node {node_id}: status={status}, role={role}")
            
        data = {
            'account_nodes': self.account_nodes,
            'transactions': self.transactions,
            'node_pairs': self.node_pairs,
            'node_hosts': self.node_hosts
        }
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(self.data_file), exist_ok=True)
        
        try:
            with open(self.data_file, 'w') as f:
                json.dump(data, f, indent=2)
            print(f"Data has been saved to {self.data_file}")
        except Exception as e:
            print(f"Error saving data: {e}")
    
    def start_server(self):
        """
        Start the TCP server to listen for incoming requests.
        Creates a new thread for each client connection.
        """
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(('0.0.0.0', self.port))
        server.listen(5)
        
        while True:
            client, addr = server.accept()
            client_thread = threading.Thread(target=self.handle_request, args=(client,))
            client_thread.daemon = True
            client_thread.start()
    
    def handle_request(self, client):
        """
        Handle incoming client requests.
        Parses the JSON request and executes the appropriate command.
        
        Args:
            client: Socket connection to the client
        """
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
                role_from_heartbeat = request.get('role', 'primary')  # Get role reported by node
                backup_node = request.get('backup_node')
                primary_node = request.get('primary_node')
                client_addr = request.get('client_addr')  # Client address if provided through the request
                
                if node_type == 'account':
                    with self.lock:
                        # Record client address if provided; otherwise use connection address
                        if not client_addr:
                            client_addr, _ = client.getpeername()
                        
                        # Store node host mapping
                        self.node_hosts[node_id] = client_addr
                        
                        # Check if node already exists
                        if node_id in self.account_nodes:
                            # Node exists, update selectively
                            existing_node_info = self.account_nodes[node_id]

                            # If node is marked as failed, only update heartbeat time, do not change status/role
                            if existing_node_info.get('status') == 'failed':
                                print(f"Received heartbeat from failed node {node_id}. Ignoring role/status update.")
                                existing_node_info['last_heartbeat'] = time.time()
                                # Optionally update port if it can change dynamically
                                # existing_node_info['port'] = port
                                response = {
                                    'status': 'success',
                                    'message': 'Heartbeat received from failed node, status unchanged'
                                }
                            else:
                                # Node is active, update normally but preserve existing status if any
                                existing_node_info['port'] = port
                                existing_node_info['last_heartbeat'] = time.time()
                                # Only update role if it's not explicitly set to something else by coordinator logic
                                # For now, let's keep the role reported by the heartbeat unless coordinator logic changed it
                                existing_node_info['role'] = role_from_heartbeat
                                print(f"Updated existing node {node_id} info from heartbeat.")
                                response = {
                                    'status': 'success',
                                    'message': 'Heartbeat received and node info updated'
                                }
                                # Re-evaluate pairing based on updated info if necessary (e.g., role changed)
                                # This part might need refinement depending on role change handling
                        else:
                            # New node, create entry
                            self.account_nodes[node_id] = {
                                'port': port,
                                'last_heartbeat': time.time(),
                                'role': role_from_heartbeat # Use role from heartbeat for new nodes
                            }
                            print(f"Registered new node {node_id} from heartbeat.")
                            response = {
                                'status': 'success',
                                'message': 'Heartbeat received, new node registered'
                            }

                        # Handle primary-backup pairing regardless of new/existing if role is relevant
                        current_role = self.account_nodes[node_id]['role']

                        # If this is a primary node trying to pair
                        if current_role == 'primary' and not backup_node and node_id not in self.node_pairs:
                            backup_id = f"{node_id}b"
                            if backup_id in self.account_nodes and self.account_nodes[backup_id]['role'] == 'backup':
                                self.node_pairs[node_id] = backup_id
                                response['backup_assigned'] = True
                                response['backup_info'] = {'node_id': backup_id, 'port': self.account_nodes[backup_id]['port']}
                                print(f"Paired primary {node_id} with backup {backup_id} via heartbeat.")

                        # If this is a backup node trying to pair
                        elif current_role == 'backup' and not primary_node:
                             if node_id.endswith('b') and len(node_id) > 1:
                                primary_id = node_id[:-1]
                                # Check if primary exists and is not already paired
                                if primary_id in self.account_nodes and self.account_nodes[primary_id]['role'] == 'primary' and primary_id not in self.node_pairs:
                                    self.node_pairs[primary_id] = node_id
                                    response['primary_assigned'] = True
                                    response['primary_info'] = {'node_id': primary_id, 'port': self.account_nodes[primary_id]['port']}
                                    print(f"Paired backup {node_id} with primary {primary_id} via heartbeat.")

                        self.save_data()
            
            elif command == 'list_accounts':
                with self.lock:
                    response = {
                        'status': 'success',
                        'accounts': list(self.account_nodes.keys())
                    }
            
            elif command == 'simulate_failure':
                # Command to simulate node failure
                node_id = request.get('node_id')
                
                with self.lock:
                    if node_id in self.account_nodes:
                        # 1. First unconditionally mark the node as failed
                        print(f"Node {node_id} status before simulation: {self.account_nodes[node_id].get('status', 'active')}")
                        self.account_nodes[node_id]['status'] = 'failed'
                        self.account_nodes[node_id]['failure_time'] = time.time()
                        print(f"Node {node_id} has been marked as failed: {self.account_nodes[node_id]}")
                        
                        # 2. Immediately save state changes to disk
                        self.save_data()
                        
                        # 3. Confirm the node status has been changed
                        assert self.account_nodes[node_id]['status'] == 'failed', "Node status was not changed successfully!"
                        print(f"Confirmed node {node_id} status has been updated to: {self.account_nodes[node_id].get('status')}")
                        
                        # 4. Build basic response
                        backup_node_id = self.node_pairs.get(node_id)
                        response = {
                            'status': 'success',
                            'message': f'Node {node_id} has been marked as failed',
                            'backup_node': backup_node_id,
                            'node_status': self.account_nodes[node_id].get('status')
                        }
                        
                        # 5. Try to promote backup node, but this doesn't affect the failed status of the node
                        backup_promoted = False
                        if backup_node_id and backup_node_id in self.account_nodes:
                            print(f"Attempting to promote backup node {backup_node_id} to take over for {node_id}")
                            
                            # First update the backup node role to primary in the coordinator
                            try:
                                previous_role = self.account_nodes[backup_node_id].get('role', 'backup')
                                self.account_nodes[backup_node_id]['role'] = 'primary'
                                print(f"Backup node {backup_node_id} role updated from {previous_role} to {self.account_nodes[backup_node_id]['role']}")
                                
                                # Update node pairing relationships
                                self.node_pairs.pop(node_id, None)
                                self.save_data()
                                backup_promoted = True
                                print(f"Backup node {backup_node_id} has been promoted to primary in the coordinator")
                                
                                # Try to notify the backup node, but consider it successful even if this fails
                                try:
                                    backup_port = self.account_nodes[backup_node_id]['port']
                                    host = self.node_hosts.get(backup_node_id, 'localhost')
                                    
                                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                                        s.settimeout(2)  # Short timeout to avoid long waits
                                        s.connect((host, backup_port))
                                        promote_request = {
                                            'command': 'become_primary'
                                        }
                                        s.send(json.dumps(promote_request).encode('utf-8'))
                                        # Ignore response, we've already updated the status in the coordinator
                                except Exception as e:
                                    print(f"Error while notifying the backup node, but this doesn't affect the status update: {e}")
                            except Exception as e:
                                print(f"Error during backup node promotion: {e}")
                        
                        # 6. Final status check and confirmation
                        print(f"Final confirmation of node {node_id} status: {self.account_nodes[node_id].get('status', 'unknown')}")
                        if backup_node_id:
                            print(f"Final confirmation of backup node {backup_node_id} role: {self.account_nodes[backup_node_id].get('role', 'unknown')}")
                        
                        # 7. Update response to include backup promotion status
                        response['backup_promoted'] = backup_promoted
                        response['final_node_status'] = self.account_nodes[node_id].get('status')
                    else:
                        response = {
                            'status': 'error',
                            'message': f'Node {node_id} does not exist'
                        }
            
            elif command == 'recover_node':
                # Command to recover a node
                node_id = request.get('node_id') # The node being recovered (e.g., a1)
                
                with self.lock:
                    if node_id in self.account_nodes:
                        print(f"Node {node_id} status before recovery: {self.account_nodes[node_id].get('status', 'active')}")
                        
                        if self.account_nodes[node_id].get('status') == 'failed':
                            # Node is indeed marked as failed, proceed with recovery
                            
                            # Step 1: Find the node that took over (the original backup, now primary)
                            potential_takeover_node_id = f"{node_id}b"
                            takeover_node_info = self.account_nodes.get(potential_takeover_node_id)
                            
                            latest_balance = None
                            sync_success = False

                            if takeover_node_info and takeover_node_info.get('role') == 'primary':
                                print(f"Found takeover node {potential_takeover_node_id}, attempting to sync state...")
                                try:
                                    # Step 2: Get the current balance from the takeover node
                                    host = self.node_hosts.get(potential_takeover_node_id, 'localhost')
                                    port = takeover_node_info['port']
                                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                                        s.settimeout(3)
                                        s.connect((host, port))
                                        s.send(json.dumps({'command': 'get_balance'}).encode('utf-8'))
                                        balance_response_data = s.recv(4096)
                                        balance_response = json.loads(balance_response_data.decode('utf-8'))
                                        
                                        if balance_response.get('status') == 'success':
                                            latest_balance = balance_response.get('balance')
                                            print(f"Retrieved latest balance from {potential_takeover_node_id}: {latest_balance}")
                                        else:
                                            print(f"Unable to retrieve balance from {potential_takeover_node_id}: {balance_response.get('message')}")
                                
                                except Exception as e:
                                    print(f"Error connecting to takeover node {potential_takeover_node_id} to get balance: {e}")

                                # Step 3: If balance was obtained, force set it on the recovering node
                                if latest_balance is not None:
                                    try:
                                        recovering_node_info = self.account_nodes[node_id]
                                        host = self.node_hosts.get(node_id, 'localhost')
                                        port = recovering_node_info['port']
                                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                                            s.settimeout(3)
                                            s.connect((host, port))
                                            force_set_req = {
                                                'command': 'force_set_balance', # Requires account_node to handle this
                                                'balance': latest_balance
                                            }
                                            s.send(json.dumps(force_set_req).encode('utf-8'))
                                            set_response_data = s.recv(4096)
                                            set_response = json.loads(set_response_data.decode('utf-8'))
                                            
                                            if set_response.get('status') == 'success':
                                                sync_success = True
                                                print(f"Successfully synchronized latest balance to recovering node {node_id}")
                                            else:
                                                 print(f"Node {node_id} balance synchronization failed: {set_response.get('message')}")
                                    except Exception as e:
                                        print(f"Error connecting to recovering node {node_id} to set balance: {e}")
                            else:
                                print(f"Warning: No valid takeover node {potential_takeover_node_id} found to sync state. Node {node_id} will recover using its local state.")
                                # Decide if recovery should proceed without sync or fail
                                # For simulation, we might allow it, but log a warning.
                                sync_success = True # Allow recovery without sync for now

                            # Step 4: If sync was successful (or skipped), mark the node as active and restore primary/backup roles
                            if sync_success:
                                # Mark the recovering node as active and ensure it's primary
                                self.account_nodes[node_id]['role'] = 'primary' # Explicitly set recovered node to primary
                                self.account_nodes[node_id].pop('status', None)
                                self.account_nodes[node_id].pop('failure_time', None)
                                print(f"Node {node_id} has been marked as active and role set to 'primary'")

                                # Step 5: Reset the takeover node (original backup) back to 'backup' role
                                if takeover_node_info and takeover_node_info.get('role') == 'primary':
                                    print(f"Attempting to reset takeover node {potential_takeover_node_id} role to 'backup'")
                                    try:
                                        # Notify the takeover node to become backup
                                        takeover_host = self.node_hosts.get(potential_takeover_node_id, 'localhost')
                                        takeover_port = takeover_node_info['port']
                                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                                            s.settimeout(2)
                                            s.connect((takeover_host, takeover_port))
                                            become_backup_req = {'command': 'become_backup'} # Node needs to handle this
                                            s.send(json.dumps(become_backup_req).encode('utf-8'))
                                            # We don't necessarily need to wait for a response, but log if node acknowledged
                                            try:
                                                backup_res_data = s.recv(1024)
                                                backup_res = json.loads(backup_res_data.decode('utf-8'))
                                                if backup_res.get('status') == 'success':
                                                    print(f"Node {potential_takeover_node_id} confirmed switch back to backup role")
                                                else:
                                                    print(f"Warning: Node {potential_takeover_node_id} returned error when switching to backup: {backup_res.get('message')}")
                                            except socket.timeout:
                                                print(f"Warning: Timeout waiting for node {potential_takeover_node_id} to confirm switch to backup")
                                            except Exception as e_recv:
                                                print(f"Warning: Error reading response from node {potential_takeover_node_id} switch to backup: {e_recv}")

                                        # Update coordinator state regardless of notification success
                                        self.account_nodes[potential_takeover_node_id]['role'] = 'backup'
                                        # Re-establish the pairing in node_pairs
                                        self.node_pairs[node_id] = potential_takeover_node_id
                                        print(f"Coordinator has updated node {potential_takeover_node_id} role to 'backup' and restored pairing relationship {node_id} -> {potential_takeover_node_id}")

                                    except Exception as e_notify:
                                        print(f"Error: Failed to notify node {potential_takeover_node_id} to switch to backup: {e_notify}. Coordinator state may be inconsistent with node state!")
                                        # Decide on error handling: proceed with coordinator state update?
                                        # For now, let's update coordinator state but log the inconsistency risk
                                        self.account_nodes[potential_takeover_node_id]['role'] = 'backup'
                                        self.node_pairs[node_id] = potential_takeover_node_id
                                        print(f"Warning: Despite notification failure, coordinator has set {potential_takeover_node_id} role to 'backup' and restored pairing")
                                else:
                                    print(f"No takeover node {potential_takeover_node_id} found or its role is not primary, no need to reset role")


                                # Step 6: Save final state and prepare response
                                print(f"Final confirmation of node {node_id} state: {self.account_nodes[node_id]}")
                                if potential_takeover_node_id in self.account_nodes:
                                     print(f"Final confirmation of node {potential_takeover_node_id} state: {self.account_nodes[potential_takeover_node_id]}")
                                print(f"Final confirmation of pairing relationship: {self.node_pairs.get(node_id)}")

                                self.save_data()
                                print(f"Confirmed node {node_id} current status: {self.account_nodes[node_id].get('status', 'active')}, role: {self.account_nodes[node_id].get('role')}")
                                if potential_takeover_node_id in self.account_nodes:
                                    print(f"Confirmed node {potential_takeover_node_id} current status: {self.account_nodes[potential_takeover_node_id].get('status', 'active')}, role: {self.account_nodes[potential_takeover_node_id].get('role')}")

                                response = {
                                    'status': 'success',
                                    'message': f'Node {node_id} has been restored to normal state and set as primary. Node {potential_takeover_node_id} has been reset to backup.' + (' (Latest balance synchronized)' if latest_balance is not None else ' (State synchronization not performed)'),
                                    'node_info': self.account_nodes[node_id],
                                    'backup_node_info': self.account_nodes.get(potential_takeover_node_id)
                                }
                            else:
                                print(f"Node {node_id} state synchronization failed, recovery aborted.")
                                response = {
                                    'status': 'error',
                                    'message': f'Node {node_id} state synchronization failed, cannot recover.'
                                }
                        else:
                            print(f"Node {node_id} is not currently in failed state: {self.account_nodes[node_id]}")
                            response = {
                                'status': 'error',
                                'message': f'Node {node_id} is not currently in failed state',
                                'node_info': self.account_nodes[node_id]
                            }
                    else:
                        response = {
                            'status': 'error',
                            'message': f'Node {node_id} does not exist'
                        }
            
            elif command == 'check_node_status':
                # Command to check node status
                node_id = request.get('node_id')
                
                with self.lock:
                    if node_id in self.account_nodes:
                        node_info = self.account_nodes[node_id]
                        # Directly check if status field is 'failed'
                        is_failed = node_info.get('status') == 'failed'
                        is_active = not is_failed
                        backup_node_id = self.node_pairs.get(node_id)
                        
                        # Debug information
                        print(f"DEBUG - Node info: {node_info}")
                        print(f"DEBUG - Node {node_id} status: {'failed' if is_failed else 'active'}")
                        
                        # Get all status information directly from memory
                        response = {
                            'status': 'success',
                            'node_id': node_id,
                            'is_active': is_active,
                            'role': node_info.get('role', 'primary'),
                            'backup_node': backup_node_id,
                            'state': 'failed' if is_failed else 'active',  # Ensure state is consistent with is_active
                            'node_info': node_info,  # Return complete node info for debugging
                            'last_heartbeat': node_info.get('last_heartbeat'),
                            'port': node_info.get('port')
                        }
                    else:
                        response = {
                            'status': 'error',
                            'message': f'Node {node_id} does not exist'
                        }
            
            elif command == 'transfer':
                # Handle transfer request
                from_account = request.get('from')
                to_account = request.get('to')
                amount = request.get('amount')
                
                # Record original account IDs
                original_from = from_account
                original_to = to_account
                
                if from_account not in self.account_nodes or to_account not in self.account_nodes:
                    response = {
                        'status': 'error',
                        'message': 'One or both accounts do not exist'
                    }
                else:
                    # Check node status, if failed immediately redirect to backup node
                    from_node_failed = self.account_nodes.get(from_account, {}).get('status') == 'failed'
                    to_node_failed = self.account_nodes.get(to_account, {}).get('status') == 'failed'
                    
                    # If source account node failed, redirect to backup
                    redirected = False
                    if from_node_failed:
                        backup_from = self.node_pairs.get(from_account)
                        if backup_from and backup_from in self.account_nodes:
                            print(f"Source account {from_account} has failed, redirecting to backup node {backup_from}")
                            from_account = backup_from
                            redirected = True
                        else:
                            # NEW LOGIC: If not in node_pairs, try deducing backup ID and check if it's the new primary
                            potential_backup_id = f"{from_account}b"
                            if potential_backup_id in self.account_nodes and self.account_nodes[potential_backup_id].get('role') == 'primary':
                                print(f"Source account {from_account} has failed, redirecting to backup node {potential_backup_id} that has been promoted to primary")
                                from_account = potential_backup_id
                                redirected = True
                            else:
                                response = {
                                    'status': 'error',
                                    'message': f'Source account {original_from} is currently unavailable and has no available takeover node' # Use original ID in error message
                                }
                                client.send(json.dumps(response).encode('utf-8'))
                                return

                    # If target account node failed, redirect to backup
                    if to_node_failed:
                        backup_to = self.node_pairs.get(to_account)
                        if backup_to and backup_to in self.account_nodes:
                            print(f"Target account {to_account} has failed, redirecting to backup node {backup_to}")
                            to_account = backup_to
                            redirected = True
                        else:
                            # NEW LOGIC: If not in node_pairs, try deducing backup ID and check if it's the new primary
                            potential_backup_id = f"{to_account}b"
                            if potential_backup_id in self.account_nodes and self.account_nodes[potential_backup_id].get('role') == 'primary':
                                print(f"Target account {to_account} has failed, redirecting to backup node {potential_backup_id} that has been promoted to primary")
                                to_account = potential_backup_id
                                redirected = True
                            else:
                                response = {
                                    'status': 'error',
                                    'message': f'Target account {original_to} is currently unavailable and has no available takeover node' # Use original ID in error message
                                }
                                client.send(json.dumps(response).encode('utf-8'))
                                return
                    
                    # Start two-phase commit protocol
                    transaction_id = str(uuid.uuid4())
                    success = self.execute_two_phase_commit(transaction_id, from_account, to_account, amount)
                    
                    if success:
                        response = {
                            'status': 'success',
                            'message': f'From {original_from} to {original_to} {amount} transfer completed',
                            'transaction_id': transaction_id,
                            'used_backup': redirected
                        }
                    else:
                        response = {
                            'status': 'error',
                            'message': 'Transfer failed in two-phase commit process'
                        }
            
            elif command == 'get_balance':
                # Handle balance query request
                account_id = request.get('account_id')
                
                if account_id not in self.account_nodes:
                    response = {
                        'status': 'error',
                        'message': f'Account {account_id} not found'
                    }
                else:
                    # Record original account ID for response display
                    original_account = account_id
                    
                    # Check node status, if failed immediately redirect to backup node
                    node_failed = self.account_nodes.get(account_id, {}).get('status') == 'failed'
                    
                    if node_failed:
                        backup_id = self.node_pairs.get(account_id)
                        if backup_id and backup_id in self.account_nodes:
                            print(f"Account {account_id} has failed, redirecting to backup node {backup_id}")
                            account_id = backup_id
                        else:
                            # NEW LOGIC: If not in node_pairs, try deducing backup ID and check if it's the new primary
                            potential_backup_id = f"{account_id}b"
                            if potential_backup_id in self.account_nodes and self.account_nodes[potential_backup_id].get('role') == 'primary':
                                print(f"Account {original_account} has failed, redirecting to backup node {potential_backup_id} that has been promoted to primary")
                                account_id = potential_backup_id # Update account_id to the promoted node
                            else:
                                response = {
                                    'status': 'error',
                                    'message': f'Account {original_account} is currently unavailable and has no available takeover node' # Use original ID in error message
                                }
                                client.send(json.dumps(response).encode('utf-8'))
                                return
                    
                    # Forward request to account node
                    try:
                        node_info = self.account_nodes[account_id]
                        host = self.node_hosts.get(account_id, 'localhost')
                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                            s.settimeout(3)  # Increase timeout to 3 seconds, avoid long waits
                            s.connect((host, node_info['port']))
                            balance_request = {
                                'command': 'get_balance'
                            }
                            s.send(json.dumps(balance_request).encode('utf-8'))
                            balance_response = json.loads(s.recv(4096).decode('utf-8'))
                            
                            if balance_response.get('status') == 'success':
                                response = {
                                    'status': 'success',
                                    'balance': balance_response.get('balance'),
                                    'account_id': account_id,
                                    'used_backup': (account_id != original_account)
                                }
                            else:
                                response = {
                                    'status': 'error',
                                    'message': f'Unable to retrieve balance from account {account_id}'
                                }
                    except Exception as e:
                        response = {
                            'status': 'error',
                            'message': f'Error accessing account {account_id}: {str(e)}'
                        }
            
            elif command == 'report_node_failure':
                # Handle node failure report from a backup node
                reporter_id = request.get('reporter')
                failed_node_id = request.get('failed_node')
                reporter_role = request.get('reporter_role')
                
                if reporter_role == 'backup' and failed_node_id:
                    # Verify that reporter is actually backup of the failed node
                    is_valid_reporter = False
                    
                    for primary_id, backup_id in self.node_pairs.items():
                        if primary_id == failed_node_id and backup_id == reporter_id:
                            is_valid_reporter = True
                            break
                    
                    if is_valid_reporter and failed_node_id in self.account_nodes:
                        # Check if the node is already marked as failed (e.g., by monitor)
                        if self.account_nodes[failed_node_id].get('status') == 'failed':
                            print(f"Received failure report for node {failed_node_id}, which is already marked as failed.")
                            # Node already marked as failed, just ensure backup is primary if needed
                            if reporter_id in self.account_nodes and self.account_nodes[reporter_id].get('role') != 'primary':
                                print(f"Ensuring reporter {reporter_id} is primary.")
                                self.promote_backup_to_primary(reporter_id, failed_node_id)
                            response = {
                                'status': 'success',
                                'message': 'Failure report acknowledged for already failed node.'
                            }
                        else:
                            # NEW LOGIC: Additional verification before marking as failed
                            # 1. Check when the last heartbeat was received from the reported node
                            current_time = time.time()
                            last_heartbeat = self.account_nodes[failed_node_id].get('last_heartbeat', 0)
                            time_since_last_heartbeat = current_time - last_heartbeat
                            
                            # 2. Only act immediately if heartbeat is significantly old (15+ seconds)
                            if time_since_last_heartbeat > 15:
                                print(f"Verified failure report for node {failed_node_id}. Last heartbeat was {time_since_last_heartbeat:.1f} seconds ago. Marking as failed and promoting backup.")
                                
                                # Mark the node as failed
                                with self.lock:
                                    self.account_nodes[failed_node_id]['status'] = 'failed'
                                    self.account_nodes[failed_node_id]['failure_time'] = time.time()
                                    self.save_data() # Save the failed status

                                # Promote the backup to primary
                                promote_success = self.promote_backup_to_primary(reporter_id, failed_node_id)
                                
                                if promote_success:
                                    response = {
                                        'status': 'success',
                                        'message': 'Failure reported, node marked as failed, and backup promoted.'
                                    }
                                else:
                                    response = {
                                        'status': 'error',
                                        'message': 'Failure reported and node marked as failed, but backup promotion failed.'
                                    }
                            else:
                                # NEW LOGIC: Heartbeat is recent, log the report but don't act yet
                                print(f"Received failure report for node {failed_node_id}, but last heartbeat was only {time_since_last_heartbeat:.1f} seconds ago. Logging report but delaying action.")
                                # Note: We're acknowledging the report but not taking action yet
                                # This allows automatic retry from the backup node
                                response = {
                                    'status': 'success',
                                    'message': 'Failure report received, but action delayed due to recent heartbeat.',
                                    'retry': True,
                                    'heartbeat_age': time_since_last_heartbeat
                                }
                    elif not is_valid_reporter:
                        response = {
                            'status': 'error',
                            'message': f'Invalid failure report: Reporter {reporter_id} is not the backup of {failed_node_id}.'
                        }
                    elif failed_node_id not in self.account_nodes:
                         response = {
                            'status': 'error',
                            'message': f'Invalid failure report: Failed node {failed_node_id} not found.'
                        }
                else:
                    response = {
                        'status': 'error',
                        'message': 'Invalid failure report format'
                    }
            
            elif command == 'init_accounts':
                # Initialize accounts with initial balance
                amount = request.get('amount', 10000)
                success = True
                
                # Only initialize primary nodes (backups will be synced automatically)
                primary_nodes = {n: info for n, info in self.account_nodes.items() 
                                if info.get('role', 'primary') == 'primary'}
                
                for node_id, node_info in primary_nodes.items():
                    try:
                        host = self.node_hosts.get(node_id, 'localhost')
                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                            s.connect((host, node_info['port']))
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
            # First ensure we only operate on primary nodes
            node_info = self.account_nodes.get(account_id)
            if not node_info:
                return False
            
            # If this is a backup node, find its corresponding primary node
            node_role = node_info.get('role', 'primary')
            if node_role == 'backup':
                # For backup node(e.g., a1b), find its corresponding primary node(a1)
                if account_id.endswith('b') and len(account_id) > 1:
                    primary_id = account_id[:-1]  # Remove 'b' suffix
                    if primary_id in self.account_nodes:
                        node_info = self.account_nodes[primary_id]
                        account_id = primary_id
                        print(f"Redirecting prepare request from backup {account_id}b to primary {account_id}")
                    else:
                        print(f"Error: Primary node for backup {account_id} not found")
                        return False
                else:
                    print(f"Error: Backup node {account_id} has invalid format")
                    return False
            
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                host = self.node_hosts.get(account_id, 'localhost')
                s.connect((host, node_info['port']))
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
            # First ensure we only operate on primary nodes
            node_info = self.account_nodes.get(account_id)
            if not node_info:
                return False
            
            # If this is a backup node, find its corresponding primary node
            node_role = node_info.get('role', 'primary')
            if node_role == 'backup':
                # For backup node(e.g., a1b), find its corresponding primary node(a1)
                if account_id.endswith('b') and len(account_id) > 1:
                    primary_id = account_id[:-1]  # Remove 'b' suffix
                    if primary_id in self.account_nodes:
                        node_info = self.account_nodes[primary_id]
                        account_id = primary_id
                        print(f"Redirecting execute request from backup {account_id}b to primary {account_id}")
                    else:
                        print(f"Error: Primary node for backup {account_id} not found")
                        return False
                else:
                    print(f"Error: Backup node {account_id} has invalid format")
                    return False
            
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                host = self.node_hosts.get(account_id, 'localhost')
                s.connect((host, node_info['port']))
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
        # Check node health periodically and handle failover
        while True:
            time.sleep(60)  # Check every 60 seconds
            nodes_marked_failed_this_cycle = [] # Re-initialize the list here

            with self.lock:
                current_time = time.time()
                
                for node_id, node_info in list(self.account_nodes.items()):
                    # Skip nodes already marked as failed
                    if node_info.get('status') == 'failed':
                        continue

                    # Check if node hasn't sent a heartbeat in 15 seconds
                    heartbeat_timeout = 60 # <---- THIS IS THE VALUE TO CHANGE <--- Changed value from 15

                    if node_info.get('last_heartbeat', 0) is None or (current_time - node_info.get('last_heartbeat', 0) > heartbeat_timeout):
                        print(f"Node {node_id} has missed heartbeats. Last heartbeat at: {node_info.get('last_heartbeat', 'never')} Current time: {current_time}")
                        
                        # Mark node as failed instead of removing immediately
                        self.account_nodes[node_id]['status'] = 'failed'
                        self.account_nodes[node_id]['failure_time'] = current_time
                        nodes_marked_failed_this_cycle.append(node_id)
                        
                        # If primary node failed, promote its backup (but don't remove primary)
                        if node_info.get('role') == 'primary':
                            backup_id = self.node_pairs.get(node_id)
                            if backup_id and backup_id in self.account_nodes:
                                print(f"Promoting backup {backup_id} for failed primary {node_id}")
                                # Promote backup but DO NOT remove the primary node record or the pair yet.
                                # The primary is kept as 'failed'. The pair removal can happen 
                                # during recovery or if backup promotion fails and needs cleanup.
                                promote_success = self.promote_backup_to_primary(backup_id, node_id)
                                if not promote_success:
                                     print(f"Warning: Failed to promote backup {backup_id}. State might be inconsistent.")
                                # Keep the node_pairs entry for now, maybe useful for recovery?
                                # Let's stick to removing it in promote_backup_to_primary for consistency.
                        # If a backup node fails, just mark it as failed. 
                        # The primary might need to find a new backup later.
                        elif node_info.get('role') == 'backup':
                             print(f"Backup node {node_id} marked as failed.")
                             # Find its primary and maybe remove the pairing if necessary, 
                             # but let's keep it simple for now and just mark failed.
                             # for primary_id, current_backup_id in list(self.node_pairs.items()):
                             #    if current_backup_id == node_id:
                             #        self.node_pairs.pop(primary_id)
                             #        print(f"Removed pairing for failed backup {node_id} of primary {primary_id}")
                             #        break

                # Save data if any nodes were marked as failed
                if nodes_marked_failed_this_cycle:
                    self.save_data()
            
    def promote_backup_to_primary(self, backup_id, failed_primary_id):
        """Promote backup node to primary"""
        # First check if backup node exists
        if backup_id not in self.account_nodes:
            print(f"Cannot promote node {backup_id}: Node does not exist")
            return False
        
        # 1. Update coordinator internal state (this step should never fail)
        try:
            with self.lock:
                # Change backup node role to primary
                self.account_nodes[backup_id]['role'] = 'primary'
                
                # Remove primary-backup relationship
                self.node_pairs.pop(failed_primary_id, None)
                
                # Save updated state
                self.save_data()
                
            print(f"Coordinator has updated node {backup_id} role to primary")
        except Exception as e:
            print(f"Error updating coordinator internal state: {e}")
            return False
        
        # 2. Try to notify backup node, but even if it fails, do not affect state update
        success = False
        try:
            backup_port = self.account_nodes[backup_id]['port']
            host = self.node_hosts.get(backup_id, 'localhost')
            
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(2)  # Short timeout, avoid long blocking
                s.connect((host, backup_port))
                promote_request = {
                    'command': 'become_primary'
                }
                s.send(json.dumps(promote_request).encode('utf-8'))
                
                # Try to get response but do not depend on it
                try:
                    response_data = s.recv(4096)
                    promote_response = json.loads(response_data.decode('utf-8'))
                    success = promote_response.get('status') == 'success'
                    
                    if success:
                        print(f"Backup node {backup_id} confirmed receiving command to become primary")
                    else:
                        print(f"Backup node {backup_id} returned error: {promote_response.get('message')}")
                except Exception as e:
                    print(f"Error reading backup node response: {e}")
        except Exception as e:
            print(f"Error sending promote notification to backup node {backup_id}: {e}")
        
        # Regardless of notification success, state is updated, so return success
        return True

if __name__ == "__main__":
    import sys
    
    port = int(sys.argv[1]) if len(sys.argv) > 1 else COORDINATOR_PORT
    
    coordinator = TransactionCoordinator(port)
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Transaction Coordinator shutting down...")
