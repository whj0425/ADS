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
        self.account_nodes = {}  # {node_id: {'port': port, 'last_heartbeat': timestamp, 'role': role, 'backup': backup_node_id}}
        self.node_hosts = {}  # {node_id: host_ip} - 跟踪每个节点的主机地址
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
        if os.path.exists(self.data_file):
            try:
                with open(self.data_file, 'r') as f:
                    data = json.load(f)
                    self.account_nodes = data.get('account_nodes', {})
                    self.transactions = data.get('transactions', {})
                    self.node_pairs = data.get('node_pairs', {})
                    self.node_hosts = data.get('node_hosts', {})
            except Exception as e:
                print(f"Error loading coordinator data: {e}")
    
    def save_data(self):
        data = {
            'account_nodes': self.account_nodes,
            'transactions': self.transactions,
            'node_pairs': self.node_pairs,
            'node_hosts': self.node_hosts
        }
        with open(self.data_file, 'w') as f:
            json.dump(data, f, indent=2)
    
    def start_server(self):
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
                role = request.get('role', 'primary')  # Default to primary if not specified
                backup_node = request.get('backup_node')
                primary_node = request.get('primary_node')
                client_addr = request.get('client_addr')  # 客户端地址，如果通过请求提供
                
                if node_type == 'account':
                    with self.lock:
                        # 记录客户端地址，如果通过请求提供；否则使用连接的地址
                        if not client_addr:
                            client_addr, _ = client.getpeername()
                        
                        # Store node host mapping
                        self.node_hosts[node_id] = client_addr
                        
                        # Store node information
                        self.account_nodes[node_id] = {
                            'port': port,
                            'last_heartbeat': time.time(),
                            'role': role
                        }
                        
                        # Assign backup/primary relationships if needed
                        response = {
                            'status': 'success',
                            'message': 'Heartbeat received'
                        }
                        
                        # Handle primary-backup pairing - use node ID patterns to auto-assign pairs
                        # Primary nodes are named a1, a2, etc.
                        # Backup nodes are named a1b, a2b, etc.
                        
                        # If this is a primary node
                        if role == 'primary' and not backup_node:
                            # Look for a backup with matching ID pattern (e.g., a1 -> a1b)
                            backup_id = f"{node_id}b"
                            if backup_id in self.account_nodes and self.account_nodes[backup_id]['role'] == 'backup':
                                # Assign this backup to this primary
                                self.node_pairs[node_id] = backup_id
                                
                                # Add backup info to response
                                response['backup_assigned'] = True
                                response['backup_info'] = {
                                    'node_id': backup_id,
                                    'port': self.account_nodes[backup_id]['port']
                                }
                                print(f"Paired primary {node_id} with backup {backup_id}")
                        
                        # If this is a backup node
                        elif role == 'backup' and not primary_node:
                            # Look for primary with matching ID pattern (a1b -> a1)
                            if node_id.endswith('b') and len(node_id) > 1:
                                primary_id = node_id[:-1]  # Remove the 'b' suffix
                                if primary_id in self.account_nodes and self.account_nodes[primary_id]['role'] == 'primary':
                                    # Assign this backup to this primary
                                    self.node_pairs[primary_id] = node_id
                                    
                                    # Add primary info to response
                                    response['primary_assigned'] = True
                                    response['primary_info'] = {
                                        'node_id': primary_id,
                                        'port': self.account_nodes[primary_id]['port']
                                    }
                                    print(f"Paired backup {node_id} with primary {primary_id}")
                        
                        self.save_data()
            
            elif command == 'list_accounts':
                with self.lock:
                    response = {
                        'status': 'success',
                        'accounts': list(self.account_nodes.keys())
                    }
            
            elif command == 'simulate_failure':
                # 模拟节点故障的命令
                node_id = request.get('node_id')
                response = self.simulate_failure(node_id)
            
            elif command == 'recover_node':
                # 恢复节点的命令
                node_id = request.get('node_id')
                response = self.recover_node(node_id)
            
            elif command == 'check_node_status':
                # 检查节点状态命令
                node_id = request.get('node_id')
                
                with self.lock:
                    if node_id in self.account_nodes:
                        node_info = self.account_nodes[node_id]
                        is_active = node_info.get('status') != 'failed'
                        backup_node_id = self.node_pairs.get(node_id)
                        
                        response = {
                            'status': 'success',
                            'node_id': node_id,
                            'is_active': is_active,
                            'role': node_info.get('role', 'primary'),
                            'backup_node': backup_node_id
                        }
                    else:
                        response = {
                            'status': 'error',
                            'message': f'节点 {node_id} 不存在'
                        }
            
            elif command == 'transfer':
                # 处理转账请求
                from_account = request.get('from')
                to_account = request.get('to')
                amount = request.get('amount')
                
                if from_account not in self.account_nodes or to_account not in self.account_nodes:
                    response = {
                        'status': 'error',
                        'message': '一个或两个账户不存在'
                    }
                else:
                    # 检查节点状态，如果是故障节点，尝试使用备份节点
                    from_node_failed = self.account_nodes.get(from_account, {}).get('status') == 'failed'
                    to_node_failed = self.account_nodes.get(to_account, {}).get('status') == 'failed'
                    
                    # 如果源账户节点故障，尝试使用备份
                    if from_node_failed:
                        backup_from = self.node_pairs.get(from_account)
                        if backup_from and backup_from in self.account_nodes:
                            print(f"源账户 {from_account} 故障，使用备份节点 {backup_from}")
                            from_account = backup_from
                        else:
                            response = {
                                'status': 'error',
                                'message': f'源账户 {from_account} 当前不可用，且没有可用的备份节点'
                            }
                            client.send(json.dumps(response).encode('utf-8'))
                            return
                    
                    # 如果目标账户节点故障，尝试使用备份
                    if to_node_failed:
                        backup_to = self.node_pairs.get(to_account)
                        if backup_to and backup_to in self.account_nodes:
                            print(f"目标账户 {to_account} 故障，使用备份节点 {backup_to}")
                            to_account = backup_to
                        else:
                            response = {
                                'status': 'error',
                                'message': f'目标账户 {to_account} 当前不可用，且没有可用的备份节点'
                            }
                            client.send(json.dumps(response).encode('utf-8'))
                            return
                    
                    # 开始两阶段提交协议
                    transaction_id = str(uuid.uuid4())
                    success = self.execute_two_phase_commit(transaction_id, from_account, to_account, amount)
                    
                    if success:
                        original_from = request.get('from')
                        original_to = request.get('to')
                        response = {
                            'status': 'success',
                            'message': f'从 {original_from} 到 {original_to} 的 {amount} 转账已完成',
                            'transaction_id': transaction_id,
                            'used_backup': (from_account != original_from or to_account != original_to)
                        }
                    else:
                        response = {
                            'status': 'error',
                            'message': '转账在两阶段提交过程中失败'
                        }
            
            elif command == 'get_balance':
                # 处理余额查询请求
                account_id = request.get('account_id')
                
                if account_id not in self.account_nodes:
                    response = {
                        'status': 'error',
                        'message': f'账户 {account_id} 不存在'
                    }
                else:
                    # 检查节点状态，如果是故障节点，尝试使用备份节点
                    node_failed = self.account_nodes.get(account_id, {}).get('status') == 'failed'
                    
                    if node_failed:
                        backup_id = self.node_pairs.get(account_id)
                        if backup_id and backup_id in self.account_nodes:
                            print(f"账户 {account_id} 故障，使用备份节点 {backup_id} 查询余额")
                            account_id = backup_id
                        else:
                            response = {
                                'status': 'error',
                                'message': f'账户 {account_id} 当前不可用，且没有可用的备份节点'
                            }
                            client.send(json.dumps(response).encode('utf-8'))
                            return
                    
                    # 转发请求到账户节点
                    try:
                        node_info = self.account_nodes[account_id]
                        host = self.node_hosts.get(account_id, 'localhost')
                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                            s.connect((host, node_info['port']))
                            balance_request = {
                                'command': 'get_balance'
                            }
                            s.send(json.dumps(balance_request).encode('utf-8'))
                            balance_response = json.loads(s.recv(4096).decode('utf-8'))
                            
                            if balance_response.get('status') == 'success':
                                original_account = request.get('account_id')
                                response = {
                                    'status': 'success',
                                    'balance': balance_response.get('balance'),
                                    'account_id': original_account,
                                    'used_backup': account_id != original_account
                                }
                            else:
                                response = {
                                    'status': 'error',
                                    'message': f'从账户 {account_id} 获取余额失败'
                                }
                    except Exception as e:
                        response = {
                            'status': 'error',
                            'message': f'访问账户 {account_id} 出错: {str(e)}'
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
                        print(f"Received verified failure report for node {failed_node_id}")
                        # Promote the backup to primary
                        self.promote_backup_to_primary(reporter_id, failed_node_id)
                        
                        # Remove the failed node
                        with self.lock:
                            self.account_nodes.pop(failed_node_id, None)
                            self.node_pairs.pop(failed_node_id, None)
                            self.save_data()
                        
                        response = {
                            'status': 'success',
                            'message': 'Failure reported and handled'
                        }
                    else:
                        response = {
                            'status': 'error',
                            'message': 'Invalid failure report'
                        }
                else:
                    response = {
                        'status': 'error',
                        'message': 'Invalid failure report format'
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
            # 首先确保我们只对主节点进行操作
            node_info = self.account_nodes.get(account_id)
            if not node_info:
                return False
            
            # 如果这是备份节点，需要找到对应的主节点
            node_role = node_info.get('role', 'primary')
            if node_role == 'backup':
                # 对于备份节点(例如a1b)，找到其对应的主节点(a1)
                if account_id.endswith('b') and len(account_id) > 1:
                    primary_id = account_id[:-1]  # 去掉'b'后缀
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
            # 首先确保我们只对主节点进行操作
            node_info = self.account_nodes.get(account_id)
            if not node_info:
                return False
            
            # 如果这是备份节点，需要找到对应的主节点
            node_role = node_info.get('role', 'primary')
            if node_role == 'backup':
                # 对于备份节点(例如a1b)，找到其对应的主节点(a1)
                if account_id.endswith('b') and len(account_id) > 1:
                    primary_id = account_id[:-1]  # 去掉'b'后缀
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
            with self.lock:
                current_time = time.time()
                dead_nodes = []
                
                for node_id, node_info in list(self.account_nodes.items()):
                    # Check if node hasn't sent a heartbeat in 15 seconds
                    if current_time - node_info.get('last_heartbeat', 0) > 15:
                        dead_nodes.append(node_id)
                
                # Process dead nodes
                for node_id in dead_nodes:
                    print(f"Node {node_id} appears to be dead, processing failover if needed")
                    node_role = self.account_nodes[node_id].get('role', 'primary')
                    
                    # If primary node is dead, promote its backup
                    if node_role == 'primary':
                        backup_id = self.node_pairs.get(node_id)
                        if backup_id and backup_id in self.account_nodes:
                            print(f"Promoting backup {backup_id} to primary")
                            self.promote_backup_to_primary(backup_id, node_id)
                    
                    # Remove the dead node
                    self.account_nodes.pop(node_id, None)
                    
                    # Clean up node pairs
                    if node_role == 'primary':
                        self.node_pairs.pop(node_id, None)
                    elif node_role == 'backup':
                        # Find and remove any entry where this node is a backup
                        for primary_id, backup_id in list(self.node_pairs.items()):
                            if backup_id == node_id:
                                self.node_pairs.pop(primary_id)
                                break
                
                if dead_nodes:
                    self.save_data()
            
            time.sleep(5)  # Check every 5 seconds
            
    def promote_backup_to_primary(self, backup_id, failed_primary_id):
        """Promote a backup node to primary role"""
        try:
            if backup_id not in self.account_nodes:
                print(f"Cannot promote {backup_id}: not found in active nodes")
                return False
            
            backup_port = self.account_nodes[backup_id]['port']
            host = self.node_hosts.get(backup_id, 'localhost')
            
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((host, backup_port))
                promote_request = {
                    'command': 'become_primary'
                }
                s.send(json.dumps(promote_request).encode('utf-8'))
                promote_response = json.loads(s.recv(4096).decode('utf-8'))
                
                if promote_response.get('status') == 'success':
                    # Update node role in coordinator
                    with self.lock:
                        self.account_nodes[backup_id]['role'] = 'primary'
                        
                        # Remove the primary-backup relationship
                        self.node_pairs.pop(failed_primary_id, None)
                        
                        # Save the updated state
                        self.save_data()
                    
                    print(f"Successfully promoted backup {backup_id} to primary")
                    return True
                else:
                    print(f"Failed to promote backup: {promote_response.get('message')}")
                    return False
        
        except Exception as e:
            print(f"Error promoting backup to primary: {e}")
            return False

    def simulate_failure(self, node_id):
        """模拟节点故障并标记节点状态"""
        with self.lock:
            if node_id in self.account_nodes:
                # 标记节点为故障状态
                self.account_nodes[node_id]['status'] = 'failed'
                self.account_nodes[node_id]['failure_time'] = time.time()
                
                print(f"节点 {node_id} 被标记为故障状态")
                
                # 检查是否有备份节点需要接管
                backup_node_id = self.node_pairs.get(node_id)
                
                if backup_node_id and backup_node_id in self.account_nodes:
                    print(f"备份节点 {backup_node_id} 将接管 {node_id} 的工作")
                    # 调用备份节点接管函数
                    self.promote_backup_to_primary(backup_node_id, node_id)
                
                self.save_data()
                
                return {
                    'status': 'success',
                    'message': f'节点 {node_id} 已被标记为故障状态',
                    'backup_node': backup_node_id if backup_node_id else None
                }
            else:
                return {
                    'status': 'error',
                    'message': f'节点 {node_id} 不存在'
                }

    def recover_node(self, node_id):
        """恢复故障节点"""
        with self.lock:
            if node_id in self.account_nodes:
                if self.account_nodes[node_id].get('status') == 'failed':
                    # 清除故障状态
                    self.account_nodes[node_id].pop('status', None)
                    self.account_nodes[node_id].pop('failure_time', None)
                    
                    print(f"节点 {node_id} 已恢复正常状态")
                    
                    self.save_data()
                    
                    return {
                        'status': 'success',
                        'message': f'节点 {node_id} 已恢复正常状态'
                    }
                else:
                    return {
                        'status': 'error',
                        'message': f'节点 {node_id} 当前不处于故障状态'
                    }
            else:
                return {
                    'status': 'error',
                    'message': f'节点 {node_id} 不存在'
                }

if __name__ == "__main__":
    import sys
    
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5000
    
    coordinator = TransactionCoordinator(port)
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Transaction Coordinator shutting down...")
