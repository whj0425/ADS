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
                    
                    # 添加调试信息
                    print(f"数据加载完成。节点状态：")
                    for node_id, node_info in self.account_nodes.items():
                        status = node_info.get('status', 'active')
                        role = node_info.get('role', 'primary')
                        print(f"  - 节点 {node_id}: 状态={status}, 角色={role}")
            except Exception as e:
                print(f"Error loading coordinator data: {e}")
    
    def save_data(self):
        # 添加调试信息
        print(f"正在保存节点状态信息:")
        for node_id, node_info in self.account_nodes.items():
            status = node_info.get('status', 'active')
            role = node_info.get('role', 'primary')
            print(f"  - 节点 {node_id}: 状态={status}, 角色={role}")
            
        data = {
            'account_nodes': self.account_nodes,
            'transactions': self.transactions,
            'node_pairs': self.node_pairs,
            'node_hosts': self.node_hosts
        }
        
        # 确保目录存在
        os.makedirs(os.path.dirname(self.data_file), exist_ok=True)
        
        try:
            with open(self.data_file, 'w') as f:
                json.dump(data, f, indent=2)
            print(f"数据已保存到 {self.data_file}")
        except Exception as e:
            print(f"保存数据时出错: {e}")
    
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
                role_from_heartbeat = request.get('role', 'primary')  # Get role reported by node
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
                # 模拟节点故障的命令
                node_id = request.get('node_id')
                
                with self.lock:
                    if node_id in self.account_nodes:
                        # 1. 首先无条件地标记节点为故障状态
                        print(f"模拟前节点 {node_id} 状态: {self.account_nodes[node_id].get('status', 'active')}")
                        self.account_nodes[node_id]['status'] = 'failed'
                        self.account_nodes[node_id]['failure_time'] = time.time()
                        print(f"节点 {node_id} 已被标记为故障状态: {self.account_nodes[node_id]}")
                        
                        # 2. 立即保存状态变更到磁盘
                        self.save_data()
                        
                        # 3. 再次确认节点状态已变更
                        assert self.account_nodes[node_id]['status'] == 'failed', "节点状态未成功变更！"
                        print(f"确认节点 {node_id} 状态已更新为: {self.account_nodes[node_id].get('status')}")
                        
                        # 4. 构建基本响应
                        backup_node_id = self.node_pairs.get(node_id)
                        response = {
                            'status': 'success',
                            'message': f'节点 {node_id} 已被标记为故障状态',
                            'backup_node': backup_node_id,
                            'node_status': self.account_nodes[node_id].get('status')
                        }
                        
                        # 5. 尝试提升备份节点，但这不影响节点已被标记为故障的状态
                        backup_promoted = False
                        if backup_node_id and backup_node_id in self.account_nodes:
                            print(f"尝试提升备份节点 {backup_node_id} 接管 {node_id} 的工作")
                            
                            # 首先在协调器端更新备份节点角色为主节点
                            try:
                                previous_role = self.account_nodes[backup_node_id].get('role', 'backup')
                                self.account_nodes[backup_node_id]['role'] = 'primary'
                                print(f"备份节点 {backup_node_id} 角色从 {previous_role} 更新为 {self.account_nodes[backup_node_id]['role']}")
                                
                                # 更新节点对应关系
                                self.node_pairs.pop(node_id, None)
                                self.save_data()
                                backup_promoted = True
                                print(f"备份节点 {backup_node_id} 在协调器中已被提升为主节点")
                                
                                # 尝试通知备份节点，但即使失败也视为成功
                                try:
                                    backup_port = self.account_nodes[backup_node_id]['port']
                                    host = self.node_hosts.get(backup_node_id, 'localhost')
                                    
                                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                                        s.settimeout(2)  # 短超时，避免长时间等待
                                        s.connect((host, backup_port))
                                        promote_request = {
                                            'command': 'become_primary'
                                        }
                                        s.send(json.dumps(promote_request).encode('utf-8'))
                                        # 忽略响应，我们已经在协调器端完成了状态更新
                                except Exception as e:
                                    print(f"通知备份节点时出错，但这不影响状态更新: {e}")
                            except Exception as e:
                                print(f"提升备份节点过程中出错: {e}")
                        
                        # 6. 最后再次检查并确认状态
                        print(f"最终确认节点 {node_id} 状态: {self.account_nodes[node_id].get('status', '未知')}")
                        if backup_node_id:
                            print(f"最终确认备份节点 {backup_node_id} 角色: {self.account_nodes[backup_node_id].get('role', '未知')}")
                        
                        # 7. 更新响应以包含备份节点提升状态
                        response['backup_promoted'] = backup_promoted
                        response['final_node_status'] = self.account_nodes[node_id].get('status')
                    else:
                        response = {
                            'status': 'error',
                            'message': f'节点 {node_id} 不存在'
                        }
            
            elif command == 'recover_node':
                # 恢复节点的命令
                node_id = request.get('node_id') # The node being recovered (e.g., a1)
                
                with self.lock:
                    if node_id in self.account_nodes:
                        print(f"恢复前节点 {node_id} 状态: {self.account_nodes[node_id].get('status', 'active')}")
                        
                        if self.account_nodes[node_id].get('status') == 'failed':
                            # Node is indeed marked as failed, proceed with recovery
                            
                            # Step 1: Find the node that took over (the original backup, now primary)
                            potential_takeover_node_id = f"{node_id}b"
                            takeover_node_info = self.account_nodes.get(potential_takeover_node_id)
                            
                            latest_balance = None
                            sync_success = False

                            if takeover_node_info and takeover_node_info.get('role') == 'primary':
                                print(f"找到接管节点 {potential_takeover_node_id}，尝试同步状态...")
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
                                            print(f"从 {potential_takeover_node_id} 获取到最新余额: {latest_balance}")
                                        else:
                                            print(f"无法从 {potential_takeover_node_id} 获取余额: {balance_response.get('message')}")
                                
                                except Exception as e:
                                    print(f"连接接管节点 {potential_takeover_node_id} 获取余额时出错: {e}")

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
                                                print(f"成功将最新余额同步到恢复中的节点 {node_id}")
                                            else:
                                                 print(f"节点 {node_id} 同步余额失败: {set_response.get('message')}")
                                    except Exception as e:
                                        print(f"连接恢复中节点 {node_id} 设置余额时出错: {e}")
                            else:
                                print(f"警告: 无法找到有效的接管节点 {potential_takeover_node_id} 来同步状态。节点 {node_id} 将使用其本地状态恢复。")
                                # Decide if recovery should proceed without sync or fail
                                # For simulation, we might allow it, but log a warning.
                                sync_success = True # Allow recovery without sync for now

                            # Step 4: If sync was successful (or skipped), mark the node as active
                            if sync_success:
                                self.account_nodes[node_id].pop('status', None)
                                self.account_nodes[node_id].pop('failure_time', None)
                                # Consider if the recovered node should become primary again or backup
                                # For simplicity, let's keep it primary for now, assuming a1b might fail later.
                                # self.account_nodes[node_id]['role'] = 'primary' # Or maybe 'backup'?

                                print(f"节点 {node_id} 已完成恢复过程并标记为活跃状态: {self.account_nodes[node_id]}")
                                self.save_data()
                                print(f"确认节点 {node_id} 当前状态: {self.account_nodes[node_id].get('status', 'active')}")
                                
                                response = {
                                    'status': 'success',
                                    'message': f'节点 {node_id} 已恢复正常状态' + ('并同步了最新余额' if latest_balance is not None else ' (未执行状态同步)'),
                                    'node_info': self.account_nodes[node_id]
                                }
                            else:
                                print(f"节点 {node_id} 状态同步失败，恢复中止。")
                                response = {
                                    'status': 'error',
                                    'message': f'节点 {node_id} 状态同步失败，无法恢复。'
                                }
                        else:
                            print(f"节点 {node_id} 当前不处于故障状态: {self.account_nodes[node_id]}")
                            response = {
                                'status': 'error',
                                'message': f'节点 {node_id} 当前不处于故障状态',
                                'node_info': self.account_nodes[node_id]
                            }
                    else:
                        response = {
                            'status': 'error',
                            'message': f'节点 {node_id} 不存在'
                        }
            
            elif command == 'check_node_status':
                # 检查节点状态命令
                node_id = request.get('node_id')
                
                with self.lock:
                    if node_id in self.account_nodes:
                        node_info = self.account_nodes[node_id]
                        # 直接检查status字段是否为'failed'
                        is_failed = node_info.get('status') == 'failed'
                        is_active = not is_failed
                        backup_node_id = self.node_pairs.get(node_id)
                        
                        # 调试信息
                        print(f"DEBUG - 节点信息: {node_info}")
                        print(f"DEBUG - 节点 {node_id} 状态: {'failed' if is_failed else 'active'}")
                        
                        # 直接从内存中获取所有状态信息
                        response = {
                            'status': 'success',
                            'node_id': node_id,
                            'is_active': is_active,
                            'role': node_info.get('role', 'primary'),
                            'backup_node': backup_node_id,
                            'state': 'failed' if is_failed else 'active',  # 确保状态与is_active一致
                            'node_info': node_info,  # 返回完整节点信息用于调试
                            'last_heartbeat': node_info.get('last_heartbeat'),
                            'port': node_info.get('port')
                        }
                    else:
                        response = {
                            'status': 'error',
                            'message': f'节点 {node_id} 不存在'
                        }
            
            elif command == 'transfer':
                # Handle transfer request
                from_account = request.get('from')
                to_account = request.get('to')
                amount = request.get('amount')
                
                # 记录原始账户ID
                original_from = from_account
                original_to = to_account
                
                if from_account not in self.account_nodes or to_account not in self.account_nodes:
                    response = {
                        'status': 'error',
                        'message': '一个或两个账户不存在'
                    }
                else:
                    # 检查节点状态，如果是故障节点，立即重定向到备份节点
                    from_node_failed = self.account_nodes.get(from_account, {}).get('status') == 'failed'
                    to_node_failed = self.account_nodes.get(to_account, {}).get('status') == 'failed'
                    
                    # 如果源账户节点故障，重定向到备份
                    redirected = False
                    if from_node_failed:
                        backup_from = self.node_pairs.get(from_account)
                        if backup_from and backup_from in self.account_nodes:
                            print(f"源账户 {from_account} 已故障，重定向到备份节点 {backup_from}")
                            from_account = backup_from
                            redirected = True
                        else:
                            # NEW LOGIC: If not in node_pairs, try deducing backup ID and check if it's the new primary
                            potential_backup_id = f"{from_account}b"
                            if potential_backup_id in self.account_nodes and self.account_nodes[potential_backup_id].get('role') == 'primary':
                                print(f"源账户 {from_account} 已故障，重定向到已提升为主节点的原备份 {potential_backup_id}")
                                from_account = potential_backup_id
                                redirected = True
                            else:
                                response = {
                                    'status': 'error',
                                    'message': f'源账户 {original_from} 当前不可用且没有可用的接管节点' # Use original ID in error message
                                }
                                client.send(json.dumps(response).encode('utf-8'))
                                return

                    # 如果目标账户节点故障，重定向到备份
                    if to_node_failed:
                        backup_to = self.node_pairs.get(to_account)
                        if backup_to and backup_to in self.account_nodes:
                            print(f"目标账户 {to_account} 已故障，重定向到备份节点 {backup_to}")
                            to_account = backup_to
                            redirected = True
                        else:
                            # NEW LOGIC: If not in node_pairs, try deducing backup ID and check if it's the new primary
                            potential_backup_id = f"{to_account}b"
                            if potential_backup_id in self.account_nodes and self.account_nodes[potential_backup_id].get('role') == 'primary':
                                print(f"目标账户 {to_account} 已故障，重定向到已提升为主节点的原备份 {potential_backup_id}")
                                to_account = potential_backup_id
                                redirected = True
                            else:
                                response = {
                                    'status': 'error',
                                    'message': f'目标账户 {original_to} 当前不可用且没有可用的接管节点' # Use original ID in error message
                                }
                                client.send(json.dumps(response).encode('utf-8'))
                                return
                    
                    # 开始两阶段提交协议
                    transaction_id = str(uuid.uuid4())
                    success = self.execute_two_phase_commit(transaction_id, from_account, to_account, amount)
                    
                    if success:
                        response = {
                            'status': 'success',
                            'message': f'从 {original_from} 到 {original_to} 的 {amount} 转账已完成',
                            'transaction_id': transaction_id,
                            'used_backup': redirected
                        }
                    else:
                        response = {
                            'status': 'error',
                            'message': '转账在两阶段提交过程中失败'
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
                    # 记录原始账户ID以便响应中显示
                    original_account = account_id
                    
                    # 检查节点状态，如果是故障节点，立即重定向到备份节点
                    node_failed = self.account_nodes.get(account_id, {}).get('status') == 'failed'
                    
                    if node_failed:
                        backup_id = self.node_pairs.get(account_id)
                        if backup_id and backup_id in self.account_nodes:
                            print(f"账户 {account_id} 已故障，重定向到备份节点 {backup_id}")
                            account_id = backup_id
                        else:
                            # NEW LOGIC: If not in node_pairs, try deducing backup ID and check if it's the new primary
                            potential_backup_id = f"{account_id}b"
                            if potential_backup_id in self.account_nodes and self.account_nodes[potential_backup_id].get('role') == 'primary':
                                print(f"账户 {original_account} 已故障，重定向到已提升为主节点的原备份 {potential_backup_id}")
                                account_id = potential_backup_id # Update account_id to the promoted node
                            else:
                                response = {
                                    'status': 'error',
                                    'message': f'账户 {original_account} 当前不可用且没有可用的接管节点' # Use original ID in error message
                                }
                                client.send(json.dumps(response).encode('utf-8'))
                                return
                    
                    # 转发请求到账户节点
                    try:
                        node_info = self.account_nodes[account_id]
                        host = self.node_hosts.get(account_id, 'localhost')
                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                            s.settimeout(3)  # 增加超时到3秒，避免长时间等待
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
                                    'message': f'无法从账户 {account_id} 获取余额'
                                }
                    except Exception as e:
                        response = {
                            'status': 'error',
                            'message': f'访问账户 {account_id} 时出错: {str(e)}'
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
            nodes_marked_failed_this_cycle = []
            with self.lock:
                current_time = time.time()
                
                for node_id, node_info in list(self.account_nodes.items()):
                    # Skip nodes already marked as failed
                    if node_info.get('status') == 'failed':
                        continue

                    # Check if node hasn't sent a heartbeat in 15 seconds
                    if current_time - node_info.get('last_heartbeat', 0) > 15:
                        print(f"Heartbeat timeout for node {node_id}. Marking as failed.")
                        
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
            
            time.sleep(5)  # Check every 5 seconds
            
    def promote_backup_to_primary(self, backup_id, failed_primary_id):
        """将备份节点提升为主节点"""
        # 首先检查备份节点是否存在
        if backup_id not in self.account_nodes:
            print(f"无法提升节点 {backup_id}：节点不存在")
            return False
        
        # 1. 在协调器内部更新状态（这步永远不应该失败）
        try:
            with self.lock:
                # 将备份节点角色更改为主节点
                self.account_nodes[backup_id]['role'] = 'primary'
                
                # 移除主备关系
                self.node_pairs.pop(failed_primary_id, None)
                
                # 保存更新后的状态
                self.save_data()
                
            print(f"协调器已将备份节点 {backup_id} 的角色更新为主节点")
        except Exception as e:
            print(f"更新协调器内部状态时出错: {e}")
            return False
        
        # 2. 尝试通知备份节点，但即使失败也不影响状态更新
        success = False
        try:
            backup_port = self.account_nodes[backup_id]['port']
            host = self.node_hosts.get(backup_id, 'localhost')
            
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(2)  # 短超时，防止长时间阻塞
                s.connect((host, backup_port))
                promote_request = {
                    'command': 'become_primary'
                }
                s.send(json.dumps(promote_request).encode('utf-8'))
                
                # 尝试获取响应但不依赖它
                try:
                    response_data = s.recv(4096)
                    promote_response = json.loads(response_data.decode('utf-8'))
                    success = promote_response.get('status') == 'success'
                    
                    if success:
                        print(f"备份节点 {backup_id} 确认已接收提升为主节点的命令")
                    else:
                        print(f"备份节点 {backup_id} 返回错误: {promote_response.get('message')}")
                except Exception as e:
                    print(f"读取备份节点响应时出错: {e}")
        except Exception as e:
            print(f"向备份节点 {backup_id} 发送提升通知时出错: {e}")
        
        # 不管通知是否成功，状态已更新，所以返回成功
        return True

if __name__ == "__main__":
    import sys
    
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5000
    
    coordinator = TransactionCoordinator(port)
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Transaction Coordinator shutting down...")
