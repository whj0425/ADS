
import socket
import json
import threading
import time
import uuid
import sys
import queue

# 配置
COORDINATOR_HOST = "localhost"  # 本地运行时使用localhost
COORDINATOR_PORT = 5010

class ImprovedTransactionCoordinator:
    """
    改进版事务协调器，引入四个关键机制：
    1. 全局事务队列和排序
    2. 账户级别的锁
    3. 完善的两阶段提交
    4. 事务日志和状态跟踪
    """
    def __init__(self, port=5010, coordinator_id="c1"):
        self.coordinator_id = coordinator_id
        self.port = port
        self.account_nodes = {}  # {node_id: {'port': port, 'last_heartbeat': timestamp, 'role': role}}
        self.node_hosts = {}  # {node_id: host_ip}
        self.transactions = {}  # {transaction_id: {'status': status, 'from': node_id, 'to': node_id, 'amount': amount}}
        self.global_lock = threading.Lock()
        self.account_locks = {}  # 账户级别的锁，{account_id: lock}
        self.transaction_queue = queue.Queue()  # 全局事务队列
        self.data_file = "data/coordinator_data.json"
        self.node_pairs = {}  # {primary_id: backup_id}
        self.transaction_log = []  # 事务日志
        
        # 加载数据
        self.load_data()
        
        # 启动服务器线程
        self.server_thread = threading.Thread(target=self.start_server)
        self.server_thread.daemon = True
        self.server_thread.start()
        
        # 启动事务处理线程
        self.transaction_processor_thread = threading.Thread(target=self.process_transaction_queue)
        self.transaction_processor_thread.daemon = True
        self.transaction_processor_thread.start()
        
        print(f"改进版事务协调器 {self.coordinator_id} 已启动，端口: {self.port}")
    
    def load_data(self):
        # 从文件加载数据的实现
        pass
    
    def save_data(self):
        # 保存数据到文件的实现
        pass
    
    def get_account_lock(self, account_id):
        """获取账户级别的锁"""
        with self.global_lock:
            if account_id not in self.account_locks:
                self.account_locks[account_id] = threading.Lock()
            return self.account_locks[account_id]
    
    def log_transaction(self, transaction_id, action, details):
        """记录事务日志"""
        log_entry = {
            'transaction_id': transaction_id,
            'action': action,
            'timestamp': time.time(),
            'details': details
        }
        with self.global_lock:
            self.transaction_log.append(log_entry)
            # 可以选择同时保存到文件
    
    def start_server(self):
        """启动服务器监听请求"""
        pass  # 实现略
    
    def process_transaction_queue(self):
        """处理全局事务队列中的转账请求"""
        while True:
            try:
                # 从队列中获取转账请求
                transaction_request = self.transaction_queue.get()
                if transaction_request is None:
                    break
                
                # 解析请求
                transaction_id = transaction_request.get('transaction_id')
                from_account = transaction_request.get('from')
                to_account = transaction_request.get('to')
                amount = transaction_request.get('amount')
                client_socket = transaction_request.get('client_socket')
                
                # 执行转账操作
                success = self.execute_transfer_with_retry(transaction_id, from_account, to_account, amount)
                
                # 发送响应
                response = {
                    'status': 'success' if success else 'error',
                    'message': f'Transfer of {amount} from {from_account} to {to_account} completed' if success else 'Transfer failed',
                    'transaction_id': transaction_id
                }
                
                if client_socket:
                    try:
                        client_socket.send(json.dumps(response).encode('utf-8'))
                    except Exception as e:
                        print(f"Error sending response: {e}")
                    finally:
                        client_socket.close()
                
                # 标记任务完成
                self.transaction_queue.task_done()
            
            except Exception as e:
                print(f"Error processing transaction: {e}")
    
    def execute_transfer_with_retry(self, transaction_id, from_account, to_account, amount, max_retries=3):
        """带重试的两阶段提交转账实现"""
        retries = 0
        while retries < max_retries:
            try:
                # 获取涉及的账户锁
                from_lock = self.get_account_lock(from_account)
                to_lock = self.get_account_lock(to_account)
                
                # 按照固定顺序获取锁，避免死锁
                first_lock = from_lock if from_account < to_account else to_lock
                second_lock = to_lock if from_account < to_account else from_lock
                
                # 记录事务开始
                self.log_transaction(transaction_id, 'start', {
                    'from': from_account,
                    'to': to_account,
                    'amount': amount
                })
                
                with first_lock:
                    with second_lock:
                        # 第一阶段：准备
                        self.log_transaction(transaction_id, 'prepare', {'status': 'started'})
                        
                        sender_ready = self.prepare_transfer(from_account, amount, True)
                        if not sender_ready:
                            self.log_transaction(transaction_id, 'prepare', {
                                'status': 'failed',
                                'reason': 'sender not ready'
                            })
                            return False
                        
                        receiver_ready = self.prepare_transfer(to_account, amount, False)
                        if not receiver_ready:
                            self.log_transaction(transaction_id, 'prepare', {
                                'status': 'failed',
                                'reason': 'receiver not ready'
                            })
                            return False
                        
                        self.log_transaction(transaction_id, 'prepare', {'status': 'completed'})
                        
                        # 第二阶段：执行
                        self.log_transaction(transaction_id, 'execute', {'status': 'started'})
                        
                        sender_success = self.execute_transfer(transaction_id, from_account, amount, True)
                        if not sender_success:
                            self.log_transaction(transaction_id, 'execute', {
                                'status': 'failed',
                                'reason': 'sender execution failed'
                            })
                            # 这里应该有回滚机制
                            return False
                        
                        receiver_success = self.execute_transfer(transaction_id, to_account, amount, False)
                        if not receiver_success:
                            self.log_transaction(transaction_id, 'execute', {
                                'status': 'failed',
                                'reason': 'receiver execution failed'
                            })
                            # 这里应该有回滚机制
                            self.rollback_transfer(transaction_id, from_account, amount)
                            return False
                        
                        self.log_transaction(transaction_id, 'execute', {'status': 'completed'})
                        self.log_transaction(transaction_id, 'complete', {'status': 'success'})
                        
                        return True
            
            except Exception as e:
                retries += 1
                self.log_transaction(transaction_id, 'error', {
                    'retry': retries,
                    'error': str(e)
                })
                time.sleep(0.5)  # 短暂延迟后重试
        
        self.log_transaction(transaction_id, 'abort', {
            'reason': 'max retries exceeded'
        })
        return False
    
    # 其他方法实现略

class ImprovedDemoScenarios:
    """改进的演示场景类"""
    def __init__(self, host=COORDINATOR_HOST, port=COORDINATOR_PORT):
        self.host = host
        self.port = port
    
    def send_request(self, request):
        """向协调器发送请求"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(10)  # 增加超时时间
                s.connect((self.host, self.port))
                s.send(json.dumps(request).encode('utf-8'))
                response = json.loads(s.recv(4096).decode('utf-8'))
                return response
        except Exception as e:
            print(f"请求失败: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def run_improved_concurrent_transfers_demo(self):
        """运行改进的并发转账演示"""
        print("\n=== 改进版并发转账演示 ===")
        
        # 创建客户端对象
        from src.client import BankClient
        import threading
        
        client = BankClient(coordinator_host=self.host, coordinator_port=self.port)
        
        # 初始化账户余额
        print("初始化账户余额...")
        response = client.initialize_accounts(10000)
        
        # 检查初始余额
        print("\n检查初始余额...")
        for account in ['a1', 'a2']:
            response = client.get_balance(account)
            if response['status'] == 'success':
                print(f"账户 {account} 余额: {response.get('balance')}")
        
        # 定义转账函数
        def do_transfer(from_acc, to_acc, amount, delay=0):
            time.sleep(delay)
            print(f"执行转账: {from_acc} -> {to_acc} ({amount})...")
            response = client.transfer(from_acc, to_acc, amount)
            if response['status'] == 'success':
                print(f"转账成功: {from_acc} -> {to_acc} ({amount})")
            else:
                print(f"转账失败: {from_acc} -> {to_acc}: {response.get('message')}")
            
            # 立即验证余额
            self.verify_balances([from_acc, to_acc])
            return response
        
        # 创建并发转账
        print("\n开始5个并发转账...")
        
        # 创建多个并发转账
        transfers = [
            ('a1', 'a2', 100, 0),
            ('a2', 'a1', 200, 0.1),
            ('a1', 'a2', 300, 0.2),
            ('a2', 'a1', 400, 0.3),
            ('a1', 'a2', 500, 0.4)
        ]
        
        # 使用线程池而不是直接创建线程
        from concurrent.futures import ThreadPoolExecutor
        results = []
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            # 提交任务到线程池
            futures = [executor.submit(do_transfer, from_acc, to_acc, amount, delay) 
                      for from_acc, to_acc, amount, delay in transfers]
            
            # 收集结果
            for future in futures:
                results.append(future.result())
        
        print("\n所有并发转账已完成")
        
        # 验证最终结果
        self.verify_final_result(transfers, results)
        
        # 检查最终余额
        print("\n检查最终余额...")
        self.verify_balances(['a1', 'a2'])
    
    def verify_balances(self, accounts):
        """验证账户余额"""
        from src.client import BankClient
        client = BankClient(coordinator_host=self.host, coordinator_port=self.port)
        
        for account in accounts:
            response = client.get_balance(account)
            if response['status'] == 'success':
                print(f"账户 {account} 余额: {response.get('balance')}")
            else:
                print(f"无法获取账户 {account} 的余额: {response.get('message')}")
    
    def verify_final_result(self, transfers, results):
        """验证最终转账结果"""
        # 计算预期的净变化
        expected_changes = {}
        for from_acc, to_acc, amount, _ in transfers:
            if from_acc not in expected_changes:
                expected_changes[from_acc] = 0
            if to_acc not in expected_changes:
                expected_changes[to_acc] = 0
            
            # 只考虑成功的转账
            success_transfers = [i for i, r in enumerate(results) if r.get('status') == 'success']
            if i in success_transfers:
                expected_changes[from_acc] -= amount
                expected_changes[to_acc] += amount
        
        print("\n预期账户变化:")
        for account, change in expected_changes.items():
            print(f"账户 {account} 应变化: {change}")

def main():
    # 如果命令行参数指定了协调器地址和端口，则使用指定的值
    host = COORDINATOR_HOST
    port = COORDINATOR_PORT
    
    if len(sys.argv) > 1:
        host = sys.argv[1]
    if len(sys.argv) > 2:
        port = int(sys.argv[2])
    
    print(f"连接到协调器: {host}:{port}")
    demo = ImprovedDemoScenarios(host, port)
    
    # 运行改进版并发转账演示
    demo.run_improved_concurrent_transfers_demo()

if __name__ == "__main__":
    main() 