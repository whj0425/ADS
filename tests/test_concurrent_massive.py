import socket
import json
import time
import sys
import os
import threading
from concurrent.futures import ThreadPoolExecutor
import random

# Add project root directory to Python path for correct importing of src package
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)  # Get project root directory
if root_dir not in sys.path:
    sys.path.append(root_dir)  # Add project root directory to Python path

from src.client import BankClient

# 配置
COORDINATOR_HOST = "localhost"
COORDINATOR_PORT = 5010

class MassiveConcurrentTransferTest:
    """测试大量互相转账的类"""
    
    def __init__(self, host=COORDINATOR_HOST, port=COORDINATOR_PORT):
        self.host = host
        self.port = port
        self.client = BankClient(coordinator_host=host, coordinator_port=port)
        self.lock = threading.Lock()
        self.success_count = 0
        self.fail_count = 0
        
    def transfer_task(self, from_acc, to_acc, amount, delay=0):
        """执行单次转账任务"""
        time.sleep(delay)
        try:
            response = self.client.transfer(from_acc, to_acc, amount)
            with self.lock:
                if response['status'] == 'success':
                    self.success_count += 1
                    print(f"转账成功: {from_acc} -> {to_acc} ({amount})")
                else:
                    self.fail_count += 1
                    print(f"转账失败: {from_acc} -> {to_acc}: {response.get('message')}")
            return response
        except Exception as e:
            with self.lock:
                self.fail_count += 1
                print(f"转账异常: {from_acc} -> {to_acc}: {str(e)}")
            return {'status': 'error', 'message': str(e)}
    
    def run_massive_concurrent_transfers(self, num_transfers=100, accounts=None):
        """运行大量互相转账的测试
        
        Args:
            num_transfers: 互相转账的总次数
            accounts: 账户列表，如果为None则使用默认的a1和a2
        """
        if accounts is None:
            accounts = ['a1', 'a2']
            
        print(f"\n=== 大量并发转账测试 ({num_transfers}次) ===")
        
        # 初始化账户余额
        print("初始化账户余额...")
        response = self.client.initialize_accounts(10000)
        if response['status'] != 'success':
            print(f"账户初始化失败: {response.get('message')}")
            return
        
        # 检查初始余额
        print("\n检查初始余额...")
        initial_balances = {}
        for account in accounts:
            response = self.client.get_balance(account)
            if response['status'] == 'success':
                initial_balances[account] = response.get('balance')
                print(f"账户 {account} 余额: {initial_balances[account]}")
        
        # 重置计数器
        self.success_count = 0
        self.fail_count = 0
        
        # 创建转账任务
        transfers = []
        total_expected_changes = {acc: 0 for acc in accounts}
        
        for i in range(num_transfers):
            # 随机选择发送和接收账户
            from_acc = random.choice(accounts)
            to_acc = random.choice([acc for acc in accounts if acc != from_acc])
            amount = random.randint(1, 50)  # 随机金额1-50
            delay = random.uniform(0, 0.5)  # 随机延迟0-0.5秒
            
            transfers.append((from_acc, to_acc, amount, delay))
            
            # 计算预期变化（假设所有转账都成功）
            total_expected_changes[from_acc] -= amount
            total_expected_changes[to_acc] += amount
        
        start_time = time.time()
        
        # 使用线程池执行转账
        results = []
        with ThreadPoolExecutor(max_workers=20) as executor:
            # 提交任务到线程池
            futures = [executor.submit(self.transfer_task, from_acc, to_acc, amount, delay) 
                      for from_acc, to_acc, amount, delay in transfers]
            
            # 收集结果
            for future in futures:
                results.append(future.result())
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        print(f"\n所有并发转账完成，耗时: {execution_time:.2f}秒")
        print(f"成功转账: {self.success_count}/{num_transfers}")
        print(f"失败转账: {self.fail_count}/{num_transfers}")
        
        # 计算实际成功的转账对账户余额的影响
        actual_expected_changes = {acc: 0 for acc in accounts}
        for i, (from_acc, to_acc, amount, _) in enumerate(transfers):
            if results[i]['status'] == 'success':
                actual_expected_changes[from_acc] -= amount
                actual_expected_changes[to_acc] += amount
        
        print("\n预期账户变化:")
        for account, change in actual_expected_changes.items():
            print(f"账户 {account} 预期变化: {change}")
        
        # 检查最终余额
        print("\n检查最终余额...")
        final_balances = {}
        for account in accounts:
            response = self.client.get_balance(account)
            if response['status'] == 'success':
                final_balances[account] = response.get('balance')
                print(f"账户 {account} 最终余额: {final_balances[account]}")
        
        # 验证余额是否符合预期
        all_verified = True
        print("\n余额验证:")
        for account in accounts:
            if account in initial_balances and account in final_balances:
                actual_change = final_balances[account] - initial_balances[account]
                expected_change = actual_expected_changes[account]
                print(f"账户 {account} - 初始: {initial_balances[account]}, 最终: {final_balances[account]}, 变化: {actual_change}, 预期: {expected_change}")
                
                if actual_change == expected_change:
                    print(f"✅ 账户 {account} 验证成功!")
                else:
                    print(f"❌ 账户 {account} 验证失败! 差异: {actual_change - expected_change}")
                    all_verified = False
        
        if all_verified:
            print("\n✅ 所有账户余额验证成功! 系统在高并发情况下保持一致性。")
        else:
            print("\n❌ 账户余额验证失败! 系统可能存在一致性问题。")
            
        return {
            'success_count': self.success_count,
            'fail_count': self.fail_count,
            'execution_time': execution_time,
            'all_verified': all_verified
        }


def run_test(host=COORDINATOR_HOST, port=COORDINATOR_PORT, num_transfers=100):
    """运行测试函数"""
    test = MassiveConcurrentTransferTest(host, port)
    return test.run_massive_concurrent_transfers(num_transfers)


if __name__ == "__main__":
    host = COORDINATOR_HOST
    port = COORDINATOR_PORT
    num_transfers = 100
    
    # 从命令行参数获取配置
    if len(sys.argv) > 1:
        host = sys.argv[1]
    if len(sys.argv) > 2:
        port = int(sys.argv[2])
    if len(sys.argv) > 3:
        num_transfers = int(sys.argv[3])
    
    print(f"连接到协调器: {host}:{port}")
    print(f"准备执行 {num_transfers} 次并发转账")
    
    run_test(host, port, num_transfers) 