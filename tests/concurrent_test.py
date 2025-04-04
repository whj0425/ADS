import socket
import json
import threading
import time
import sys
from client import BankClient

class ConcurrentTest:
    def __init__(self, coordinator_port=5010):
        self.client = BankClient(coordinator_port=coordinator_port)
        self.lock = threading.Lock()
        self.successful_transfers = 0
        self.failed_transfers = 0
    
    def initialize_accounts(self, amount=1000):
        """初始化所有账户余额"""
        print(f"初始化所有账户余额为 {amount}...")
        response = self.client.initialize_accounts(amount)
        
        if response['status'] == 'success':
            print(f"成功: 所有账户已初始化为 {amount}")
            return True
        else:
            print(f"错误: {response.get('message', '初始化失败')}")
            return False
    
    def get_accounts(self):
        """获取所有账户列表"""
        response = self.client.list_accounts()
        if response['status'] == 'success':
            return response['accounts']
        else:
            print(f"错误: {response.get('message', '无法获取账户列表')}")
            return []
    
    def perform_transfer(self, from_account, to_account, amount):
        """执行单次转账操作"""
        try:
            print(f"转账 {amount} 从 {from_account} 到 {to_account}...")
            response = self.client.transfer(from_account, to_account, amount)
            
            with self.lock:
                if response['status'] == 'success':
                    self.successful_transfers += 1
                    print(f"成功: 从 {from_account} 转账 {amount} 到 {to_account}")
                else:
                    self.failed_transfers += 1
                    print(f"失败: 从 {from_account} 转账 {amount} 到 {to_account} - {response.get('message', '转账失败')}")
            
            return response['status'] == 'success'
        
        except Exception as e:
            with self.lock:
                self.failed_transfers += 1
            print(f"错误: 转账过程中出现异常 - {e}")
            return False
    
    def check_final_balances(self, account_ids):
        """检查最终余额"""
        print("\n最终余额验证:")
        total_balance = 0
        
        for account_id in account_ids:
            response = self.client.get_balance(account_id)
            if response['status'] == 'success':
                balance = response.get('balance', 0)
                print(f"账户 {account_id} 余额: {balance}")
                total_balance += balance
            else:
                print(f"无法获取账户 {account_id} 的余额: {response.get('message', '未知错误')}")
        
        print(f"所有账户总余额: {total_balance}")

    def run_concurrent_transfers(self, from_account, to_account, amount, num_transfers):
        """运行多个并发转账"""
        threads = []
        
        print(f"开始 {num_transfers} 个并发转账 从 {from_account} 到 {to_account}, 每次 {amount}...")
        self.successful_transfers = 0
        self.failed_transfers = 0
        
        # 创建并启动所有线程
        for i in range(num_transfers):
            thread = threading.Thread(
                target=self.perform_transfer, 
                args=(from_account, to_account, amount)
            )
            threads.append(thread)
            thread.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join()
        
        print(f"\n并发测试完成:")
        print(f"- 成功转账: {self.successful_transfers}")
        print(f"- 失败转账: {self.failed_transfers}")
        return self.successful_transfers, self.failed_transfers
    
    def run_bidirectional_transfers(self, account1, account2, amount, num_transfers):
        """运行双向并发转账"""
        threads = []
        
        print(f"开始双向转账测试，每个方向 {num_transfers} 个并发转账，每次 {amount}...")
        self.successful_transfers = 0
        self.failed_transfers = 0
        
        # 创建并启动所有线程 (两个方向)
        for i in range(num_transfers):
            # 从 account1 到 account2
            thread1 = threading.Thread(
                target=self.perform_transfer, 
                args=(account1, account2, amount)
            )
            # 从 account2 到 account1
            thread2 = threading.Thread(
                target=self.perform_transfer, 
                args=(account2, account1, amount)
            )
            
            threads.append(thread1)
            threads.append(thread2)
            thread1.start()
            thread2.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join()
        
        print(f"\n双向并发测试完成:")
        print(f"- 成功转账: {self.successful_transfers}")
        print(f"- 失败转账: {self.failed_transfers}")
        return self.successful_transfers, self.failed_transfers

def main():
    # 设置协调器端口
    if len(sys.argv) > 1:
        coordinator_port = int(sys.argv[1])
    else:
        coordinator_port = 6010
    
    # 测试案例数量
    if len(sys.argv) > 2:
        test_case = int(sys.argv[2])
    else:
        test_case = 0  # 运行所有测试用例
    
    # 创建测试实例
    tester = ConcurrentTest(coordinator_port)
    
    # 获取账户列表
    accounts = tester.get_accounts()
    if len(accounts) < 2:
        print("错误: 至少需要两个账户节点才能进行测试")
        return
    
    # 简化测试，仅使用前两个账户
    account1 = accounts[0]  # 第一个账户 (例如 a1)
    account2 = accounts[1]  # 第二个账户 (例如 a2)
    
    print(f"使用账户 {account1} 和 {account2} 进行测试")
    
    # 初始化账户
    if not tester.initialize_accounts(1000):
        print("无法初始化账户余额，测试终止")
        return
    
    time.sleep(1)  # 给系统一点时间处理
    
    # 测试用例1: 从同一账户并发转出
    if test_case == 0 or test_case == 1:
        print("\n=== 测试案例 1: 从同一账户并发转出 ===")
        print(f"从 {account1} 向 {account2} 发起 5 个并发转账请求，每个 100")
        print(f"预期: {account1} 余额减少 500，{account2} 余额增加 500")
        tester.run_concurrent_transfers(account1, account2, 100, 5)
        tester.check_final_balances([account1, account2])
        
        # 重新初始化
        if not tester.initialize_accounts(1000):
            print("无法重置账户余额，测试终止")
            return
        time.sleep(1)
    
    # 测试用例2: 向同一账户并发转入
    if test_case == 0 or test_case == 2:
        print("\n=== 测试案例 2: 向同一账户并发转入 ===")
        print(f"从 {account2} 向 {account1} 发起 5 个并发转账请求，每个 100")
        print(f"预期: {account1} 余额增加 500，{account2} 余额减少 500")
        tester.run_concurrent_transfers(account2, account1, 100, 5)
        tester.check_final_balances([account1, account2])
        
        # 重新初始化
        if not tester.initialize_accounts(1000):
            print("无法重置账户余额，测试终止")
            return
        time.sleep(1)
    
    # 测试用例3: 交叉转账
    if test_case == 0 or test_case == 3:
        print("\n=== 测试案例 3: 交叉转账 ===")
        print(f"同时从 {account1} 向 {account2} 和从 {account2} 向 {account1} 发起 3 个并发转账，每个 100")
        print(f"预期: {account1} 和 {account2} 的余额保持不变（假设所有交易都成功）")
        tester.run_bidirectional_transfers(account1, account2, 100, 3)
        tester.check_final_balances([account1, account2])
    
    print("\n并发测试完成")

if __name__ == "__main__":
    main()
