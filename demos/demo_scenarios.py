
import socket
import json
import time
import sys

# 配置 
COORDINATOR_HOST = "localhost"  # 本地运行时使用localhost
COORDINATOR_PORT = 5010

class DemoScenarios:
    def __init__(self, host=COORDINATOR_HOST, port=COORDINATOR_PORT):
        self.host = host
        self.port = port
    
    def send_request(self, request):
        """向协调器发送请求"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5)
                s.connect((self.host, self.port))
                s.send(json.dumps(request).encode('utf-8'))
                response = json.loads(s.recv(4096).decode('utf-8'))
                return response
        except Exception as e:
            print(f"请求失败: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def simulate_node_failure(self, node_id):
        """模拟节点故障"""
        request = {
            'command': 'simulate_failure',
            'node_id': node_id
        }
        print(f"正在模拟节点 {node_id} 故障...")
        response = self.send_request(request)
        if response['status'] == 'success':
            print(f"节点 {node_id} 已被标记为故障状态")
            if response.get('backup_node'):
                print(f"备份节点 {response['backup_node']} 将接管")
        else:
            print(f"故障模拟失败: {response.get('message')}")
        return response
    
    def check_node_status(self, node_id):
        """检查节点状态"""
        request = {
            'command': 'check_node_status',
            'node_id': node_id
        }
        print(f"正在检查节点 {node_id} 状态...")
        response = self.send_request(request)
        if response['status'] == 'success':
            status = "活跃" if response.get('is_active', False) else "故障"
            role = response.get('role', 'unknown')
            print(f"节点 {node_id} 状态: {status}, 角色: {role}")
            if response.get('backup_node'):
                print(f"备份节点: {response['backup_node']}")
        else:
            print(f"状态检查失败: {response.get('message')}")
        return response
    
    def list_accounts(self):
        """列出所有账户节点"""
        request = {
            'command': 'list_accounts'
        }
        print("正在获取所有账户节点...")
        response = self.send_request(request)
        if response['status'] == 'success':
            print("可用的账户节点:")
            for account in response.get('accounts', []):
                print(f"  - {account}")
        else:
            print(f"获取账户列表失败: {response.get('message')}")
        return response
    
    def run_transfer_demo(self):
        """运行转账演示"""
        print("\n=== 转账演示 ===")
        
        # 创建客户端对象
        from src.client import BankClient
        client = BankClient(coordinator_host=self.host, coordinator_port=self.port)
        
        # 初始化账户余额
        print("初始化账户余额...")
        response = client.initialize_accounts(10000)
        if response['status'] == 'success':
            print("所有账户已初始化余额为10000")
        else:
            print(f"初始化账户失败: {response.get('message')}")
            return
        
        # 检查账户余额
        print("\n检查初始余额...")
        for account in ['a1', 'a2']:
            response = client.get_balance(account)
            if response['status'] == 'success':
                print(f"账户 {account} 余额: {response.get('balance')}")
        
        # 执行第一次转账
        print("\n执行转账: a1 -> a2 (1000)...")
        response = client.transfer('a1', 'a2', 1000)
        if response['status'] == 'success':
            print("转账成功!")
        else:
            print(f"转账失败: {response.get('message')}")
        
        # 再次检查余额
        print("\n检查转账后余额...")
        for account in ['a1', 'a2']:
            response = client.get_balance(account)
            if response['status'] == 'success':
                print(f"账户 {account} 余额: {response.get('balance')}")
    
    def run_failure_recovery_demo(self):
        """运行故障恢复演示"""
        print("\n=== 故障恢复演示 ===")
        
        # 列出所有账户
        self.list_accounts()
        
        # 模拟a1节点故障
        print("\n模拟节点a1故障...")
        self.simulate_node_failure('a1')
        
        # 等待故障检测和恢复
        print("\n等待故障检测和恢复过程...(5秒)")
        time.sleep(5)
        
        # 检查节点状态
        print("\n检查节点状态...")
        self.check_node_status('a1')
        self.check_node_status('a1b')
        
        # 在故障期间执行转账
        print("\n在主节点故障期间尝试转账...")
        from src.client import BankClient
        client = BankClient(coordinator_host=self.host, coordinator_port=self.port)
        
        response = client.transfer('a1', 'a2', 500)
        if response['status'] == 'success':
            print("转账成功! (通过备份节点完成)")
        else:
            print(f"转账失败: {response.get('message')}")
        
        # 检查账户余额
        print("\n检查转账后余额...")
        for account in ['a1', 'a2']:
            response = client.get_balance(account)
            print(f"账户 {account} 余额: {response.get('balance', '未知')}")
    
    def run_concurrent_transfers_demo(self):
        """运行并发转账演示"""
        print("\n=== 并发转账演示 (存在问题的版本) ===")
        print("注意：此演示故意保留了并发处理问题，用于展示分布式并发转账的常见问题")
        print("问题包括：")
        print("1. 并发请求处理不当 - 同时处理多个请求导致冲突")
        print("2. 锁粒度过大 - 不相关账户的操作也会互相阻塞")
        print("3. 错误处理不完善 - 转账失败时没有适当的恢复机制")
        print("4. 缺乏事务状态追踪 - 无法追踪每笔交易的执行状态")
        print("执行此演示后，可能会出现部分转账失败、余额查询超时或不一致的情况")
        
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
        
        # 创建并发转账
        print("\n开始5个并发转账...")
        threads = []
        
        # 创建多个并发转账
        transfers = [
            ('a1', 'a2', 100, 0),
            ('a2', 'a1', 200, 0.1),
            ('a1', 'a2', 300, 0.2),
            ('a2', 'a1', 400, 0.3),
            ('a1', 'a2', 500, 0.4)
        ]
        
        for from_acc, to_acc, amount, delay in transfers:
            t = threading.Thread(target=do_transfer, args=(from_acc, to_acc, amount, delay))
            threads.append(t)
            t.start()
        
        # 等待所有转账完成
        for t in threads:
            t.join()
        
        print("\n所有并发转账已完成")
        
        # 检查最终余额
        print("\n检查最终余额...")
        for account in ['a1', 'a2']:
            response = client.get_balance(account)
            if response['status'] == 'success':
                print(f"账户 {account} 余额: {response.get('balance')}")

    def run_improved_concurrent_transfers_demo(self):
        """运行改进的并发转账演示"""
        print("\n=== 改进版并发转账演示 ===")
        print("此演示将使用以下改进：")
        print("1. 全局事务队列和排序 - 按顺序处理转账请求，避免混乱")
        print("2. 账户级别的锁 - 只有涉及同一账户的交易才互相等待")
        print("3. 完善的两阶段提交 - 完整实现事务处理流程，包括错误恢复")
        print("4. 事务日志和状态跟踪 - 记录所有交易步骤和状态，方便追踪")
        
        # 这里我们只是运行测试，实际改进只会在协调器部分实现
        # 但通过测试可以看到效果的改进
        
        # 创建客户端对象
        from src.client import BankClient
        import threading
        from concurrent.futures import ThreadPoolExecutor
        
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
        expected_changes = {'a1': 0, 'a2': 0}
        success_count = 0
        
        for i, (from_acc, to_acc, amount, _) in enumerate(transfers):
            if results[i]['status'] == 'success':
                expected_changes[from_acc] -= amount
                expected_changes[to_acc] += amount
                success_count += 1
        
        print(f"\n成功完成的转账数: {success_count}/{len(transfers)}")
        print("预期账户变化:")
        for account, change in expected_changes.items():
            print(f"账户 {account} 预期变化: {change}")
        
        # 检查最终余额
        print("\n检查最终余额...")
        balances = {}
        for account in ['a1', 'a2']:
            response = client.get_balance(account)
            if response['status'] == 'success':
                balances[account] = response.get('balance')
                print(f"账户 {account} 余额: {balances[account]}")
        
        # 验证余额是否符合预期
        if len(balances) == 2:
            a1_change = balances['a1'] - 10000
            a2_change = balances['a2'] - 10000
            print(f"\n账户 a1 实际变化: {a1_change}")
            print(f"账户 a2 实际变化: {a2_change}")
            
            if a1_change == expected_changes['a1'] and a2_change == expected_changes['a2']:
                print("\n✅ 验证成功：余额变化符合预期！")
            else:
                print("\n❌ 验证失败：余额变化与预期不符！")
                print(f"a1 预期变化 {expected_changes['a1']}, 实际变化 {a1_change}")
                print(f"a2 预期变化 {expected_changes['a2']}, 实际变化 {a2_change}")

def print_menu():
    """打印演示菜单"""
    print("\n=== 分布式银行系统演示 ===")
    print("1. 转账演示")
    print("2. 故障恢复演示")
    print("3. 并发转账演示 (有问题)")
    print("4. 列出所有账户")
    print("5. 检查节点状态")
    print("6. 模拟节点故障")
    print("7. 改进版并发转账演示")
    print("0. 退出")
    print("请选择演示场景: ", end="")

def main():
    # 如果命令行参数指定了协调器地址和端口，则使用指定的值
    host = COORDINATOR_HOST
    port = COORDINATOR_PORT
    
    if len(sys.argv) > 1:
        host = sys.argv[1]
    if len(sys.argv) > 2:
        port = int(sys.argv[2])
    
    print(f"连接到协调器: {host}:{port}")
    demo = DemoScenarios(host, port)
    
    while True:
        print_menu()
        try:
            choice = input().strip()
            
            if choice == '0':
                break
            elif choice == '1':
                demo.run_transfer_demo()
            elif choice == '2':
                demo.run_failure_recovery_demo()
            elif choice == '3':
                demo.run_concurrent_transfers_demo()
            elif choice == '4':
                demo.list_accounts()
            elif choice == '5':
                node_id = input("请输入要检查的节点ID: ").strip()
                demo.check_node_status(node_id)
            elif choice == '6':
                node_id = input("请输入要模拟故障的节点ID: ").strip()
                demo.simulate_node_failure(node_id)
            elif choice == '7':
                demo.run_improved_concurrent_transfers_demo()
            else:
                print("无效选择，请重试")
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"发生错误: {e}")
    
    print("演示结束")

if __name__ == "__main__":
    main() 