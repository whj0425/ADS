import socket
import json
import time
import sys
import os

# 添加项目根目录到Python路径，以便正确导入src包
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)  # 获取项目根目录
if root_dir not in sys.path:
    sys.path.append(root_dir)  # 将项目根目录添加到Python路径

# 配置 
COORDINATOR_HOST = "localhost"  # 本地运行时使用localhost
COORDINATOR_PORT = 5010

class DemoScenarios:
    def __init__(self, host=COORDINATOR_HOST, port=COORDINATOR_PORT):
        self.host = host
        self.port = port
    
    def send_request(self, request):
        """向协调器发送请求"""
        max_retries = 2
        retry_count = 1
        last_error = None
        
        while retry_count <= max_retries:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(10)  # 增加超时时间到10秒
                    s.connect((self.host, self.port))
                    s.send(json.dumps(request).encode('utf-8'))
                    response = json.loads(s.recv(4096).decode('utf-8'))
                    return response
            except Exception as e:
                last_error = e
                retry_count += 1
                if retry_count <= max_retries:
                    print(f"请求失败(尝试 {retry_count}/{max_retries}): {e}，正在重试...")
                    time.sleep(1)  # 等待1秒后重试
                else:
                    print(f"请求失败: {e}，已达到最大重试次数。")
        
        # 如果所有重试均失败
        return {'status': 'error', 'message': str(last_error)}
    
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
            
            # 显示最终节点状态
            if 'final_node_status' in response:
                print(f"节点最终状态: {response['final_node_status']}")
            
            if response.get('backup_node'):
                print(f"备份节点 {response['backup_node']} 将接管")
                
                # 检查备份节点是否成功提升
                if response.get('backup_promoted', False):
                    print(f"✅ 备份节点 {response['backup_node']} 已成功提升为主节点")
                else:
                    print(f"⚠️ 备份节点 {response['backup_node']} 提升过程可能未完成，但节点 {node_id} 仍被标记为故障")
            else:
                print(f"⚠️ 警告: 节点 {node_id} 没有备份节点，故障将导致服务不可用！")
            
            # 立即检查被模拟故障的节点状态，确认状态已改变
            print("\n立即检查节点状态，确认故障模拟是否生效：")
            self.check_node_status(node_id)
        else:
            print(f"故障模拟失败: {response.get('message')}")
            print("请检查节点是否存在或系统是否运行正常")
        return response
    
    def recover_node(self, node_id):
        """恢复故障节点"""
        request = {
            'command': 'recover_node',
            'node_id': node_id
        }
        print(f"正在恢复节点 {node_id}...")
        response = self.send_request(request)
        if response['status'] == 'success':
            print(f"节点 {node_id} 已恢复正常状态")
        else:
            print(f"节点恢复失败: {response.get('message')}")
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
            
            # 显示节点的状态字段
            if 'state' in response:
                print(f"节点状态标记: {response['state']}")
            
            # 显示完整节点信息
            if 'node_info' in response:
                print(f"节点完整信息: {response['node_info']}")
            
            # 特别强调故障状态
            if not response.get('is_active', True) or response.get('state') == 'failed':
                print(f"⚠️ 警告: 节点 {node_id} 当前处于故障状态！")
                if response.get('backup_node'):
                    backup_node = response['backup_node']
                    print(f"✅ 备份节点 {backup_node} 应该已接管其工作")
                    # 检查备份节点状态
                    self.check_node_status(backup_node)
                else:
                    print(f"❌ 该节点没有可用的备份节点！")
        else:
            print(f"状态检查失败: {response.get('message')}")
            print(f"⚠️ 节点 {node_id} 可能无法访问或不存在")
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
    
    def run_interactive_failure_recovery_demo(self):
        """运行交互式故障恢复演示"""
        print("\n=== 交互式故障恢复演示 ===")
        print("本演示将引导您完成节点故障和恢复的完整流程，体验系统的高可用性特性")
        
        # 创建客户端对象
        from src.client import BankClient
        client = BankClient(coordinator_host=self.host, coordinator_port=self.port)
        
        # 提示第1步：查看当前可用节点
        input("\n第1步：查看当前可用节点。按回车继续...")
        self.list_accounts()
        
        # 提示第2步：初始化账户
        input("\n第2步：初始化所有账户余额为10000。按回车继续...")
        response = client.initialize_accounts(10000)
        if response['status'] == 'success':
            print("所有账户已初始化余额为10000")
        else:
            print(f"初始化账户失败: {response.get('message')}")
            return
        
        # 提示第3步：查询余额
        input("\n第3步：查询初始余额。按回车继续...")
        for account in ['a1', 'a2']:
            response = client.get_balance(account)
            if response['status'] == 'success':
                print(f"账户 {account} 余额: {response.get('balance')}")
        
        # 提示第4步：模拟节点故障
        node_to_fail = input("\n第4步：请输入要模拟故障的节点ID（推荐：a1）: ").strip()
        if not node_to_fail:
            node_to_fail = 'a1'
        
        # 检查初始状态
        print("\n故障模拟前先检查节点状态:")
        self.check_node_status(node_to_fail)
        
        # 执行故障模拟
        failure_response = self.simulate_node_failure(node_to_fail)
        if failure_response['status'] == 'success':
            print(f"\n节点 {node_to_fail} 已被模拟为故障状态")
        else:
            print(f"\n故障模拟失败: {failure_response.get('message')}")
            return
        
        # 提示第5步：查看节点状态
        input("\n第5步：检查节点状态。按回车继续...")
        print("\n再次确认节点状态:")
        self.check_node_status(node_to_fail)
        backup_node = f"{node_to_fail}b"
        self.check_node_status(backup_node)
        
        # 提示第6步：故障节点余额查询
        input(f"\n第6步：尝试查询故障节点 {node_to_fail} 的余额（应自动重定向到备份节点）。按回车继续...")
        response = client.get_balance(node_to_fail)
        if response['status'] == 'success':
            print(f"账户 {node_to_fail} 余额: {response.get('balance')}")
            if response.get('used_backup'):
                print(f"(通过备份节点 {backup_node} 查询)")
        else:
            print(f"查询余额失败: {response.get('message')}")
        
        # 提示第7步：故障节点转账
        input(f"\n第7步：尝试从故障节点 {node_to_fail} 转账（应自动使用备份节点）。按回车继续...")
        transfer_amount = 500
        transfer_to = 'a2' if node_to_fail != 'a2' else 'a1'
        print(f"执行转账: {node_to_fail} -> {transfer_to} ({transfer_amount})...")
        response = client.transfer(node_to_fail, transfer_to, transfer_amount)
        if response['status'] == 'success':
            print(f"转账成功! {response.get('message', '')}")
            if response.get('used_backup'):
                print("(通过备份节点完成)")
        else:
            print(f"转账失败: {response.get('message')}")
        
        # 提示第8步：检查余额变化
        input("\n第8步：检查转账后余额。按回车继续...")
        for account in ['a1', 'a2']:
            response = client.get_balance(account)
            if response['status'] == 'success':
                print(f"账户 {account} 余额: {response.get('balance')}")
                if response.get('used_backup'):
                    print("(通过备份节点查询)")
        
        # 提示第9步：恢复故障节点
        input(f"\n第9步：恢复故障节点 {node_to_fail}。按回车继续...")
        self.recover_node(node_to_fail)
        
        # 提示第10步：检查恢复后的节点状态
        input("\n第10步：检查恢复后的节点状态。按回车继续...")
        print("\n确认节点已恢复:")
        self.check_node_status(node_to_fail)
        
        # 提示第11步：恢复后查询余额
        input(f"\n第11步：恢复后查询 {node_to_fail} 余额。按回车继续...")
        response = client.get_balance(node_to_fail)
        if response['status'] == 'success':
            print(f"账户 {node_to_fail} 余额: {response.get('balance')}")
            if response.get('used_backup'):
                print("(通过备份节点查询)")
        else:
            print(f"查询余额失败: {response.get('message')}")
        
        # 提示第12步：恢复后执行转账
        input(f"\n第12步：恢复后执行另一笔转账。按回车继续...")
        transfer_amount = 300
        print(f"执行转账: {node_to_fail} -> {transfer_to} ({transfer_amount})...")
        response = client.transfer(node_to_fail, transfer_to, transfer_amount)
        if response['status'] == 'success':
            print(f"转账成功!")
        else:
            print(f"转账失败: {response.get('message')}")
        
        # 提示第13步：最终检查余额
        input("\n第13步：最终检查所有账户余额。按回车继续...")
        for account in ['a1', 'a2']:
            response = client.get_balance(account)
            print(f"账户 {account} 余额: {response.get('balance', '未知')}")
        
        print("\n=== 交互式故障恢复演示完成 ===")
        print("您已成功体验了：")
        print("1. 节点故障模拟与检测")
        print("2. 故障节点的自动重定向（查询和转账）")
        print("3. 节点恢复过程")
        print("4. 恢复后的正常功能")
        print("\n整个过程展示了系统的高可用性和容错能力")
    
    def run_concurrent_transfers_demo(self):
        """运行并发转账演示"""
        print("\n=== 并发转账演示 ===")

        
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
    print("1. 交互式故障恢复演示")
    print("2. 并发转账演示")
    print("3. 检查节点状态")
    print("4. 模拟节点故障")
    print("5. 恢复故障节点")
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
                demo.run_interactive_failure_recovery_demo()
            elif choice == '2':
                demo.run_concurrent_transfers_demo()
            elif choice == '3':
                node_id = input("请输入要检查的节点ID: ").strip()
                demo.check_node_status(node_id)
            elif choice == '4':
                node_id = input("请输入要模拟故障的节点ID: ").strip()
                demo.simulate_node_failure(node_id)
            elif choice == '5':
                node_id = input("请输入要恢复的节点ID: ").strip()
                demo.recover_node(node_id)
            else:
                print("无效选择，请重试")
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"发生错误: {e}")
    
    print("演示结束")

if __name__ == "__main__":
    main() 