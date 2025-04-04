
import time
import socket
import json
import threading
import sys
import os

# 确保可以导入客户端模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.client import BankClient

class FailureTest:
    """针对节点故障的测试类"""
    
    def __init__(self, coordinator_host='localhost', coordinator_port=6010):
        """初始化测试环境"""
        self.coordinator_host = coordinator_host
        self.coordinator_port = coordinator_port
        
        # 连接到协调器
        print(f"Connecting to coordinator at {coordinator_host}:{coordinator_port}...")
        try:
            # 创建客户端实例
            self.client = BankClient(coordinator_host, coordinator_port)
            print("Connection established, sending request...")
        except ConnectionRefusedError:
            print(f"Error: Connection to {coordinator_host}:{coordinator_port} refused")
            print("Make sure the coordinator is running and the port is correct.")
            raise
    
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
    
    def initialize_accounts(self, initial_balance):
        """初始化所有账户的余额"""
        print(f"初始化所有账户余额为 {initial_balance}...")
        request = {
            'command': 'init_accounts',
            'amount': initial_balance
        }
        
        response = self.client.send_request(request)
        
        if response['status'] == 'success':
            print(f"成功: 所有账户已初始化为 {initial_balance}")
            return True
        else:
            print(f"失败: 无法初始化账户余额 - {response.get('message', '未知错误')}")
            return False
    
    def perform_transfer(self, from_account, to_account, amount):
        """执行单个转账操作"""
        print(f"转账 {amount} 从 {from_account} 到 {to_account}...")
        result = self.client.transfer(from_account, to_account, amount)
        if result['status'] == 'success':
            print(f"成功: 从 {from_account} 转账 {amount} 到 {to_account}")
            return True
        else:
            print(f"失败: 转账操作失败 - {result.get('message', '未知错误')}")
            return False
    
    def simulate_node_failure(self, node_id):
        """模拟节点故障"""
        try:
            print(f"\n模拟节点 {node_id} 故障...")
            # 向协调器发送请求，模拟节点故障
            request = {
                'command': 'simulate_failure',
                'node_id': node_id
            }
            response = self.client.send_request(request)
            
            if response['status'] == 'success':
                print(f"成功: 节点 {node_id} 已标记为故障")
                return True
            else:
                print(f"失败: 无法模拟节点故障 - {response.get('message', '未知错误')}")
                return False
        except Exception as e:
            print(f"错误: 模拟节点故障时出现异常 - {e}")
            return False
    
    def test_node_recovery(self, primary_node_id):
        """测试备份节点是否成功接管"""
        print(f"\n测试备份节点是否成功接管 {primary_node_id}...")
        # 向协调器发送请求，检查节点状态
        request = {
            'command': 'check_node_status',
            'node_id': primary_node_id
        }
        
        try:
            response = self.client.send_request(request)
            
            if response['status'] == 'success':
                backup_node = response.get('backup_node')
                is_active = response.get('is_active', False)
                
                if not is_active and backup_node:
                    print(f"主节点 {primary_node_id} 当前不活跃")
                    print(f"备份节点 {backup_node} 应已接管")
                    
                    # 尝试向备份节点发送请求
                    balance_response = self.client.get_balance(backup_node)
                    if balance_response['status'] == 'success':
                        print(f"成功: 备份节点 {backup_node} 已接管，当前余额: {balance_response.get('balance', 0)}")
                        return True
                    else:
                        print(f"失败: 无法从备份节点获取余额")
                        return False
                else:
                    print(f"节点 {primary_node_id} 仍然活跃或没有备份节点")
                    return False
            else:
                print(f"失败: 无法检查节点状态 - {response.get('message', '未知错误')}")
                return False
        
        except Exception as e:
            print(f"错误: 测试节点恢复时出现异常 - {e}")
            return False
    
    def test_transfer_during_failure(self, from_account, to_account, amount, fail_node=None):
        """测试在转账过程中节点故障的情况"""
        print(f"\n测试在转账过程中节点故障: 从 {from_account} 到 {to_account}, 金额 {amount}")
        print(f"将在转账中模拟 {fail_node} 节点故障")
        
        # 首先获取交易前的账户余额
        from_balance_before = None
        to_balance_before = None
        
        from_response = self.client.get_balance(from_account)
        if from_response['status'] == 'success':
            from_balance_before = from_response.get('balance', 0)
            print(f"转账前 {from_account} 余额: {from_balance_before}")
        
        to_response = self.client.get_balance(to_account)
        if to_response['status'] == 'success':
            to_balance_before = to_response.get('balance', 0)
            print(f"转账前 {to_account} 余额: {to_balance_before}")
        
        # 开始转账，并模拟节点故障
        transfer_thread = threading.Thread(target=self.perform_transfer, args=(from_account, to_account, amount))
        transfer_thread.start()
        
        # 短暂延迟，确保转账已经开始
        time.sleep(0.5)
        
        # 模拟节点故障
        if fail_node:
            self.simulate_node_failure(fail_node)
        
        # 等待转账完成或超时
        transfer_thread.join(timeout=10)
        
        # 检查转账后的账户余额
        time.sleep(2)  # 等待系统处理
        print("\n转账后余额验证:")
        
        from_balance_after = None
        to_balance_after = None
        
        from_response = self.client.get_balance(from_account)
        if from_response['status'] == 'success':
            from_balance_after = from_response.get('balance', 0)
            print(f"转账后 {from_account} 余额: {from_balance_after}")
        else:
            print(f"无法获取账户 {from_account} 的余额: {from_response.get('message', '未知错误')}")
        
        to_response = self.client.get_balance(to_account)
        if to_response['status'] == 'success':
            to_balance_after = to_response.get('balance', 0)
            print(f"转账后 {to_account} 余额: {to_balance_after}")
        else:
            print(f"无法获取账户 {to_account} 的余额: {to_response.get('message', '未知错误')}")
        
        # 检查交易是否一致（要么完成，要么都未变化）
        if from_balance_before is not None and to_balance_before is not None and \
           from_balance_after is not None and to_balance_after is not None:
            
            from_diff = from_balance_before - from_balance_after
            to_diff = to_balance_after - to_balance_before
            
            if abs(from_diff - amount) < 0.01 and abs(to_diff - amount) < 0.01:
                print("交易成功完成，余额变化符合预期")
                return True, "完成"
            elif abs(from_diff) < 0.01 and abs(to_diff) < 0.01:
                print("交易被中止，余额未发生变化")
                return True, "中止"
            else:
                print("警告: 余额变化不一致，可能存在数据不一致问题")
                return False, "不一致"
        else:
            print("无法验证交易结果，无法获取完整的余额信息")
            return False, "未知"


def main(test_case=0):
    """主函数，运行故障测试案例"""
    try:
        tester = FailureTest(coordinator_port=6010)
    except Exception as e:
        print(f"错误: {e}")
        return
    
    # 确定要使用的测试账户
    account1 = "a1"
    account2 = "a2"
    print(f"使用账户 {account1} 和 {account2} 进行测试")
    
    # 测试案例1: 模拟账户节点在转账过程中失败
    if test_case == 0 or test_case == 1:
        print("\n=== 测试案例 1: 账户节点在转账过程中失败 ===")
        # 重新初始化
        if not tester.initialize_accounts(1000):
            print("无法重置账户余额，测试终止")
            return
        time.sleep(1)
        
        # 测试发送方账户节点失败
        print("\n1.1 测试发送方账户节点失败")
        tester.test_transfer_during_failure(account1, account2, 200, fail_node=account1)
        
        # 重新初始化
        if not tester.initialize_accounts(1000):
            print("无法重置账户余额，测试终止")
            return
        time.sleep(1)
        
        # 测试接收方账户节点失败
        print("\n1.2 测试接收方账户节点失败")
        tester.test_transfer_during_failure(account1, account2, 200, fail_node=account2)
    
    # 测试案例2: 测试节点恢复和备份接管
    if test_case == 0 or test_case == 2:
        print("\n=== 测试案例 2: 测试节点恢复和备份接管 ===")
        
        # 重新初始化
        if not tester.initialize_accounts(1000):
            print("无法重置账户余额，测试终止")
            return
        time.sleep(1)
        
        # 先检查一下账户余额
        print("\n首先查看当前账户余额:")
        tester.check_final_balances([account1, account2])
        
        # 模拟主节点失败
        primary_node = account1  # 使用第一个账户作为测试目标
        print(f"\n2.1 测试主节点 {primary_node} 失败后的备份接管")
        if tester.simulate_node_failure(primary_node):
            # 等待系统处理故障
            print("等待系统检测到节点故障并进行恢复...")
            time.sleep(5)  # 给系统一些时间来检测故障并进行恢复
            
            # 测试备份节点是否成功接管
            tester.test_node_recovery(primary_node)
            
            # 尝试向已故障的主节点发起转账，看系统是否能够正确路由到备份节点
            print("\n尝试向已故障节点的账户发起转账，测试系统是否能够路由到备份节点")
            result, status = tester.test_transfer_during_failure(account2, primary_node, 150)
            
            # 再次检查账户余额
            print("\n操作后检查余额:")
            tester.check_final_balances([account1, f"{account1}b", account2])
    
    print("\n所有故障测试案例完成")


if __name__ == "__main__":
    # 如果命令行有参数，则运行特定测试案例
    if len(sys.argv) > 1:
        try:
            test_case = int(sys.argv[1])
            main(test_case)
        except ValueError:
            print("错误: 测试案例参数必须是整数")
    else:
        # 否则运行所有测试案例
        main(0)
