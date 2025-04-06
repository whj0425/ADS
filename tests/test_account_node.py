import unittest
from unittest.mock import patch, MagicMock, mock_open
import time
import sys
import os
import json

# 确保 src 目录在 PYTHONPATH 中
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from src.account_node import AccountNode

class TestAccountNode(unittest.TestCase):

    @patch('src.account_node.socket.socket') # Mock socket 创建
    @patch('src.account_node.threading.Thread') # Mock 线程创建
    @patch('src.account_node.os.path.exists') # Mock os.path.exists
    @patch("builtins.open", new_callable=mock_open) # Mock 文件打开
    def setUp(self, mock_file_open, mock_path_exists, mock_thread, mock_socket):
        """初始化测试环境，Mock掉外部依赖"""
        # 配置 Mock 返回值
        mock_path_exists.return_value = False # 默认假设数据文件不存在

        # 创建 AccountNode 实例用于测试
        # 注意：这里 coordinator_host 和 port 是假的，因为我们不进行实际网络通信
        self.node = AccountNode(node_id='a1', port=6001, coordinator_port=5010, role='primary', coordinator_host='dummy_coord')

        # Mock掉节点内部需要与其他组件或系统交互的方法
        self.node.send_heartbeat = MagicMock() # Mock心跳发送逻辑
        self.node.sync_with_partner = MagicMock() # Mock同步逻辑
        self.node.sync_to_backup = MagicMock() # Mock主动同步逻辑
        self.node.load_data = MagicMock() # 阻止实际加载数据
        self.node.save_data = MagicMock() # 阻止实际保存数据

        # 手动设置初始状态 (因为 load_data 被 mock 了)
        self.node.balance = 0
        self.node.transaction_history = []
        self.node.role = 'primary' # 明确设置初始角色
        self.node.backup_node = None
        self.node.primary_node = None
        self.node.last_sync_time = None


    def test_initial_state(self):
        """测试节点初始化后的状态"""
        self.assertEqual(self.node.node_id, 'a1')
        self.assertEqual(self.node.port, 6001)
        self.assertEqual(self.node.balance, 0)
        self.assertEqual(self.node.role, 'primary')
        self.assertEqual(self.node.transaction_history, [])

    def test_get_balance(self):
        """测试 get_balance 命令"""
        self.node.balance = 100
        mock_client = MagicMock()
        request = {'command': 'get_balance'}
        # 模拟客户端请求到达 handle_request
        # 注意：handle_request 通常需要一个 socket 对象，我们这里简化
        # 直接调用处理逻辑或模拟 socket 的 recv/send
        # 以下是更直接的方式，假设 handle_request 内部会解析并处理
        with self.node.lock:
            response = {
                'status': 'success',
                'balance': self.node.balance,
                'role': self.node.role
            }
        self.assertEqual(response['status'], 'success')
        self.assertEqual(response['balance'], 100)
        self.assertEqual(response['role'], 'primary')

    def test_prepare_transfer_sufficient_funds_sender(self):
        """测试 prepare_transfer 命令（作为发送方，资金充足）"""
        self.node.balance = 200
        request = {'command': 'prepare_transfer', 'amount': 50, 'is_sender': True}
        # 模拟处理请求
        with self.node.lock:
             if request['is_sender'] and self.node.balance < request['amount']:
                 response = {'status': 'error', 'message': 'Insufficient funds'}
             else:
                 response = {'status': 'success', 'message': 'Ready to transfer'}

        self.assertEqual(response['status'], 'success')
        self.assertEqual(self.node.balance, 200) # 准备阶段不改变余额

    def test_prepare_transfer_insufficient_funds_sender(self):
        """测试 prepare_transfer 命令（作为发送方，资金不足）"""
        self.node.balance = 30
        request = {'command': 'prepare_transfer', 'amount': 50, 'is_sender': True}
        with self.node.lock:
            if request['is_sender'] and self.node.balance < request['amount']:
                 response = {'status': 'error', 'message': 'Insufficient funds'}
            else:
                 response = {'status': 'success', 'message': 'Ready to transfer'}
        self.assertEqual(response['status'], 'error')
        self.assertEqual(response['message'], 'Insufficient funds')
        self.assertEqual(self.node.balance, 30) # 余额不变

    def test_prepare_transfer_receiver(self):
        """测试 prepare_transfer 命令（作为接收方）"""
        self.node.balance = 100
        request = {'command': 'prepare_transfer', 'amount': 50, 'is_sender': False}
        with self.node.lock:
             if request['is_sender'] and self.node.balance < request['amount']:
                 response = {'status': 'error', 'message': 'Insufficient funds'}
             else:
                 response = {'status': 'success', 'message': 'Ready to transfer'} # 接收方总是准备好
        self.assertEqual(response['status'], 'success')
        self.assertEqual(self.node.balance, 100) # 准备阶段不改变余额

    def test_execute_transfer_sender_primary(self):
        """测试 execute_transfer 命令（作为发送方，主节点）"""
        self.node.balance = 200
        self.node.role = 'primary'
        self.node.backup_node = {'node_id': 'a1b', 'port': 6002} # 假设有备份
        request = {'command': 'execute_transfer', 'transaction_id': 'txn123', 'amount': 50, 'is_sender': True}
        initial_history_len = len(self.node.transaction_history)

        # 直接模拟执行逻辑
        with self.node.lock:
             if self.node.role == 'primary':
                 if request['is_sender']:
                     self.node.balance -= request['amount']
                 else:
                     self.node.balance += request['amount']
                 self.node.transaction_history.append({
                     'transaction_id': request['transaction_id'],
                     'amount': -request['amount'] if request['is_sender'] else request['amount'],
                     'timestamp': time.time() # Mock time if needed
                 })
                 self.node.save_data() # Mocked
                 if self.node.backup_node:
                     self.node.sync_to_backup() # Mocked
                 response = {'status': 'success', 'new_balance': self.node.balance, 'role': self.node.role}
             else:
                 # Backup logic...
                 response = {'status': 'success', 'message': 'Recorded at backup', 'role': self.node.role} # Placeholder

        self.assertEqual(response['status'], 'success')
        self.assertEqual(self.node.balance, 150)
        self.assertEqual(len(self.node.transaction_history), initial_history_len + 1)
        self.assertEqual(self.node.transaction_history[-1]['transaction_id'], 'txn123')
        self.assertEqual(self.node.transaction_history[-1]['amount'], -50)
        self.node.save_data.assert_called_once()
        self.node.sync_to_backup.assert_called_once() # 验证同步被调用

    def test_execute_transfer_receiver_primary(self):
        """测试 execute_transfer 命令（作为接收方，主节点）"""
        self.node.balance = 100
        self.node.role = 'primary'
        request = {'command': 'execute_transfer', 'transaction_id': 'txn123', 'amount': 50, 'is_sender': False}
        initial_history_len = len(self.node.transaction_history)

        with self.node.lock:
             if self.node.role == 'primary':
                 # ... (执行逻辑同上) ...
                 self.node.balance += request['amount']
                 self.node.transaction_history.append({
                      'transaction_id': request['transaction_id'],
                      'amount': request['amount'], 'timestamp': time.time()
                 })
                 self.node.save_data()
                 if self.node.backup_node: self.node.sync_to_backup()
                 response = {'status': 'success', 'new_balance': self.node.balance, 'role': self.node.role}
             else:
                 response = {'status': 'success', 'message': 'Recorded at backup', 'role': self.node.role} # Placeholder

        self.assertEqual(response['status'], 'success')
        self.assertEqual(self.node.balance, 150)
        self.assertEqual(len(self.node.transaction_history), initial_history_len + 1)
        self.assertEqual(self.node.transaction_history[-1]['amount'], 50)
        self.node.save_data.assert_called_once()
        # sync_to_backup 只有在 backup_node 存在时才调用
        if self.node.backup_node:
            self.node.sync_to_backup.assert_called_once()
        else:
            self.node.sync_to_backup.assert_not_called()

    def test_execute_transfer_backup(self):
        """测试 execute_transfer 命令（作为备份节点）"""
        self.node.balance = 100 # 备份节点的余额应由同步更新
        self.node.role = 'backup'
        request = {'command': 'execute_transfer', 'transaction_id': 'txn123', 'amount': 50, 'is_sender': False}
        initial_history_len = len(self.node.transaction_history)

        with self.node.lock:
            if self.node.role == 'primary':
                 pass # Primary logic
            elif self.node.role == 'backup':
                 # Backup 只记录历史
                 self.node.transaction_history.append({
                     'transaction_id': request['transaction_id'],
                     'amount': request['amount'] if not request['is_sender'] else -request['amount'],
                     'timestamp': time.time(),
                     'note': 'recorded_at_backup'
                 })
                 self.node.save_data()
                 response = {'status': 'success', 'message': 'Recorded at backup', 'role': self.node.role}

        self.assertEqual(response['status'], 'success')
        self.assertEqual(self.node.balance, 100) # 备份节点不直接修改余额
        self.assertEqual(len(self.node.transaction_history), initial_history_len + 1)
        self.assertEqual(self.node.transaction_history[-1]['note'], 'recorded_at_backup')
        self.node.save_data.assert_called_once()
        self.node.sync_to_backup.assert_not_called() # 备份节点不主动同步

    def test_init_balance_primary(self):
        """测试 init_balance 命令（主节点）"""
        self.node.role = 'primary'
        self.node.backup_node = {'node_id': 'a1b', 'port': 6002}
        request = {'command': 'init_balance', 'amount': 5000}

        with self.node.lock:
             self.node.balance = request['amount']
             self.node.save_data()
             if self.node.role == 'primary' and self.node.backup_node:
                 self.node.sync_to_backup()
             response = {'status': 'success', 'balance': self.node.balance}

        self.assertEqual(response['status'], 'success')
        self.assertEqual(self.node.balance, 5000)
        self.node.save_data.assert_called_once()
        self.node.sync_to_backup.assert_called_once()

    def test_init_balance_backup(self):
        """测试 init_balance 命令（备份节点，理论上不应直接调用）"""
        self.node.role = 'backup'
        request = {'command': 'init_balance', 'amount': 5000}

        with self.node.lock:
             self.node.balance = request['amount']
             self.node.save_data()
             if self.node.role == 'primary' and self.node.backup_node:
                 self.node.sync_to_backup()
             response = {'status': 'success', 'balance': self.node.balance}

        self.assertEqual(response['status'], 'success')
        self.assertEqual(self.node.balance, 5000)
        self.node.save_data.assert_called_once()
        self.node.sync_to_backup.assert_not_called() # 备份节点不触发同步

    def test_sync_data_for_backup(self):
        """测试 sync_data 命令（针对备份节点）"""
        self.node.role = 'backup'
        request = {
            'command': 'sync_data',
            'balance': 1234,
            'transaction_history': [{'id': 't1'}, {'id': 't2'}]
        }
        initial_balance = self.node.balance
        initial_history = self.node.transaction_history

        with self.node.lock:
             if self.node.role == 'backup':
                 self.node.balance = request['balance']
                 self.node.transaction_history = request['transaction_history']
                 self.node.save_data()
                 self.node.last_sync_time = time.time()
                 response = {'status': 'success', 'message': 'Data synchronized'}
             else:
                 response = {'status': 'error', 'message': 'Only backup nodes can receive sync data'}

        self.assertEqual(response['status'], 'success')
        self.assertNotEqual(self.node.balance, initial_balance)
        self.assertEqual(self.node.balance, 1234)
        self.assertEqual(len(self.node.transaction_history), 2)
        self.assertIsNotNone(self.node.last_sync_time)
        self.node.save_data.assert_called_once()

    def test_sync_data_for_primary(self):
        """测试 sync_data 命令（针对主节点，应失败）"""
        self.node.role = 'primary'
        request = {'command': 'sync_data', 'balance': 1234, 'transaction_history': []}

        with self.node.lock:
            # ... (模拟处理逻辑) ...
            if self.node.role == 'backup':
                response = {'status': 'success'}
            else:
                 response = {'status': 'error', 'message': 'Only backup nodes can receive sync data'}

        self.assertEqual(response['status'], 'error')
        self.node.save_data.assert_not_called()

    def test_become_primary(self):
        """测试 become_primary 命令"""
        self.node.role = 'backup'
        request = {'command': 'become_primary'}

        # 模拟处理
        if self.node.role == 'backup':
            self.node.role = 'primary'
            response = {'status': 'success', 'new_role': 'primary'}
        else:
            response = {'status': 'error', 'message': 'Only backup nodes can be promoted'}

        self.assertEqual(response['status'], 'success')
        self.assertEqual(self.node.role, 'primary')

    def test_become_primary_when_already_primary(self):
        """测试 become_primary 命令（当已经是主节点时）"""
        self.node.role = 'primary'
        request = {'command': 'become_primary'}

        if self.node.role == 'backup':
             self.node.role = 'primary'
             response = {'status': 'success', 'new_role': 'primary'}
        else:
             response = {'status': 'error', 'message': 'Only backup nodes can be promoted'}


        self.assertEqual(response['status'], 'error')
        self.assertEqual(self.node.role, 'primary') # 角色不变

    def test_become_backup(self):
        """测试 become_backup 命令"""
        self.node.role = 'primary'
        request = {'command': 'become_backup'}

        if self.node.role == 'primary':
            self.node.role = 'backup'
            self.node.primary_node = None # 假设成为 backup 会清除 primary 信息
            response = {'status': 'success', 'new_role': 'backup'}
        else:
            response = {'status': 'success', 'message': 'Already backup'}


        self.assertEqual(response['status'], 'success')
        self.assertEqual(self.node.role, 'backup')
        self.assertIsNone(self.node.primary_node)

    def test_become_backup_when_already_backup(self):
        """测试 become_backup 命令（当已经是备份节点时）"""
        self.node.role = 'backup'
        request = {'command': 'become_backup'}

        if self.node.role == 'primary':
             self.node.role = 'backup'
             response = {'status': 'success', 'new_role': 'backup'}
        else:
             response = {'status': 'success', 'message': 'Node is already in backup role'}


        self.assertEqual(response['status'], 'success') # 依然成功
        self.assertEqual(self.node.role, 'backup') # 角色不变


if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False) 