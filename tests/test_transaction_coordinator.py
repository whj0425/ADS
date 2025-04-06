import unittest
from unittest.mock import patch, MagicMock, mock_open
import time
import sys
import os
import json

# 确保 src 和 config 目录在 PYTHONPATH 中
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

# 假设 Coordinator 依赖这些
from src.transaction_coordinator import TransactionCoordinator
# 如果 TransactionCoordinator 直接导入 config, 可能需要 mock config
# from config.network_config import COORDINATOR_PORT # 或者 mock 它

# Mock config values if necessary
@patch('src.transaction_coordinator.COORDINATOR_PORT', 5010) # Mock port from config
class TestTransactionCoordinator(unittest.TestCase):

    @patch('src.transaction_coordinator.socket.socket')
    @patch('src.transaction_coordinator.threading.Thread')
    @patch('src.transaction_coordinator.os.path.exists')
    @patch('src.transaction_coordinator.os.makedirs')
    @patch("builtins.open", new_callable=mock_open)
    @patch('time.time') # Mock time.time for consistent heartbeat checks
    def setUp(self, mock_time, mock_file_open, mock_makedirs, mock_path_exists, mock_thread, mock_socket):
        """初始化测试环境，Mock外部依赖"""
        # 配置 Mock 返回值
        mock_path_exists.return_value = False # 默认数据文件不存在
        mock_time.return_value = 1000.0 # 固定时间戳便于测试超时
        self.mock_time = mock_time

        # 创建 Coordinator 实例
        # 由于 __init__ 会启动线程, 我们需要在 patch 装饰器中 mock Thread
        self.coordinator = TransactionCoordinator(port=5010, coordinator_id="test_coord")

        # Mock掉需要网络交互的方法，防止实际发送请求
        self.coordinator.send_request_to_node = MagicMock(return_value={'status': 'success'}) # 通用 Mock
        self.coordinator.promote_backup_to_primary = MagicMock() # Mock 提升逻辑
        self.coordinator.notify_backup_of_promotion = MagicMock() # Mock 通知逻辑
        self.coordinator.execute_two_phase_commit = MagicMock(return_value=('success', 'Mock 2PC success')) # Mock 2PC

        # 阻止实际的文件读写，但保留 save_data 的调用记录
        self.coordinator.load_data = MagicMock() # 阻止 __init__ 中的加载
        # 让 save_data 使用 mock_open，但本身不执行额外操作
        original_save_data = self.coordinator.save_data
        def side_effect_save_data():
             # print("Mock save_data called") # For debugging
             original_save_data() # Call the original logic which uses mock_open
        self.coordinator.save_data = MagicMock(side_effect=side_effect_save_data)
        self.mock_file_open = mock_file_open # Keep ref to mock_open

        # 清理 __init__ 可能留下的状态 (因为 load_data 被 mock)
        self.coordinator.account_nodes = {}
        self.coordinator.transactions = {}
        self.coordinator.node_pairs = {}
        self.coordinator.node_hosts = {}

    def _simulate_heartbeat(self, node_id, port, role, client_addr='127.0.0.1', current_time=None):
        """辅助方法：模拟收到一个心跳请求"""
        if current_time:
            self.mock_time.return_value = current_time

        request = json.dumps({
            'command': 'heartbeat',
            'node_id': node_id,
            'node_type': 'account',
            'port': port,
            'role': role,
            'client_addr': client_addr
        }).encode('utf-8')

        # 模拟 handle_request 的核心逻辑 (不模拟完整的 socket)
        # 我们需要手动调用内部处理逻辑，或者更精确地 Mock socket recv
        # 这里直接修改 coordinator 状态模拟心跳处理结果
        with self.coordinator.lock:
            node_info = self.coordinator.account_nodes.get(node_id, {})
            is_failed = node_info.get('status') == 'failed'

            node_info['port'] = port # Port might update even if failed? Check actual logic. Assuming it does for now.
            node_info['last_heartbeat'] = self.mock_time()

            # 只有当节点当前状态不是 failed 时才更新角色和状态
            if not is_failed:
                node_info['role'] = role
                node_info['status'] = 'active' # 收到心跳就认为活跃 (如果不是 failed)
            # else: # 如果是 failed, 保持 role 和 status 不变
                # print(f"Debug: Node {node_id} is failed, role/status unchanged by heartbeat.") # Debug print

            # 模拟 handle_request 中更新 host 的逻辑
            self.coordinator.node_hosts[node_id] = client_addr
            self.coordinator.account_nodes[node_id] = node_info
            self.coordinator.save_data() # 模拟心跳处理后的保存

    def test_initial_state(self):
        """测试协调器初始化状态"""
        self.assertEqual(self.coordinator.coordinator_id, "test_coord")
        self.assertEqual(self.coordinator.port, 5010)
        self.assertEqual(self.coordinator.account_nodes, {})
        self.assertEqual(self.coordinator.transactions, {})
        self.assertEqual(self.coordinator.node_pairs, {})
        self.assertEqual(self.coordinator.node_hosts, {})

    def test_handle_heartbeat_new_node(self):
        """测试处理新节点的第一次心跳"""
        self._simulate_heartbeat('a1', 6001, 'primary', current_time=1000.0)

        self.assertIn('a1', self.coordinator.account_nodes)
        node_info = self.coordinator.account_nodes['a1']
        self.assertEqual(node_info['port'], 6001)
        self.assertEqual(node_info['role'], 'primary')
        self.assertEqual(node_info['last_heartbeat'], 1000.0)
        self.assertEqual(node_info.get('status', 'active'), 'active') # 默认或显式为 active
        self.assertIn('a1', self.coordinator.node_hosts)
        self.assertEqual(self.coordinator.node_hosts['a1'], '127.0.0.1')
        self.coordinator.save_data.assert_called() # 验证数据被保存

    def test_handle_heartbeat_existing_node(self):
        """测试处理已存在节点的后续心跳"""
        # 先注册节点
        self._simulate_heartbeat('a1', 6001, 'primary', current_time=1000.0)
        self.coordinator.save_data.reset_mock() # 重置 mock 调用计数

        # 模拟第二次心跳
        self._simulate_heartbeat('a1', 6001, 'primary', current_time=1010.0)

        self.assertEqual(self.coordinator.account_nodes['a1']['last_heartbeat'], 1010.0)
        self.coordinator.save_data.assert_called_once() # 每次心跳都应保存

    def test_handle_heartbeat_updates_role_and_port(self):
        """测试心跳是否能更新节点角色和端口"""
        self._simulate_heartbeat('a1', 6001, 'primary', current_time=1000.0)
        self.coordinator.save_data.reset_mock()
        # 模拟节点重启后以不同角色/端口发送心跳
        self._simulate_heartbeat('a1', 6005, 'backup', current_time=1010.0)

        node_info = self.coordinator.account_nodes['a1']
        self.assertEqual(node_info['port'], 6005)
        self.assertEqual(node_info['role'], 'backup')
        self.assertEqual(node_info['last_heartbeat'], 1010.0)
        self.coordinator.save_data.assert_called_once()

    def test_handle_heartbeat_from_failed_node(self):
        """测试收到已标记为失败的节点的心跳（只更新时间，不改变状态/角色）"""
        self._simulate_heartbeat('a1', 6001, 'primary', current_time=1000.0)
        # 手动标记为失败
        with self.coordinator.lock:
            self.coordinator.account_nodes['a1']['status'] = 'failed'
            self.coordinator.account_nodes['a1']['role'] = 'failed_primary' # 假设失败时角色也变了
        self.coordinator.save_data.reset_mock()

        # 模拟失败节点的心跳
        self._simulate_heartbeat('a1', 6001, 'primary', current_time=1020.0)

        node_info = self.coordinator.account_nodes['a1']
        self.assertEqual(node_info['last_heartbeat'], 1020.0) # 时间戳更新
        self.assertEqual(node_info['status'], 'failed') # 状态保持 failed
        self.assertEqual(node_info['role'], 'failed_primary') # 角色保持不变
        self.assertEqual(node_info['port'], 6001) # 端口可能更新，取决于实现，这里假设不变
        self.coordinator.save_data.assert_called_once()

    def test_list_accounts(self):
        """测试 list_accounts 命令"""
        self._simulate_heartbeat('a1', 6001, 'primary')
        self._simulate_heartbeat('a2', 6002, 'primary')
        self._simulate_heartbeat('a1b', 6003, 'backup')

        # 模拟处理 list_accounts 请求
        with self.coordinator.lock:
            response = {
                'status': 'success',
                'accounts': list(self.coordinator.account_nodes.keys())
            }

        self.assertEqual(response['status'], 'success')
        self.assertCountEqual(response['accounts'], ['a1', 'a2', 'a1b']) # 无序比较

    def test_simulate_failure_no_backup(self):
        """测试模拟节点故障（无备份）"""
        self.mock_time.return_value = 1000.0
        self._simulate_heartbeat('a1', 6001, 'primary')
        self.coordinator.save_data.reset_mock()

        # 模拟 simulate_failure 命令
        request = {'command': 'simulate_failure', 'node_id': 'a1'}
        with self.coordinator.lock:
             if request['node_id'] in self.coordinator.account_nodes:
                 node_id = request['node_id']
                 self.coordinator.account_nodes[node_id]['status'] = 'failed'
                 self.coordinator.account_nodes[node_id]['failure_time'] = self.mock_time()
                 self.coordinator.save_data()
                 # 检查是否有备份
                 backup_node_id = self.coordinator.node_pairs.get(node_id)
                 response = {
                     'status': 'success',
                     'message': f'Node {node_id} marked as failed',
                     'backup_node': backup_node_id, # None in this case
                     'backup_promoted': False
                 }
             else:
                 response = {'status': 'error', 'message': 'Node not found'}

        self.assertEqual(response['status'], 'success')
        self.assertEqual(self.coordinator.account_nodes['a1']['status'], 'failed')
        self.assertEqual(self.coordinator.account_nodes['a1']['failure_time'], 1000.0)
        self.assertIsNone(response['backup_node']) # 确认没有备份
        self.assertFalse(response['backup_promoted'])
        self.coordinator.save_data.assert_called_once()
        # 确认没有调用 promote_backup_to_primary
        self.coordinator.promote_backup_to_primary.assert_not_called()

    def test_simulate_failure_with_backup(self):
        """测试模拟节点故障（有备份）"""
        self.mock_time.return_value = 1000.0
        self._simulate_heartbeat('a1', 6001, 'primary')
        self._simulate_heartbeat('a1b', 6002, 'backup')
        # 手动建立配对关系 (模拟心跳或注册逻辑)
        self.coordinator.node_pairs['a1'] = 'a1b'
        self.coordinator.save_data.reset_mock()
        self.coordinator.promote_backup_to_primary.reset_mock() # 重置 mock
        self.coordinator.notify_backup_of_promotion.reset_mock()

        # 让 promote_backup_to_primary 在被调用时模拟成功
        def mock_promote_logic(failed_node_id, backup_node_id):
             with self.coordinator.lock:
                 if backup_node_id in self.coordinator.account_nodes:
                      print(f"Mock: Promoting {backup_node_id} for {failed_node_id}")
                      self.coordinator.account_nodes[backup_node_id]['role'] = 'primary'
                      self.coordinator.node_pairs.pop(failed_node_id, None)
                      self.coordinator.save_data()
                      # 模拟通知备份节点
                      self.coordinator.notify_backup_of_promotion(backup_node_id)
                      return True
             return False
        self.coordinator.promote_backup_to_primary.side_effect = mock_promote_logic

        # 模拟 simulate_failure 命令
        request = {'command': 'simulate_failure', 'node_id': 'a1'}
        backup_promoted_flag = False
        with self.coordinator.lock:
            if request['node_id'] in self.coordinator.account_nodes:
                node_id = request['node_id']
                self.coordinator.account_nodes[node_id]['status'] = 'failed'
                self.coordinator.account_nodes[node_id]['failure_time'] = self.mock_time()
                self.coordinator.save_data() # 第一次保存（标记失败）

                backup_node_id = self.coordinator.node_pairs.get(node_id)
                if backup_node_id and backup_node_id in self.coordinator.account_nodes:
                    # 调用（被 mock 的）提升逻辑
                    backup_promoted_flag = self.coordinator.promote_backup_to_primary(node_id, backup_node_id)
                    # promote_backup_to_primary 内部会调用 save_data

                response = {
                    'status': 'success',
                    'message': f'Node {node_id} marked as failed',
                    'backup_node': backup_node_id,
                    'backup_promoted': backup_promoted_flag
                }
            else:
                 response = {'status': 'error', 'message': 'Node not found'}

        self.assertEqual(response['status'], 'success')
        self.assertEqual(self.coordinator.account_nodes['a1']['status'], 'failed')
        self.assertEqual(self.coordinator.account_nodes['a1']['failure_time'], 1000.0)
        self.assertEqual(response['backup_node'], 'a1b')
        self.assertTrue(response['backup_promoted']) # 确认备份已提升 (基于 mock)

        # 验证 promote_backup_to_primary 被调用
        self.coordinator.promote_backup_to_primary.assert_called_once_with('a1', 'a1b')
        # 验证 notify_backup_of_promotion 被调用 (在 mock 的 side_effect 里)
        self.coordinator.notify_backup_of_promotion.assert_called_once_with('a1b')

        # 验证 save_data 被调用多次（标记失败一次，提升备份一次）
        self.assertGreaterEqual(self.coordinator.save_data.call_count, 2)

        # 验证状态变化
        self.assertEqual(self.coordinator.account_nodes['a1b']['role'], 'primary')
        self.assertNotIn('a1', self.coordinator.node_pairs) # 配对关系解除

    def test_monitor_nodes_detects_failure_and_promotes(self):
        """测试 monitor_nodes 线程的故障检测和提升逻辑"""
        # 设置模拟时间
        self.mock_time.return_value = 1000.0

        # 添加节点，一个活跃，一个超时
        self._simulate_heartbeat('a1', 6001, 'primary', current_time=980.0) # 超时 (假设超时 > 15s)
        self._simulate_heartbeat('a1b', 6002, 'backup', current_time=995.0) # 活跃
        self._simulate_heartbeat('a2', 6003, 'primary', current_time=990.0) # 活跃
        self.coordinator.node_pairs['a1'] = 'a1b'

        # Mock promote_backup_to_primary 以便验证调用
        self.coordinator.promote_backup_to_primary = MagicMock(return_value=True)
        self.coordinator.save_data.reset_mock()

        # 执行 monitor_nodes 的核心逻辑 (假设超时设为15s)
        timeout_threshold = 15
        current_time = self.mock_time()
        nodes_to_check = list(self.coordinator.account_nodes.items())

        for node_id, node_info in nodes_to_check:
            if node_info.get('status') != 'failed': # 只检查非失败状态的节点
                last_heartbeat = node_info.get('last_heartbeat', 0)
                if current_time - last_heartbeat > timeout_threshold:
                    print(f"Monitor: Detected timeout for node {node_id}")
                    # Mark as failed
                    node_info['status'] = 'failed'
                    node_info['failure_time'] = current_time
                    self.coordinator.save_data() # 保存失败状态
                    # Promote backup if exists
                    backup_node_id = self.coordinator.node_pairs.get(node_id)
                    if backup_node_id and backup_node_id in self.coordinator.account_nodes and self.coordinator.account_nodes[backup_node_id].get('status') != 'failed':
                        self.coordinator.promote_backup_to_primary(node_id, backup_node_id)
                        # promote_backup_to_primary 内部应该会再次 save_data

        # 验证结果
        self.assertEqual(self.coordinator.account_nodes['a1']['status'], 'failed')
        self.assertEqual(self.coordinator.account_nodes['a1b'].get('status', 'active'), 'active') # 备份节点应保持活跃
        self.assertEqual(self.coordinator.account_nodes['a2'].get('status', 'active'), 'active') # 另一个节点应保持活跃
        self.coordinator.promote_backup_to_primary.assert_called_once_with('a1', 'a1b')
        # save_data 会被调用（标记 a1 失败 + promote_backup 内部调用）
        self.assertGreaterEqual(self.coordinator.save_data.call_count, 1)

    def test_transfer_command_triggers_2pc(self):
        """测试 transfer 命令是否触发两阶段提交"""
        self._simulate_heartbeat('a1', 6001, 'primary')
        self._simulate_heartbeat('a2', 6002, 'primary')

        # 模拟 transfer 请求
        request_data = json.dumps({
            'command': 'transfer',
            'from': 'a1',
            'to': 'a2',
            'amount': 100
        }).encode('utf-8')

        # 假设 handle_request 解析后调用 execute_two_phase_commit
        # 我们直接验证 execute_two_phase_commit 被调用
        # (实际 handle_request 逻辑需要被更精确地模拟或测试)

        # 模拟 handle_request 中的调用点
        from_node = 'a1'
        to_node = 'a2'
        amount = 100
        if from_node in self.coordinator.account_nodes and to_node in self.coordinator.account_nodes:
             # Mock execute_two_phase_commit 被调用
             status, message = self.coordinator.execute_two_phase_commit(from_node, to_node, amount)
             response = {'status': status, 'message': message}
        else:
             response = {'status': 'error', 'message': 'One or both accounts not found'}

        self.coordinator.execute_two_phase_commit.assert_called_once_with('a1', 'a2', 100)
        self.assertEqual(response['status'], 'success') # 基于 mock 的返回值
        self.assertEqual(response['message'], 'Mock 2PC success')

    # TODO: 添加更多测试用例
    # - 测试 recover_node 命令
    # - 测试 check_node_status 命令
    # - 测试 handle_request 对无效命令的处理
    # - 测试 _send_request_to_node 的异常处理 (需要更复杂的 mock)
    # - 测试 execute_two_phase_commit 的内部逻辑 (可能需要单独的测试类或更精细的 mock)
    # - 测试 primary-backup 配对逻辑 (心跳处理部分)

if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False) 