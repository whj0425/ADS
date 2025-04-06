import unittest
import threading
import time
import socket
import json
import sys
import os
import shutil # 用于清理数据目录
from unittest.mock import patch

# 确保 src 和 config 目录在 PYTHONPATH 中
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from src.transaction_coordinator import TransactionCoordinator
from src.account_node import AccountNode
from src.client import BankClient # 可能需要用于触发操作

# 定义测试使用的端口，避免与实际运行冲突
TEST_COORDINATOR_PORT = 5100
TEST_NODE_A1_PORT = 6101
TEST_NODE_A1B_PORT = 6102
TEST_NODE_A2_PORT = 6103
TEST_DATA_DIR = os.path.join(root_dir, "data_test") # 使用单独的测试数据目录

class TestIntegration(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """在所有测试开始前，清理并创建测试数据目录"""
        if os.path.exists(TEST_DATA_DIR):
            shutil.rmtree(TEST_DATA_DIR)
        os.makedirs(TEST_DATA_DIR)
        # 重写 AccountNode 的数据文件路径，指向测试目录
        # (这通常通过环境变量或修改配置实现，这里用 patch 模拟)
        cls.account_node_data_patch = patch('src.account_node.AccountNode.data_file', property(lambda self: os.path.join(TEST_DATA_DIR, f"{self.node_id}_data.json")))

        cls.account_node_data_patch.start()

    @classmethod
    def tearDownClass(cls):
        """在所有测试结束后，清理测试数据目录"""
        cls.account_node_data_patch.stop()
        if os.path.exists(TEST_DATA_DIR):
            try:
                 #pass
                 shutil.rmtree(TEST_DATA_DIR) # 清理测试数据
            except OSError as e:
                 print(f"Error removing test data directory {TEST_DATA_DIR}: {e}")
                 pass # 忽略可能的错误


    def setUp(self):
        """每个测试开始前，启动协调器和节点"""
        self.coordinator = None
        self.node_a1 = None
        self.node_a1b = None
        self.node_a2 = None
        self.threads = []
        self.stop_event = threading.Event() # 用于优雅停止线程

        try:
            # 启动协调器
            # 在 AccountNode 的 __init__ 中打补丁，使其连接到测试协调器端口
            with patch('src.account_node.AccountNode.coordinator_port', TEST_COORDINATOR_PORT):
                # 假设 TransactionCoordinator 的 __init__ 接受 data_dir 参数
                self.coordinator = TransactionCoordinator(port=TEST_COORDINATOR_PORT, coordinator_id="int_test_coord", data_dir=TEST_DATA_DIR)
                # coordinator 的 server_thread 和 monitor_thread 是 daemon，理论上主线程结束它们会退出
                # 但为了更明确控制，我们可以在 tearDown 中尝试停止它们
                print(f"Coordinator {self.coordinator.coordinator_id} starting...")
                time.sleep(0.5) # 等待协调器启动并监听
                print(f"Coordinator {self.coordinator.coordinator_id} likely started.")

                # 启动节点 A1 (Primary)
                self.node_a1 = AccountNode(node_id='a1', port=TEST_NODE_A1_PORT, coordinator_port=TEST_COORDINATOR_PORT, role='primary')
                print(f"Node {self.node_a1.node_id} starting...")
                time.sleep(0.2)

                # 启动节点 A1b (Backup)
                self.node_a1b = AccountNode(node_id='a1b', port=TEST_NODE_A1B_PORT, coordinator_port=TEST_COORDINATOR_PORT, role='backup')
                print(f"Node {self.node_a1b.node_id} starting...")
                time.sleep(0.2)

                # 启动节点 A2 (Primary)
                self.node_a2 = AccountNode(node_id='a2', port=TEST_NODE_A2_PORT, coordinator_port=TEST_COORDINATOR_PORT, role='primary')
                print(f"Node {self.node_a2.node_id} starting...")
                time.sleep(1.5) # 给足够时间让所有节点发送心跳并被协调器记录

            # 创建 BankClient 用于交互 (可选)
            self.client = BankClient(coordinator_host='localhost', coordinator_port=TEST_COORDINATOR_PORT)
            print("Setup complete.")

        except Exception as e:
            print(f"Error during setUp: {e}")
            self.tearDown() # 出错时尝试清理
            raise

    def tearDown(self):
        """每个测试结束后，停止所有组件并清理"""
        print("Starting tearDown...")
        self.stop_event.set() # 设置停止信号

        # 尝试优雅地停止节点和协调器
        # 注意：Python 的 socket server 没有内置的 shutdown 方法，且线程是 daemon
        # 关闭可能不完美，但尽力而为

        # 粗暴方式：依赖 daemon 线程自动退出 (可能不清理 socket)
        # 更好的方式是在 Coordinator 和 Node 中实现 shutdown 逻辑

        print("Stopping components (relying on daemon threads or needing explicit shutdown)...")
        # 等待一小段时间让 daemon 线程有机会退出
        time.sleep(0.5)

        # 清理实例变量
        self.coordinator = None
        self.node_a1 = None
        self.node_a1b = None
        self.node_a2 = None
        self.client = None
        print("Teardown complete.")

    def test_node_registration(self):
        """测试节点是否成功注册到协调器"""
        print("Running test_node_registration...")
        self.assertIsNotNone(self.coordinator, "Coordinator should be initialized")

        # 从协调器获取已注册节点列表
        registered_nodes = {}
        with self.coordinator.lock:
            # 复制一份以避免并发问题
            registered_nodes = dict(self.coordinator.account_nodes)

        print(f"Coordinator registered nodes: {registered_nodes.keys()}")

        # 验证所有启动的节点都已注册
        self.assertIn('a1', registered_nodes, "Node a1 should be registered")
        self.assertIn('a1b', registered_nodes, "Node a1b should be registered")
        self.assertIn('a2', registered_nodes, "Node a2 should be registered")

        # 验证节点信息（端口、角色）是否正确
        self.assertEqual(registered_nodes['a1']['port'], TEST_NODE_A1_PORT)
        # 注意：角色可能在心跳中被确认，或在协调器配对逻辑中改变，检查最终状态
        # 如果 setUp 中节点启动后立刻检查，可能角色还未完全同步，加个 sleep 或后续检查
        self.assertEqual(registered_nodes['a1'].get('role', 'unknown'), 'primary')

        self.assertEqual(registered_nodes['a1b']['port'], TEST_NODE_A1B_PORT)
        self.assertEqual(registered_nodes['a1b'].get('role', 'unknown'), 'backup')

        self.assertEqual(registered_nodes['a2']['port'], TEST_NODE_A2_PORT)
        self.assertEqual(registered_nodes['a2'].get('role', 'unknown'), 'primary')

        # 验证主备配对是否建立 (这依赖于协调器的心跳处理和配对逻辑)
        with self.coordinator.lock:
             pair = self.coordinator.node_pairs.get('a1')
        self.assertEqual(pair, 'a1b', "Primary a1 should be paired with backup a1b")
        print("test_node_registration completed successfully.")

    def test_basic_transfer(self):
        """测试通过协调器进行的基本转账 (2PC)"""
        print("Running test_basic_transfer...")
        initial_balance = 1000

        # 1. 初始化账户余额 (直接操作节点或通过客户端)
        #    直接操作更简单，但通过客户端更贴近实际流程
        print(f"Initializing accounts a1 and a2 with {initial_balance}...")
        # 等待节点确实启动完毕并可以处理请求
        time.sleep(0.5)
        init_resp_a1 = self.client.send_request({
            'command': 'init_balance', 'account_id': 'a1', 'amount': initial_balance
        })
        init_resp_a2 = self.client.send_request({
            'command': 'init_balance', 'account_id': 'a2', 'amount': initial_balance
        })
        # 粗略检查初始化是否成功
        # self.assertEqual(init_resp_a1.get('status'), 'success')
        # self.assertEqual(init_resp_a2.get('status'), 'success')
        # 短暂等待确保状态更新
        time.sleep(0.5)

        # 确认初始余额
        self.assertEqual(self.node_a1.balance, initial_balance)
        self.assertEqual(self.node_a2.balance, initial_balance)
        initial_history_len_a1 = len(self.node_a1.transaction_history)
        initial_history_len_a2 = len(self.node_a2.transaction_history)

        # 2. 执行转账 (a1 -> a2, 100)
        transfer_amount = 100
        print(f"Attempting to transfer {transfer_amount} from a1 to a2...")
        transfer_response = self.client.transfer('a1', 'a2', transfer_amount)
        print(f"Transfer response: {transfer_response}")

        # 3. 验证转账结果
        self.assertEqual(transfer_response.get('status'), 'success', f"Transfer failed: {transfer_response.get('message')}")

        # 4. 验证节点状态变化
        #    给一点时间让状态更新（特别是如果涉及到异步操作）
        time.sleep(0.5)

        # 检查余额
        self.assertEqual(self.node_a1.balance, initial_balance - transfer_amount, "Sender balance incorrect")
        self.assertEqual(self.node_a2.balance, initial_balance + transfer_amount, "Receiver balance incorrect")

        # 检查交易历史 (可选，但推荐)
        self.assertEqual(len(self.node_a1.transaction_history), initial_history_len_a1 + 1, "Sender history length incorrect")
        self.assertEqual(len(self.node_a2.transaction_history), initial_history_len_a2 + 1, "Receiver history length incorrect")
        # 可以进一步检查历史记录的内容，例如金额和交易ID（如果响应中有返回）
        self.assertEqual(self.node_a1.transaction_history[-1]['amount'], -transfer_amount)
        self.assertEqual(self.node_a2.transaction_history[-1]['amount'], transfer_amount)
        if 'transaction_id' in transfer_response:
            txn_id = transfer_response['transaction_id']
            self.assertEqual(self.node_a1.transaction_history[-1]['transaction_id'], txn_id)
            self.assertEqual(self.node_a2.transaction_history[-1]['transaction_id'], txn_id)

        print("test_basic_transfer completed successfully.")

    def test_primary_backup_sync(self):
        """测试主备节点间的数据同步"""
        print("Running test_primary_backup_sync...")
        initial_balance = 5000
        sync_wait_time = self.node_a1.sync_interval * 1.5 # 等待超过一个同步周期

        # 1. 确认初始状态 (假设都为 0 或由 setUp 决定)
        self.assertEqual(self.node_a1.balance, 0, "Initial primary balance should be 0")
        self.assertEqual(self.node_a1b.balance, 0, "Initial backup balance should be 0")

        # 2. 在主节点上初始化余额
        print(f"Initializing balance on primary node a1 to {initial_balance}...")
        # 直接调用主节点的内部方法或通过客户端
        # 使用客户端更贴近实际，但需要协调器转发
        init_resp_a1 = self.client.send_request({
            'command': 'init_balance', 'account_id': 'a1', 'amount': initial_balance
        })
        self.assertEqual(init_resp_a1.get('status'), 'success')
        self.assertEqual(self.node_a1.balance, initial_balance, "Primary balance not updated immediately")
        print(f"Primary node a1 balance updated to {self.node_a1.balance}")

        # 3. 等待同步发生
        print(f"Waiting {sync_wait_time} seconds for synchronization...")
        time.sleep(sync_wait_time)

        # 4. 验证备份节点的余额是否已同步
        print(f"Checking backup node a1b balance. Current balance: {self.node_a1b.balance}")
        # 可能由于线程调度，同步稍有延迟，增加一个小的额外等待或重试逻辑
        retries = 3
        for i in range(retries):
            if self.node_a1b.balance == initial_balance:
                break
            print(f"Retry {i+1}/{retries}: Backup balance not yet synced. Waiting a bit more...")
            time.sleep(1)

        self.assertEqual(self.node_a1b.balance, initial_balance, "Backup balance was not synchronized correctly")

        # 5. (可选) 验证交易历史同步
        #    如果 init_balance 也产生交易历史，或者执行一笔转账再检查历史
        #    假设 init_balance 不产生历史，我们执行一笔小额转账
        transfer_amount = 50
        print(f"Performing a transfer involving a1 ({transfer_amount}) to trigger history sync...")
        tx_resp = self.client.transfer('a1', 'a2', transfer_amount)
        self.assertEqual(tx_resp.get('status'), 'success')
        time.sleep(sync_wait_time) # 等待同步

        # 比较主备节点的交易历史长度和内容
        self.assertGreater(len(self.node_a1.transaction_history), 0)
        self.assertEqual(len(self.node_a1.transaction_history), len(self.node_a1b.transaction_history),
                         "Transaction history length mismatch between primary and backup")
        # 可以进一步比较最后一条记录
        self.assertEqual(self.node_a1.transaction_history[-1]['amount'], self.node_a1b.transaction_history[-1]['amount'])

        print("test_primary_backup_sync completed successfully.")

    def test_failure_detection_and_promotion(self):
        """测试协调器故障检测和备份节点提升"""
        pass # Placeholder for the next test

    # --- 后续将添加更多集成测试用例 --- #
    # def test_primary_backup_sync(self):
    #     pass


if __name__ == '__main__':
    # 创建一个测试套件，只包含这个文件中的测试
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestIntegration))

    # 创建一个测试运行器
    runner = unittest.TextTestRunner(verbosity=2)

    # 运行测试
    print("Starting integration tests...")
    runner.run(suite)
    print("Integration tests finished.") 