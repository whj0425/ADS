import subprocess
import time
import os
import sys
import signal
import json

# 从配置文件导入网络设置
sys.path.append('.')
from config.network_config import (
    COMPUTER_A_IP, 
    COMPUTER_B_IP, 
    COORDINATOR_PORT,
    A2_PORT,
    A2B_PORT
)

# 进程列表，用于跟踪启动的进程
processes = []

def setup_networking():
    """修改源代码中的网络配置，将localhost改为0.0.0.0"""
    # 读取并修改account_node.py
    try:
        with open('src/account_node.py', 'r') as f:
            content = f.read()
        
        # 将'localhost'替换为'0.0.0.0'以允许远程连接
        if 'server.bind((\'localhost\'' in content:
            content = content.replace('server.bind((\'localhost\'', 'server.bind((\'0.0.0.0\'')
            with open('src/account_node.py', 'w') as f:
                f.write(content)
            print("已更新account_node.py的网络设置")
    except Exception as e:
        print(f"更新account_node.py失败: {e}")
    
    # 修改client.py中的协调器主机地址
    try:
        with open('src/client.py', 'r') as f:
            content = f.read()
        
        # 找到BankClient类初始化部分，修改协调器主机地址
        if 'def __init__(self, coordinator_host=\'localhost\'' in content:
            content = content.replace('def __init__(self, coordinator_host=\'localhost\'', 
                                     f'def __init__(self, coordinator_host=\'{COMPUTER_A_IP}\'')
            with open('src/client.py', 'w') as f:
                f.write(content)
            print(f"已更新client.py的协调器地址为 {COMPUTER_A_IP}")
    except Exception as e:
        print(f"更新client.py失败: {e}")

def create_account_data(node_id, initial_balance=10000):
    """创建或更新账户节点的数据文件"""
    data = {
        'balance': initial_balance,
        'transaction_history': []
    }
    
    try:
        with open(f'data/{node_id}_data.json', 'w') as f:
            json.dump(data, f, indent=2)
        print(f"已创建/更新账户节点 {node_id} 的数据文件")
    except Exception as e:
        print(f"创建账户数据失败: {e}")

def signal_handler(sig, frame):
    """处理Ctrl+C信号，确保干净地关闭所有进程"""
    print("\n正在关闭所有进程...")
    for process in processes:
        if process.poll() is None:  # 如果进程仍在运行
            process.terminate()
    sys.exit(0)

def main():
    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    
    # 设置网络配置
    setup_networking()
    
    # 创建必要的配置文件
    create_account_data('a2')
    create_account_data('a2b')
    
    print(f"电脑A: {COMPUTER_A_IP}")
    print(f"电脑B: {COMPUTER_B_IP}")
    print("\n正在启动分布式银行系统（电脑B）...\n")
    
    # 检查电脑A是否可以连接
    print(f"正在检查与事务协调器（{COMPUTER_A_IP}:{COORDINATOR_PORT}）的连接...")
    import socket
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        s.connect((COMPUTER_A_IP, COORDINATOR_PORT))
        s.close()
        print("成功连接到事务协调器！")
    except Exception as e:
        print(f"无法连接到事务协调器: {e}")
        print("请确保电脑A已启动事务协调器且网络连接正常")
        print("是否仍要继续启动节点？(y/n)")
        response = input().lower()
        if response != 'y':
            print("已取消启动")
            return
    
    # 启动账户节点a2（主节点）
    print("启动账户节点 a2（主节点）...")
    a2_cmd = [sys.executable, '-u', 'src/account_node.py', 'a2', str(A2_PORT), str(COORDINATOR_PORT), 'primary', COMPUTER_A_IP]
    a2_process = subprocess.Popen(a2_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    processes.append(a2_process)
    time.sleep(1)
    
    # 启动账户节点a2b（备份节点）
    print("启动账户节点 a2b（备份节点）...")
    a2b_cmd = [sys.executable, '-u', 'src/account_node.py', 'a2b', str(A2B_PORT), str(COORDINATOR_PORT), 'backup', COMPUTER_A_IP]
    a2b_process = subprocess.Popen(a2b_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    processes.append(a2b_process)
    time.sleep(1)
    
    print("\n所有节点已启动！")
    print(f"账户节点a2运行在端口: {A2_PORT}")
    print(f"备份节点a2b运行在端口: {A2B_PORT}")
    
    # 启动客户端
    print("\n启动客户端...")
    client_cmd = [sys.executable, '-u', 'src/client.py', str(COORDINATOR_PORT)]
    client_process = subprocess.Popen(client_cmd)
    processes.append(client_process)
    
    # 等待所有进程结束
    try:
        for process in processes:
            process.wait()
    except KeyboardInterrupt:
        signal_handler(None, None)

if __name__ == "__main__":
    main() 