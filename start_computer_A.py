#!/usr/bin/env python
import subprocess
import time
import os
import sys
import signal
import json

# 获取本机IP地址，这里需要填写电脑A的实际IP地址
COMPUTER_A_IP = "172.20.10.2"  # 请修改为电脑A的实际IP地址
COMPUTER_B_IP = "172.20.10.9"  # 请修改为电脑B的实际IP地址

# 端口配置
COORDINATOR_PORT = 5010
A1_PORT = 5011
A1B_PORT = 5012

# 进程列表，用于跟踪启动的进程
processes = []

def setup_networking():
    """修改源代码中的网络配置，将localhost改为0.0.0.0"""
    # 读取并修改transaction_coordinator.py
    try:
        with open('src/transaction_coordinator.py', 'r') as f:
            content = f.read()
        
        # 将'localhost'替换为'0.0.0.0'以允许远程连接
        if 'server.bind((\'localhost\'' in content:
            content = content.replace('server.bind((\'localhost\'', 'server.bind((\'0.0.0.0\'')
            with open('src/transaction_coordinator.py', 'w') as f:
                f.write(content)
            print("已更新transaction_coordinator.py的网络设置")
    except Exception as e:
        print(f"更新transaction_coordinator.py失败: {e}")
    
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

def create_coordinator_config():
    """创建或更新协调器配置文件"""
    config = {
        'account_nodes': {},
        'transactions': {},
        'node_pairs': {}
    }
    
    try:
        with open('src/coordinator_data.json', 'w') as f:
            json.dump(config, f, indent=2)
        print("已创建/更新协调器配置文件")
    except Exception as e:
        print(f"创建协调器配置失败: {e}")

def create_account_data(node_id, initial_balance=10000):
    """创建或更新账户节点的数据文件"""
    data = {
        'balance': initial_balance,
        'transaction_history': []
    }
    
    try:
        with open(f'src/{node_id}_data.json', 'w') as f:
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
    create_coordinator_config()
    create_account_data('a1')
    create_account_data('a1b')
    
    print(f"电脑A: {COMPUTER_A_IP}")
    print(f"电脑B: {COMPUTER_B_IP}")
    print("\n正在启动分布式银行系统（电脑A）...\n")
    
    # 启动事务协调器
    print("启动事务协调器...")
    coordinator_cmd = [sys.executable, '-u', 'src/transaction_coordinator.py', str(COORDINATOR_PORT)]
    coordinator_process = subprocess.Popen(coordinator_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    processes.append(coordinator_process)
    
    # 等待协调器启动
    time.sleep(2)
    print("事务协调器已启动")
    
    # 启动账户节点a1（主节点）
    print("启动账户节点 a1（主节点）...")
    a1_cmd = [sys.executable, '-u', 'src/account_node.py', 'a1', str(A1_PORT), str(COORDINATOR_PORT), 'primary']
    a1_process = subprocess.Popen(a1_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    processes.append(a1_process)
    time.sleep(1)
    
    # 启动账户节点a1b（备份节点）
    print("启动账户节点 a1b（备份节点）...")
    a1b_cmd = [sys.executable, '-u', 'src/account_node.py', 'a1b', str(A1B_PORT), str(COORDINATOR_PORT), 'backup']
    a1b_process = subprocess.Popen(a1b_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    processes.append(a1b_process)
    time.sleep(1)
    
    print("\n所有节点已启动！")
    print(f"事务协调器运行在端口: {COORDINATOR_PORT}")
    print(f"账户节点a1运行在端口: {A1_PORT}")
    print(f"备份节点a1b运行在端口: {A1B_PORT}")
    
    # 打印连接信息
    print("\n=== 连接信息 ===")
    print(f"电脑B应当使用以下配置连接到协调器:")
    print(f"协调器地址: {COMPUTER_A_IP}")
    print(f"协调器端口: {COORDINATOR_PORT}")
    
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