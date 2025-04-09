import subprocess
import time
import os
import sys
import signal
import json

# Import network settings from configuration file
sys.path.append('.')
from config.network_config import (
    COMPUTER_A_IP, 
    COMPUTER_B_IP, 
    COORDINATOR_PORT, 
    A1_PORT, 
    A1B_PORT
)

# List to track all spawned processes
processes = []

def setup_networking():
    """Modify network configuration in source code, replacing localhost with 0.0.0.0 to allow remote connections"""
    # Read and modify transaction_coordinator.py
    try:
        with open('src/transaction_coordinator.py', 'r') as f:
            content = f.read()
        
        # Replace 'localhost' with '0.0.0.0' to allow remote connections
        if 'server.bind((\'localhost\'' in content:
            content = content.replace('server.bind((\'localhost\'', 'server.bind((\'0.0.0.0\'')
            with open('src/transaction_coordinator.py', 'w') as f:
                f.write(content)
            print("Updated network settings in transaction_coordinator.py")
    except Exception as e:
        print(f"Failed to update transaction_coordinator.py: {e}")
    
    # Read and modify account_node.py
    try:
        with open('src/account_node.py', 'r') as f:
            content = f.read()
        
        # Replace 'localhost' with '0.0.0.0' to allow remote connections
        if 'server.bind((\'localhost\'' in content:
            content = content.replace('server.bind((\'localhost\'', 'server.bind((\'0.0.0.0\'')
            with open('src/account_node.py', 'w') as f:
                f.write(content)
            print("Updated network settings in account_node.py")
    except Exception as e:
        print(f"Failed to update account_node.py: {e}")

def create_coordinator_config():
    """Create or update the transaction coordinator configuration file
    
    Initializes an empty structure with placeholders for account nodes,
    transactions, and node pairs that will be populated during runtime.
    """
    config = {
        'account_nodes': {},
        'transactions': {},
        'node_pairs': {}
    }
    
    try:
        with open('data/coordinator_data.json', 'w') as f:
            json.dump(config, f, indent=2)
        print("Created/updated coordinator configuration file")
    except Exception as e:
        print(f"Failed to create coordinator configuration: {e}")

def create_account_data(node_id, initial_balance=10000):
    """Create or update account node data file
    
    Args:
        node_id: Identifier for the account node
        initial_balance: Starting balance for the account (default: 10000)
    
    Creates a JSON file with the initial balance and empty transaction history.
    """
    data = {
        'balance': initial_balance,
        'transaction_history': []
    }
    
    try:
        with open(f'data/{node_id}_data.json', 'w') as f:
            json.dump(data, f, indent=2)
        print(f"Created/updated data file for account node {node_id}")
    except Exception as e:
        print(f"Failed to create account data: {e}")

def signal_handler(sig, frame):
    """Handle Ctrl+C signal to ensure clean shutdown of all processes
    
    Terminates all spawned processes when the user interrupts the program.
    """
    print("\nShutting down all processes...")
    for process in processes:
        if process.poll() is None:  # If process is still running
            process.terminate()
    sys.exit(0)

def main():
    # Register signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    
    # Configure network settings
    setup_networking()
    
    # Create necessary configuration files
    create_coordinator_config()
    create_account_data('a1')
    create_account_data('a1b')
    
    print(f"Computer A: {COMPUTER_A_IP}")
    print(f"Computer B: {COMPUTER_B_IP}")
    print("\nStarting distributed banking system (Computer A)...\n")
    
    # Start transaction coordinator
    print("Starting transaction coordinator...")
    coordinator_cmd = [sys.executable, '-u', 'src/transaction_coordinator.py', str(COORDINATOR_PORT)]
    coordinator_process = subprocess.Popen(coordinator_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    processes.append(coordinator_process)
    
    # Wait for coordinator to initialize
    time.sleep(2)
    print("Transaction coordinator started")
    
    # Start account node a1 (primary node)
    print("Starting account node a1 (primary)...")
    a1_cmd = [sys.executable, '-u', 'src/account_node.py', 'a1', str(A1_PORT), str(COORDINATOR_PORT), 'primary']
    a1_process = subprocess.Popen(a1_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    processes.append(a1_process)
    time.sleep(1)
    
    # Start account node a1b (backup node)
    print("Starting account node a1b (backup)...")
    a1b_cmd = [sys.executable, '-u', 'src/account_node.py', 'a1b', str(A1B_PORT), str(COORDINATOR_PORT), 'backup']
    a1b_process = subprocess.Popen(a1b_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    processes.append(a1b_process)
    time.sleep(1)
    
    print("\nAll nodes started successfully!")
    print(f"Transaction coordinator running on port: {COORDINATOR_PORT}")
    print(f"Account node a1 running on port: {A1_PORT}")
    print(f"Backup node a1b running on port: {A1B_PORT}")
    
    # Print connection information
    print("\n=== Connection Information ===")
    print(f"Computer B should use the following configuration to connect to the coordinator:")
    print(f"Coordinator address: {COMPUTER_A_IP}")
    print(f"Coordinator port: {COORDINATOR_PORT}")
    
    # Start client application
    print("\nStarting client application...")
    client_cmd = [sys.executable, '-u', 'src/client.py', str(COORDINATOR_PORT)]
    client_process = subprocess.Popen(client_cmd)
    processes.append(client_process)
    
    # Wait for all processes to complete
    try:
        for process in processes:
            process.wait()
    except KeyboardInterrupt:
        signal_handler(None, None)

if __name__ == "__main__":
    main()