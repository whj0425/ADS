import subprocess
import time
import socket
import json
import sys
import os

def is_port_available(port):
    """Check if a port is available for use."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(('localhost', port))
            return True
        except:
            return False

def wait_for_service(port, timeout=30):
    """Wait for a service to be available on a port."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(('localhost', port))
                return True
        except:
            time.sleep(1)
    return False

def initialize_accounts(coordinator_port, amount=10000):
    """Initialize all accounts with a starting balance."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(('localhost', coordinator_port))
            init_request = {
                'command': 'init_accounts',
                'amount': amount
            }
            s.send(json.dumps(init_request).encode('utf-8'))
            init_response = json.loads(s.recv(4096).decode('utf-8'))
            
            if init_response.get('status') == 'success':
                print(f"All accounts initialized with {amount}")
                return True
            else:
                print(f"Failed to initialize accounts: {init_response.get('message')}")
                return False
    
    except Exception as e:
        print(f"Error initializing accounts: {e}")
        return False

def main():
    # Configuration
    coordinator_port = 5010
    account_ports = [5011, 5012]  # Ports for account nodes
    initial_balance = 10000
    
    # Check if ports are available
    unavailable_ports = []
    if not is_port_available(coordinator_port):
        unavailable_ports.append(coordinator_port)
    
    for port in account_ports:
        if not is_port_available(port):
            unavailable_ports.append(port)
    
    if unavailable_ports:
        print(f"Error: The following ports are already in use: {unavailable_ports}")
        print("Please close the applications using these ports and try again.")
        return
    
    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    
    # Start the coordinator
    coordinator_id = "c1"
    print(f"Starting transaction coordinator {coordinator_id} on port {coordinator_port}...")
    coordinator_process = subprocess.Popen(
        [sys.executable, 'transaction_coordinator.py', str(coordinator_port)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    # Wait for the coordinator to start
    if not wait_for_service(coordinator_port):
        print("Error: Failed to start coordinator")
        coordinator_process.terminate()
        return
    
    print("Transaction coordinator started successfully")
    
    # Start the account nodes
    account_processes = []
    for i, port in enumerate(account_ports):
        account_id = f"a{i+1}"
        print(f"Starting account node {account_id} on port {port}...")
        
        account_process = subprocess.Popen(
            [sys.executable, 'account_node.py', account_id, str(port), str(coordinator_port)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        account_processes.append((account_id, account_process))
    
    # Wait for all account nodes to register with the coordinator
    print("Waiting for account nodes to register...")
    time.sleep(10)  # Give more time for nodes to register
    
    # Verify the coordinator is responding
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(('localhost', coordinator_port))
            test_request = {'command': 'list_accounts'}
            s.send(json.dumps(test_request).encode('utf-8'))
            response = json.loads(s.recv(4096).decode('utf-8'))
            print(f"Coordinator test response: {response}")
    except Exception as e:
        print(f"Error testing coordinator connection: {e}")
        print("This indicates the coordinator might not be running properly.")
    
    # Initialize account balances
    print(f"Initializing all accounts with {initial_balance}...")
    if not initialize_accounts(coordinator_port, initial_balance):
        print("Failed to initialize accounts. Shutting down...")
        for _, process in account_processes:
            process.terminate()
        coordinator_process.terminate()
        return
    
    print("\nDistributed Banking System started successfully!")
    print(f"- Transaction Coordinator {coordinator_id} running on port {coordinator_port}")
    for account_id, _ in account_processes:
        print(f"- Account Node {account_id} registered")
    print(f"- All accounts initialized with balance of {initial_balance}")
    print("\nTo interact with the system, run the client: python client.py")
    print(f"\nIMPORTANT: Make sure to use port {coordinator_port} for the client!")
    
    try:
        # Keep the system running until Ctrl+C
        print("\nPress Ctrl+C to shut down the system")
        while True:
            time.sleep(1)
    
    except KeyboardInterrupt:
        print("\nShutting down the system...")
        for account_id, process in account_processes:
            print(f"Terminating account node {account_id}...")
            process.terminate()
        
        print("Terminating transaction coordinator...")
        coordinator_process.terminate()
        
        print("System shutdown complete")

if __name__ == "__main__":
    main()
