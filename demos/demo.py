import socket
import json
import time
import sys
import os

# Add project root directory to Python path for correct importing of src package
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)  # Get project root directory
if root_dir not in sys.path:
    sys.path.append(root_dir)  # Add project root directory to Python path

# Configuration 
COORDINATOR_HOST = "localhost"  # Use localhost for local running
COORDINATOR_PORT = 5010

# 导入我们创建的大量并发转账测试模块
# 使用sys.path确保可以找到测试模块
tests_dir = os.path.join(root_dir, 'tests')
if tests_dir not in sys.path:
    sys.path.append(tests_dir)

# 在正确设置sys.path后导入测试模块
from test_concurrent_massive import run_test as run_massive_transfers

class DemoScenarios:
    def __init__(self, host=COORDINATOR_HOST, port=COORDINATOR_PORT):
        self.host = host
        self.port = port
    
    def send_request(self, request):
        """Send request to the coordinator with retry mechanism
        
        Args:
            request: Dictionary containing the request details
            
        Returns:
            Dictionary containing the response or error message
        """
        max_retries = 2
        retry_count = 1
        last_error = None
        
        while retry_count <= max_retries:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(10)  # Increase timeout to 10 seconds
                    s.connect((self.host, self.port))
                    s.send(json.dumps(request).encode('utf-8'))
                    response = json.loads(s.recv(4096).decode('utf-8'))
                    return response
            except Exception as e:
                last_error = e
                retry_count += 1
                if retry_count <= max_retries:
                    print(f"Request failed (attempt {retry_count}/{max_retries}): {e}, retrying...")
                    time.sleep(1)  # Wait 1 second before retrying
                else:
                    print(f"Request failed: {e}, maximum retry attempts reached.")
        
        # If all retries fail
        return {'status': 'error', 'message': str(last_error)}
    
    def simulate_node_failure(self, node_id):
        """Simulate node failure to test failover mechanism
        
        Args:
            node_id: ID of the node to simulate failure for
            
        Returns:
            Response from the coordinator about the failure simulation
        """
        request = {
            'command': 'simulate_failure',
            'node_id': node_id
        }
        print(f"Simulating failure for node {node_id}...")
        response = self.send_request(request)
        if response['status'] == 'success':
            print(f"Node {node_id} has been marked as failed")
            
            # Display final node status
            if 'final_node_status' in response:
                print(f"Node final status: {response['final_node_status']}")
            
            if response.get('backup_node'):
                print(f"Backup node {response['backup_node']} will take over")
                
                # Check if backup node was successfully promoted
                if response.get('backup_promoted', False):
                    print(f"✅ Backup node {response['backup_node']} was successfully promoted to primary")
                else:
                    print(f"⚠️ Backup node {response['backup_node']} promotion process may not be complete, but node {node_id} is still marked as failed")
            else:
                print(f"⚠️ Warning: Node {node_id} has no backup node, failure will cause service unavailability!")
            
            # Immediately check the status of the simulated failed node to confirm status change
            print("\nChecking node status immediately to confirm failure simulation is effective:")
            self.check_node_status(node_id)
        else:
            print(f"Failure simulation failed: {response.get('message')}")
            print("Please check if the node exists or if the system is running properly")
        return response
    
    def recover_node(self, node_id):
        """Recover a failed node
        
        Args:
            node_id: ID of the node to recover
            
        Returns:
            Response from the coordinator about the recovery process
        """
        request = {
            'command': 'recover_node',
            'node_id': node_id
        }
        print(f"Recovering node {node_id}...")
        response = self.send_request(request)
        if response['status'] == 'success':
            print(f"Node {node_id} has been restored to normal status")
        else:
            print(f"Node recovery failed: {response.get('message')}")
        return response
    
    def check_node_status(self, node_id):
        """Check the status of a specific node
        
        Args:
            node_id: ID of the node to check
            
        Returns:
            Response containing node status information
        """
        request = {
            'command': 'check_node_status',
            'node_id': node_id
        }
        print(f"Checking status of node {node_id}...")
        response = self.send_request(request)
        if response['status'] == 'success':
            status = "active" if response.get('is_active', False) else "failed"
            role = response.get('role', 'unknown')
            print(f"Node {node_id} status: {status}, role: {role}")
            if response.get('backup_node'):
                print(f"Backup node: {response['backup_node']}")
            
            # Display node state field
            if 'state' in response:
                print(f"Node state marker: {response['state']}")
            
            # Display complete node information
            if 'node_info' in response:
                print(f"Complete node info: {response['node_info']}")
            
            # Emphasize failed status
            if not response.get('is_active', True) or response.get('state') == 'failed':
                print(f"⚠️ Warning: Node {node_id} is currently in failed state!")
                if response.get('backup_node'):
                    backup_node = response['backup_node']
                    print(f"✅ Backup node {backup_node} should have taken over its operation")
                    # Check backup node status
                    self.check_node_status(backup_node)
                else:
                    print(f"❌ This node has no available backup node!")
        else:
            print(f"Status check failed: {response.get('message')}")
            print(f"⚠️ Node {node_id} may be inaccessible or does not exist")
        return response
    
    def list_accounts(self):
        """List all account nodes in the system
        
        Returns:
            Response containing the list of available accounts
        """
        request = {
            'command': 'list_accounts'
        }
        print("Retrieving all account nodes...")
        response = self.send_request(request)
        if response['status'] == 'success':
            print("Available account nodes:")
            for account in response.get('accounts', []):
                print(f"  - {account}")
        else:
            print(f"Failed to get account list: {response.get('message')}")
        return response
    
    def run_interactive_failure_recovery_demo(self):
        """Run an interactive demonstration of failure recovery
        
        This demo guides users through the complete process of node failure
        and recovery, showcasing the high availability features of the system.
        """
        print("\n=== Interactive Failure Recovery Demo ===")
        print("This demo will guide you through the complete process of node failure and recovery, demonstrating the system's high availability features")
        
        # Create client object
        from src.client import BankClient
        client = BankClient(coordinator_host=self.host, coordinator_port=self.port)
        
        # Prompt Step 1: View current available nodes
        input("\nStep 1: View current available nodes. Press Enter to continue...")
        self.list_accounts()
        
        # Prompt Step 2: Initialize accounts
        input("\nStep 2: Initialize all accounts with balance 10000. Press Enter to continue...")
        response = client.initialize_accounts(10000)
        if response['status'] == 'success':
            print("All accounts have been initialized with balance 10000")
        else:
            print(f"Account initialization failed: {response.get('message')}")
            return
        
        # Prompt Step 3: Query balance
        input("\nStep 3: Query initial balances. Press Enter to continue...")
        for account in ['a1', 'a2']:
            response = client.get_balance(account)
            if response['status'] == 'success':
                print(f"Account {account} balance: {response.get('balance')}")
        
        # Prompt Step 4: Simulate node failure
        node_to_fail = input("\nStep 4: Enter the node ID to simulate failure (recommended: a1): ").strip()
        if not node_to_fail:
            node_to_fail = 'a1'
        
        # Check initial state
        print("\nChecking node status before failure simulation:")
        self.check_node_status(node_to_fail)
        
        # Execute failure simulation
        failure_response = self.simulate_node_failure(node_to_fail)
        if failure_response['status'] == 'success':
            print(f"\nNode {node_to_fail} has been simulated as failed")
        else:
            print(f"\nFailure simulation failed: {failure_response.get('message')}")
            return
        
        # Prompt Step 5: Check node status
        input("\nStep 5: Check node status. Press Enter to continue...")
        print("\nConfirming node status again:")
        self.check_node_status(node_to_fail)
        backup_node = f"{node_to_fail}b"
        self.check_node_status(backup_node)
        
        # Prompt Step 6: Query failed node balance
        input(f"\nStep 6: Try querying the balance of failed node {node_to_fail} (should automatically redirect to backup node). Press Enter to continue...")
        response = client.get_balance(node_to_fail)
        if response['status'] == 'success':
            print(f"Account {node_to_fail} balance: {response.get('balance')}")
            if response.get('used_backup'):
                print(f"(queried through backup node {backup_node})")
        else:
            print(f"Query balance failed: {response.get('message')}")
        
        # Prompt Step 7: Transfer funds from failed node
        input(f"\nStep 7: Try transferring funds from failed node {node_to_fail} (should automatically use backup node). Press Enter to continue...")
        transfer_amount = 500
        transfer_to = 'a2' if node_to_fail != 'a2' else 'a1'
        print(f"Executing transfer: {node_to_fail} -> {transfer_to} ({transfer_amount})...")
        response = client.transfer(node_to_fail, transfer_to, transfer_amount)
        if response['status'] == 'success':
            print(f"Transfer succeeded! {response.get('message', '')}")
            if response.get('used_backup'):
                print("(completed through backup node)")
        else:
            print(f"Transfer failed: {response.get('message')}")
        
        # Prompt Step 8: Check balance after transfer
        input("\nStep 8: Check balance after transfer. Press Enter to continue...")
        for account in ['a1', 'a2']:
            response = client.get_balance(account)
            if response['status'] == 'success':
                print(f"Account {account} balance: {response.get('balance')}")
                if response.get('used_backup'):
                    print("(queried through backup node)")
        
        # Prompt Step 9: Recover failed node
        input(f"\nStep 9: Recover failed node {node_to_fail}. Press Enter to continue...")
        self.recover_node(node_to_fail)
        
        # Prompt Step 10: Check recovered node status
        input("\nStep 10: Check recovered node status. Press Enter to continue...")
        print("\nConfirming node recovery:")
        self.check_node_status(node_to_fail)
        
        # Prompt Step 11: Query recovered node balance
        input(f"\nStep 11: Query recovered node {node_to_fail} balance. Press Enter to continue...")
        response = client.get_balance(node_to_fail)
        if response['status'] == 'success':
            print(f"Account {node_to_fail} balance: {response.get('balance')}")
            if response.get('used_backup'):
                print("(queried through backup node)")
        else:
            print(f"Query balance failed: {response.get('message')}")
        
        # Prompt Step 12: Execute another transfer after recovery
        input(f"\nStep 12: Execute another transfer after recovery. Press Enter to continue...")
        transfer_amount = 300
        print(f"Executing transfer: {node_to_fail} -> {transfer_to} ({transfer_amount})...")
        response = client.transfer(node_to_fail, transfer_to, transfer_amount)
        if response['status'] == 'success':
            print(f"Transfer succeeded!")
        else:
            print(f"Transfer failed: {response.get('message')}")
        
        # Prompt Step 13: Final check balance
        input("\nStep 13: Final check all account balances. Press Enter to continue...")
        for account in ['a1', 'a2']:
            response = client.get_balance(account)
            print(f"Account {account} balance: {response.get('balance', 'unknown')}")
        
        print("\n=== Interactive Failure Recovery Demo Completed ===")
        print("You have successfully experienced:")
        print("1. Node failure simulation and detection")
        print("2. Automatic redirection of failed node (query and transfer)")
        print("3. Node recovery process")
        print("4. Normal functionality after recovery")
        print("\nThe entire process demonstrates the system's high availability and fault tolerance")
    
    def run_concurrent_transfers_demo(self):
        """Run concurrent transfer demonstration"""
        print("\n=== Concurrent Transfer Demo ===")

        
        # Create client object
        from src.client import BankClient
        import threading
        
        client = BankClient(coordinator_host=self.host, coordinator_port=self.port)
        
        # Initialize account balances
        print("Initializing account balances...")
        response = client.initialize_accounts(10000)
        
        # Check initial balance
        print("\nChecking initial balance...")
        for account in ['a1', 'a2']:
            response = client.get_balance(account)
            if response['status'] == 'success':
                print(f"Account {account} balance: {response.get('balance')}")
        
        # Define transfer function
        def do_transfer(from_acc, to_acc, amount, delay=0):
            time.sleep(delay)
            print(f"Executing transfer: {from_acc} -> {to_acc} ({amount})...")
            response = client.transfer(from_acc, to_acc, amount)
            if response['status'] == 'success':
                print(f"Transfer succeeded: {from_acc} -> {to_acc} ({amount})")
            else:
                print(f"Transfer failed: {from_acc} -> {to_acc}: {response.get('message')}")
        
        # Create concurrent transfers
        print("\nStarting 5 concurrent transfers...")
        threads = []
        
        # Create multiple concurrent transfers
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
        
        # Wait for all transfers to complete
        for t in threads:
            t.join()
        
        print("\nAll concurrent transfers completed")
        
        # Check final balance
        print("\nChecking final balance...")
        for account in ['a1', 'a2']:
            response = client.get_balance(account)
            if response['status'] == 'success':
                print(f"Account {account} balance: {response.get('balance')}")

    def run_improved_concurrent_transfers_demo(self):
        """Run improved concurrent transfer demonstration"""
        print("\n=== Improved Concurrent Transfer Demo ===")
        print("This demo will use the following improvements:")
        print("1. Global transaction queue and sorting - Process transfer requests in order, avoiding chaos")
        print("2. Account-level locks - Only transactions involving the same account wait for each other")
        print("3. Complete two-phase commit - Fully implement transaction processing flow, including error recovery")
        print("4. Transaction log and status tracking - Record all transaction steps and status, for easy tracking")
        
        # Here we just run a test, actual improvements will only be implemented in the coordinator part
        # But through testing we can see the effect of improvements
        
        # Create client object
        from src.client import BankClient
        import threading
        from concurrent.futures import ThreadPoolExecutor
        
        client = BankClient(coordinator_host=self.host, coordinator_port=self.port)
        
        # Initialize account balances
        print("Initializing account balances...")
        response = client.initialize_accounts(10000)
        
        # Check initial balance
        print("\nChecking initial balance...")
        for account in ['a1', 'a2']:
            response = client.get_balance(account)
            if response['status'] == 'success':
                print(f"Account {account} balance: {response.get('balance')}")
        
        # Define transfer function
        def do_transfer(from_acc, to_acc, amount, delay=0):
            time.sleep(delay)
            print(f"Executing transfer: {from_acc} -> {to_acc} ({amount})...")
            response = client.transfer(from_acc, to_acc, amount)
            if response['status'] == 'success':
                print(f"Transfer succeeded: {from_acc} -> {to_acc} ({amount})")
            else:
                print(f"Transfer failed: {from_acc} -> {to_acc}: {response.get('message')}")
            
            # Immediately verify balance
            return response
        
        # Create concurrent transfers
        print("\nStarting 5 concurrent transfers...")
        
        # Create multiple concurrent transfers
        transfers = [
            ('a1', 'a2', 100, 0),
            ('a2', 'a1', 200, 0.1),
            ('a1', 'a2', 300, 0.2),
            ('a2', 'a1', 400, 0.3),
            ('a1', 'a2', 500, 0.4)
        ]
        
        # Use thread pool instead of directly creating threads
        results = []
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            # Submit tasks to thread pool
            futures = [executor.submit(do_transfer, from_acc, to_acc, amount, delay) 
                      for from_acc, to_acc, amount, delay in transfers]
            
            # Collect results
            for future in futures:
                results.append(future.result())
        
        print("\nAll concurrent transfers completed")
        
        # Verify final results
        expected_changes = {'a1': 0, 'a2': 0}
        success_count = 0
        
        for i, (from_acc, to_acc, amount, _) in enumerate(transfers):
            if results[i]['status'] == 'success':
                expected_changes[from_acc] -= amount
                expected_changes[to_acc] += amount
                success_count += 1
        
        print(f"\nSuccessfully completed transfer count: {success_count}/{len(transfers)}")
        print("Expected account changes:")
        for account, change in expected_changes.items():
            print(f"Account {account} expected change: {change}")
        
        # Check final balance
        print("\nChecking final balance...")
        balances = {}
        for account in ['a1', 'a2']:
            response = client.get_balance(account)
            if response['status'] == 'success':
                balances[account] = response.get('balance')
                print(f"Account {account} balance: {balances[account]}")
        
        # Verify balance is as expected
        if len(balances) == 2:
            a1_change = balances['a1'] - 10000
            a2_change = balances['a2'] - 10000
            print(f"\nAccount a1 actual change: {a1_change}")
            print(f"Account a2 actual change: {a2_change}")
            
            if a1_change == expected_changes['a1'] and a2_change == expected_changes['a2']:
                print("\n✅ Verification succeeded: Balance change matches expected!")
            else:
                print("\n❌ Verification failed: Balance change does not match expected!")
                print(f"a1 expected change {expected_changes['a1']}, actual change {a1_change}")
                print(f"a2 expected change {expected_changes['a2']}, actual change {a2_change}")

def print_menu():
    """Print demonstration menu"""
    print("\n=== Distributed Bank System Demonstration ===")
    print("1. Interactive Failure Recovery Demo")
    print("2. Concurrent Transfer Demo")
    print("3. Check Node Status")
    print("4. Simulate Node Failure")
    print("5. Recover Failed Node")
    print("6. Massive Concurrent Transfers Test (100 transfers)")
    print("0. Exit")
    print("Please select demonstration scenario: ", end="")

def main():
    # If command line arguments specify coordinator address and port, use specified values
    host = COORDINATOR_HOST
    port = COORDINATOR_PORT
    
    if len(sys.argv) > 1:
        host = sys.argv[1]
    if len(sys.argv) > 2:
        port = int(sys.argv[2])
    
    print(f"Connecting to coordinator: {host}:{port}")
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
                node_id = input("Please enter the node ID to check: ").strip()
                demo.check_node_status(node_id)
            elif choice == '4':
                node_id = input("Please enter the node ID to simulate failure: ").strip()
                demo.simulate_node_failure(node_id)
            elif choice == '5':
                node_id = input("Please enter the node ID to recover: ").strip()
                demo.recover_node(node_id)
            elif choice == '6':
                num_transfers = input("请输入要执行的转账次数 [默认100]: ").strip()
                if not num_transfers:
                    num_transfers = 100
                else:
                    num_transfers = int(num_transfers)
                print(f"执行{num_transfers}次大量并发转账测试...")
                run_massive_transfers(host, port, num_transfers)
            else:
                print("Invalid choice, please try again")
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error occurred: {e}")
    
    print("Demonstration ended")

if __name__ == "__main__":
    main() 