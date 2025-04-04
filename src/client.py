import socket
import json
import sys

class BankClient:
    def __init__(self, coordinator_host='172.20.10.2', coordinator_port=5010):
        self.coordinator_host = coordinator_host
        self.coordinator_port = coordinator_port
    
    def send_request(self, request):
        try:
            print(f"Connecting to coordinator at {self.coordinator_host}:{self.coordinator_port}...")
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5)  # Set timeout to 5 seconds
                s.connect((self.coordinator_host, self.coordinator_port))
                print("Connection established, sending request...")
                s.send(json.dumps(request).encode('utf-8'))
                print("Request sent, waiting for response...")
                response = json.loads(s.recv(4096).decode('utf-8'))
                print("Response received.")
                return response
        except socket.timeout:
            print(f"Error: Connection to {self.coordinator_host}:{self.coordinator_port} timed out")
            return {'status': 'error', 'message': 'Connection timed out'}
        except ConnectionRefusedError:
            print(f"Error: Connection to {self.coordinator_host}:{self.coordinator_port} refused")
            print("Make sure the coordinator is running and the port is correct.")
            return {'status': 'error', 'message': 'Connection refused'}
        except Exception as e:
            print(f"Error sending request: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def list_accounts(self):
        request = {
            'command': 'list_accounts'
        }
        return self.send_request(request)
    
    def transfer(self, from_account, to_account, amount):
        request = {
            'command': 'transfer',
            'from': from_account,
            'to': to_account,
            'amount': amount
        }
        return self.send_request(request)
    
    def initialize_accounts(self, amount=10000):
        request = {
            'command': 'init_accounts',
            'amount': amount
        }
        return self.send_request(request)
    
    def get_balance(self, account_id):
        request = {
            'command': 'get_balance',
            'account_id': account_id
        }
        return self.send_request(request)

def print_help():
    print("\nBank Client Commands:")
    print("  list                    - List all available accounts")
    print("  transfer <from> <to> <amount> - Transfer money between accounts (e.g., transfer a1 a2 100)")
    print("  init <amount>           - Initialize all accounts with amount (default: 10000)")
    print("  balance <account_id>    - Get the balance of an account")
    print("  exit                    - Exit the client")
    print("  help                    - Show this help message")

def main():
    if len(sys.argv) > 1:
        coordinator_port = int(sys.argv[1])
    else:
        coordinator_port = 5010
    
    client = BankClient(coordinator_port=coordinator_port)
    print(f"Bank Client initialized to connect to coordinator on port {coordinator_port}")
    print(f"Note: This doesn't mean a connection has been established yet.")
    print_help()
    
    while True:
        try:
            command = input("\nEnter command: ").strip().split()
            
            if not command:
                continue
            
            if command[0] == 'exit':
                break
            
            elif command[0] == 'help':
                print_help()
            
            elif command[0] == 'list':
                response = client.list_accounts()
                if response['status'] == 'success':
                    print("Available accounts:")
                    for account in response['accounts']:
                        print(f"  - {account}")
                else:
                    print(f"Error: {response.get('message', 'Unknown error')}")
            
            elif command[0] == 'transfer':
                if len(command) < 4:
                    print("Error: transfer requires <from> <to> <amount>")
                    continue
                
                from_account = command[1]
                to_account = command[2]
                
                try:
                    amount = float(command[3])
                except ValueError:
                    print("Error: amount must be a number")
                    continue
                
                print(f"Transferring {amount} from {from_account} to {to_account}...")
                response = client.transfer(from_account, to_account, amount)
                
                if response['status'] == 'success':
                    print(f"Success: {response.get('message', 'Transfer completed')}")
                else:
                    print(f"Error: {response.get('message', 'Transfer failed')}")
            
            elif command[0] == 'init':
                amount = 10000
                if len(command) > 1:
                    try:
                        amount = float(command[1])
                    except ValueError:
                        print("Error: amount must be a number")
                        continue
                
                print(f"Initializing all accounts with balance {amount}...")
                response = client.initialize_accounts(amount)
                
                if response['status'] == 'success':
                    print(f"Success: {response.get('message', 'Accounts initialized')}")
                else:
                    print(f"Error: {response.get('message', 'Initialization failed')}")
            
            elif command[0] == 'balance':
                if len(command) < 2:
                    print("Error: balance requires <account_id>")
                    continue
                
                account_id = command[1]
                print(f"Fetching balance for account {account_id}...")
                response = client.get_balance(account_id)
                
                if response['status'] == 'success':
                    print(f"Account {account_id} balance: {response.get('balance', 'N/A')}")
                else:
                    print(f"Error: {response.get('message', 'Could not get balance')}")
            
            else:
                print(f"Unknown command: {command[0]}")
                print_help()
        
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error: {e}")
    
    print("Exiting Bank Client")

if __name__ == "__main__":
    main()
