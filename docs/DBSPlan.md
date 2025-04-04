# Week 5 Project Proposal: Distributed Banking System

## Application Overview

Our project implements a Distributed Banking System (DBS) that allows for secure and consistent money transfers between accounts across distributed nodes. The system demonstrates core distributed systems concepts including distributed communication, concurrency control, consistency management, and fault tolerance.

The application's primary purpose is to maintain accurate account balances while handling concurrent transfer operations in a distributed environment, ensuring that all transactions remain atomic and consistent even in the presence of node failures or network issues.

The Distributed Banking System addresses several key challenges in distributed computing:

1. **State Consistency**: Ensuring that account balances remain consistent across all nodes, even when multiple transactions are executed concurrently.
2. **Fault Resilience**: Maintaining system functionality and data integrity even when nodes fail or network partitions occur.
3. **Transaction Atomicity**: Guaranteeing that money transfers either complete entirely or fail entirely, with no partial execution that could lead to lost funds.
4. **System Availability**: Providing continuous service availability through redundancy and failover mechanisms.

### Project Scope

The scope of our project encompasses the following key components and functionalities:

* **Account Management**:
  * Creation, retrieval, and updating of bank accounts across distributed nodes
  * Persistent storage of account balances and transaction histories
  * Authentication and authorization for account operations (basic implementation)

* **Transaction Processing**:
  * Secure money transfers between accounts residing on different nodes
  * Validation of transfer requests (sufficient funds, valid accounts)
  * Transaction logging with comprehensive audit trails
  * Mechanisms to prevent double-spending and race conditions

* **Fault Tolerance**:
  * Redundant storage through primary-backup replication
  * Automated failure detection and recovery procedures
  * Robust handling of network partitions and message loss
  * State synchronization after node recovery

* **System Monitoring**:
  * Real-time tracking of node status and health
  * Visualization of transaction flows and system state
  * Performance metrics collection and reporting

### Use Cases

The system is designed to support the following primary use cases:

1. **Account Creation and Management**: Users can create new bank accounts, check balances, and view transaction histories.

2. **Money Transfers**: Users can transfer money between accounts, with the system ensuring that all transactions are executed atomically and consistently.

3. **System Administration**: Administrators can monitor system health, restart failed nodes, and verify system integrity.

4. **Failure Recovery**: The system can automatically recover from various failure scenarios, including node crashes and network partitions.

## System Design

### Architecture Diagram

```
+----------------+      +----------------+      +----------------+
|  Account Node  | <--> | Transaction    | <--> |  Account Node  |
|  (Account A)   |      | Coordinator    |      |  (Account B)   |
+----------------+      +----------------+      +----------------+
        ^                       ^                       ^
        |                       |                       |
        v                       v                       v
+----------------+      +----------------+      +----------------+
|  Backup Node   |      |  Registry      |      |  Backup Node   |
|  (Account A)   |      |  Service       |      |  (Account B)   |
+----------------+      +----------------+      +----------------+
```

### Node Structure and Responsibilities

Our system consists of four main node types that work together to provide a robust distributed banking service:

#### 1. Account Nodes

Account nodes serve as the primary repositories for individual bank accounts. Each account node:

* Maintains the authoritative state of one or more bank accounts
* Processes direct account operations (balance queries, history retrieval)
* Participates in distributed transactions as resource managers
* Implements locking mechanisms for concurrency control
* Performs validation of transaction operations
* Manages a transaction log for recovery purposes

The internal structure of an account node includes several key components:

* **Account Manager**: Handles CRUD operations on account records
* **Lock Manager**: Implements the two-phase locking protocol
* **Transaction Participant**: Executes the 2PC protocol steps
* **Replication Manager**: Synchronizes state with backup nodes
* **Communication Module**: Handles network communication with other nodes

#### 2. Transaction Coordinator Node

The transaction coordinator node orchestrates distributed transactions across multiple account nodes. Its responsibilities include:

* Receiving and validating transfer requests from clients
* Initiating and coordinating the two-phase commit protocol
* Managing transaction timeouts and handling failures
* Maintaining a transaction log for recovery purposes
* Coordinating with the registry service for node discovery

The coordinator implements several crucial mechanisms:

* **Transaction Manager**: Tracks the state of all active transactions
* **2PC Protocol Handler**: Implements the coordinator-side logic of 2PC
* **Recovery Manager**: Handles coordinator failures and restart recovery
* **Client Interface**: Provides an API for clients to initiate transfers

#### 3. Backup Nodes

Backup nodes provide redundancy and fault tolerance by maintaining replicas of primary account node data. Each backup node:

* Maintains a synchronized copy of account data from its primary node
* Monitors the health of its primary node through heartbeats
* Takes over as primary when the original primary node fails
* Participates in recovery procedures after failures
* Re-synchronizes state when a failed primary node recovers

#### 4. Registry Service

The registry service acts as a central directory for the entire distributed system, with responsibilities including:

* Tracking all active nodes and their current roles (primary/backup)
* Facilitating node discovery by providing location information
* Monitoring global system health through aggregated heartbeats
* Supporting dynamic addition and removal of nodes
* Providing configuration data to newly joined nodes

### Data Organization and Storage

Each node stores its data locally in JSON format, providing a lightweight and human-readable storage solution that simplifies debugging and development:

1. **Account Data**:
   ```json
   {
     "account_id": "A12345",
     "balance": 5000.00,
     "owner_name": "John Doe",
     "creation_date": "2025-01-15T10:30:45Z",
     "last_updated": "2025-03-18T14:22:33Z",
     "transaction_history": [
       {"id": "T001", "type": "DEPOSIT", "amount": 2000.00, "timestamp": "2025-01-15T10:35:12Z"},
       {"id": "T002", "type": "TRANSFER_OUT", "amount": 500.00, "destination": "B67890", "timestamp": "2025-02-10T09:15:30Z"}
     ]
   }
   ```

2. **Transaction Logs**:
   ```json
   {
     "transaction_id": "TX78901",
     "source_account": "A12345",
     "destination_account": "B67890",
     "amount": 500.00,
     "status": "COMMITTED",
     "start_time": "2025-03-18T14:20:10Z",
     "completion_time": "2025-03-18T14:20:15Z",
     "coordinator_id": "COORD-1",
     "participants": ["NODE-A", "NODE-B"],
     "2pc_logs": [
       {"phase": "PREPARE", "time": "2025-03-18T14:20:11Z", "responses": {"NODE-A": "YES", "NODE-B": "YES"}},
       {"phase": "COMMIT", "time": "2025-03-18T14:20:14Z", "responses": {"NODE-A": "ACK", "NODE-B": "ACK"}}
     ]
   }
   ```

By storing data in these structured JSON formats, our system achieves:

* **Simplicity**: No external database dependencies simplify deployment
* **Readability**: Easy inspection and debugging of system state
* **Flexibility**: Schema can evolve as the system requirements change
* **Persistence**: Data survives node restarts through file-based storage

### Communication Protocol

Our system employs a direct TCP/IP socket-based communication protocol with JSON-formatted messages. This approach provides:

1. **Reliability**: TCP ensures reliable message delivery with automatic retransmission
2. **Simplicity**: Direct socket communication avoids dependencies on complex middleware
3. **Readability**: JSON-formatted messages are human-readable and easily debugged
4. **Extensibility**: Message formats can be extended without breaking compatibility

The protocol defines several message types:

* **Transaction Messages**:
  * `TransactionRequest`: Client-initiated request for a fund transfer
  * `PrepareRequest`: First phase of 2PC from coordinator to participants
  * `PrepareResponse`: Participant's vote on transaction preparedness
  * `CommitRequest`: Second phase commit instruction
  * `AbortRequest`: Second phase abort instruction
  * `AcknowledgmentMessage`: Confirmation of commit/abort receipt

* **Account Operation Messages**:
  * `BalanceRequest`: Query for current account balance
  * `HistoryRequest`: Query for transaction history
  * `AccountUpdateNotification`: Notification of account state changes

* **System Management Messages**:
  * `HeartbeatMessage`: Regular pulse to indicate node health
  * `NodeStatusUpdate`: Changes in node role or availability
  * `ReplicationMessage`: Updates sent from primary to backup nodes
  * `SynchronizationRequest`: Request for full state synchronization

## Distributed System Mechanisms

### Concurrency Control and Consistency Management

Our system implements sophisticated concurrency control and consistency management mechanisms to ensure data integrity even in the presence of simultaneous operations:

#### Two-Phase Locking (2PL)

The Two-Phase Locking protocol is central to our concurrency control strategy. It operates in two distinct phases:

1. **Growing Phase**:
   * The transaction coordinator requests locks on all accounts involved in a transfer
   * Locks are acquired in a globally defined order (by account ID) to prevent deadlocks
   * If any lock cannot be acquired within a timeout period, the transaction is aborted
   * No locks are released during this phase

2. **Shrinking Phase**:
   * After all necessary locks are acquired and the transaction is complete
   * All locks are released, allowing other transactions to proceed
   * Once a lock is released, no new locks can be acquired by the same transaction

This approach prevents issues such as:

* **Lost Updates**: When two transactions attempt to modify the same account simultaneously
* **Inconsistent Reads**: When a transaction reads data that is being modified by another transaction
* **Phantom Reads**: When a transaction retrieves a set of records that is subsequently modified by another transaction

To further enhance our concurrency control, we implement:

* **Lock Timeouts**: Preventing indefinite waiting if a resource is unavailable
* **Global Lock Ordering**: Acquiring locks in a consistent order to prevent deadlocks
* **Lock Escalation**: Using read locks for query operations and write locks for modifications
* **Lock Manager**: A dedicated component on each node that tracks lock status

#### Two-Phase Commit Protocol (2PC)

To ensure atomicity across distributed transactions, we implement the Two-Phase Commit protocol:

1. **Preparation Phase**:
   * The coordinator sends a `PrepareRequest` to all participants (account nodes)
   * Each participant checks if it can commit the transaction (sufficient funds, valid account)
   * Participants prepare the transaction by reserving funds but not updating balances
   * Each participant responds with a `PrepareResponse` (YES/NO)
   * Participants that vote YES enter a prepared state where they must honor the coordinator's final decision

2. **Commit/Abort Phase**:
   * If all participants vote YES, the coordinator sends a `CommitRequest`
   * If any participant votes NO, the coordinator sends an `AbortRequest`
   * Upon receiving a commit, participants finalize the transaction and release locks
   * Upon receiving an abort, participants roll back any preparatory work and release locks
   * All participants send acknowledgments back to the coordinator

The protocol ensures that:

* All participants agree on the transaction outcome (commitment or abortion)
* No funds are lost or duplicated during the transfer process
* The system can recover from failures during the transaction

To enhance the reliability of 2PC, we implement:

* **Timeout Management**: Handling cases where nodes fail to respond
* **Transaction Logging**: Recording all protocol steps for recovery purposes
* **Heuristic Resolution**: Procedures for resolving inconsistencies in rare failure scenarios

### Fault Tolerance

Our system implements comprehensive fault tolerance mechanisms to ensure reliable operation even when components fail:

#### Heartbeat Mechanism

The heartbeat mechanism serves as the foundation of our failure detection system:

* **Regular Pulse**: Each node sends heartbeat messages to related nodes every 2 seconds
* **Failure Detection**: If no heartbeat is received for 3 consecutive intervals (6 seconds), a node is suspected of failure
* **Confirmation Process**: Before declaring a node failed, the system attempts direct communication
* **Health Metrics**: Heartbeats include basic performance metrics for monitoring purposes

The registry service aggregates heartbeat information to maintain a global view of system health, enabling:

* **Early Warning**: Detection of nodes with degrading performance
* **Failure Visualization**: Dashboard showing system state and failed components
* **Historical Analysis**: Recording of failure patterns for system improvement

#### Primary-Backup Replication

Our primary-backup replication strategy provides redundancy while maintaining clear authority over data:

1. **Assignment Process**:
   * Each account node is assigned one designated backup node
   * The registry service manages these assignments and updates them when nodes join or leave
   * Backup assignments are distributed to balance the load across nodes

2. **Replication Methods**:
   * **Synchronous Updates**: Critical operations (e.g., balance changes) are synchronously replicated
   * **Asynchronous Updates**: Non-critical operations (e.g., accessing transaction history) use asynchronous replication
   * **Full Synchronization**: Periodically, the entire account state is synchronized to detect any inconsistencies

3. **Update Protocol**:
   * Primary node sends update to backup and waits for acknowledgment
   * Updates include sequence numbers to detect missed messages
   * Backup applies updates only if they arrive in order
   * Periodic reconciliation to detect and fix any inconsistencies

The primary-backup approach offers several advantages:

* **Clear Authority**: One node is always authoritative for each account
* **Simplicity**: Straightforward replication without complex consensus
* **Low Overhead**: Minimal communication compared to multi-copy replication
* **Fast Recovery**: Quick promotion of backup to primary role

#### Failure Recovery

When failures occur, our system employs a structured recovery process:

1. **Primary Node Failure**:
   * Failure detected through missed heartbeats
   * Backup node promotes itself to primary status
   * Registry service updates its directory with the new primary
   * New primary registers itself and starts accepting client requests
   * A new backup is assigned to the new primary

2. **Backup Node Failure**:
   * Primary continues operation uninterrupted
   * Registry service assigns a new backup node
   * Primary initiates full state synchronization with the new backup

3. **Coordinator Failure**:
   * In-progress transactions enter a resolution protocol
   * Participants use timeout mechanisms to detect coordinator failure
   * For transactions in prepared state, participants communicate to reach consensus
   * Registry service assigns a new coordinator node

4. **Registry Service Failure**:
   * System continues operation with last known configuration
   * A backup registry service takes over (if implemented)
   * Nodes re-register with the new registry service

Our recovery mechanisms ensure:

* **No Single Point of Failure**: System continues to operate despite any single node failure
* **Minimal Downtime**: Fast recovery processes minimize service interruption
* **Data Integrity**: No loss of committed transactions during failover
* **Automatic Recovery**: Minimal manual intervention required

### Scalability Considerations

While our current implementation focuses on a contained demonstration system, we have designed with future scalability in mind:

#### Horizontal Scaling

The system architecture supports horizontal scaling through:

1. **Sharding**: Distributing accounts across nodes based on account ID ranges:
   * Each shard handles a specific range of account IDs
   * Transfers within a shard require only local coordination
   * Cross-shard transfers use the distributed transaction protocol
   * Dynamic resharding capabilities can balance load as the system grows

2. **Hierarchical Coordination**:
   * Multiple coordinator nodes managing different account groups
   * Meta-coordinators for cross-group transactions
   * Load balancing of transaction processing across coordinators
   * Coordinator specialization for different transaction types

3. **Gossip Protocol for Large Networks**:
   * Efficient propagation of node status information
   * Logarithmic scaling of control messages as the network grows
   * Eventual consistency for non-critical system state
   * Probabilistic dissemination algorithms for reliability

#### Consistency Optimizations

For larger scale deployments, we would implement:

1. **Paxos/Raft for Consensus**:
   * Replacing simple primary-backup with consensus protocols
   * Supporting larger replication groups with stronger guarantees
   * Automated leader election for coordinator roles
   * Majority-based decision making for higher availability

2. **Read Optimizations**:
   * Read-only replicas for scaling query traffic
   * Snapshot isolation for analytical operations
   * Caching of frequently accessed account information
   * Eventual consistency options for non-critical reads

3. **Transaction Optimizations**:
   * Batching of related transactions for efficiency
   * Partitioned two-phase commit for reduced coordination
   * Optimistic concurrency control for low-contention scenarios
   * Specialized protocols for common transaction patterns

## Testing Plan

Our comprehensive testing strategy ensures system reliability and correctness across multiple dimensions:

### Functional Testing

Functional testing verifies that the basic operations of the system work correctly:

1. **Account Operations Testing**:
   * **Account Creation**: Verify that new accounts are created with correct initial state
   * **Balance Queries**: Ensure that balance information is correctly retrieved
   * **Transaction History**: Validate that transaction records are properly maintained
   * **Account Updates**: Confirm that account information can be properly modified

2. **Transaction Testing**:
   * **Basic Transfers**: Verify successful transfers between accounts on different nodes
   * **Validation Logic**: Test rejection of invalid transfers (insufficient funds, non-existent accounts)
   * **Transaction Idempotency**: Ensure duplicate transaction requests are properly handled
   * **Transaction Atomicity**: Verify that partial transfers never occur

3. **Edge Case Testing**:
   * **Boundary Conditions**: Test behavior with zero balances, maximum transaction values
   * **Concurrent Self-Transfers**: Attempt transfers from an account to itself during other operations
   * **Multi-Account Transfers**: Test transfers involving multiple source or destination accounts
   * **Transaction Rollbacks**: Verify proper restoration of state after failed transactions

### Concurrency Testing

Concurrency testing ensures that the system handles simultaneous operations correctly:

1. **Stress Testing Scenarios**:
   * **Many-to-One**: Multiple sources transferring to one destination
   * **One-to-Many**: One source transferring to multiple destinations
   * **Many-to-Many**: Complex transfer patterns among multiple accounts
   * **Circular Transfers**: A chain of transfers that forms a cycle

2. **Race Condition Detection**:
   * **Intentional Race Conditions**: Specially crafted test cases to trigger potential races
   * **Timing Variation**: Running tests with different timing patterns to expose issues
   * **Thread Sanitizers**: Using specialized tools to detect thread interaction problems
   * **State Validation**: Rigorous checking of system state consistency after concurrent operations

3. **Lock Verification**:
   * **Deadlock Detection**: Tests designed to trigger potential deadlock scenarios
   * **Lock Timeout Testing**: Verifying that lock acquisition timeouts work correctly
   * **Lock Release Verification**: Ensuring locks are properly released after transactions
   * **Lock Escalation Testing**: Validating the interaction between read and write locks

### Fault Tolerance Testing

Fault tolerance testing validates the system's ability to handle and recover from failures:

1. **Simulated Failure Scenarios**:
   * **Crash Failures**: Abrupt termination of nodes during various operations
   * **Network Partitions**: Temporary inability of nodes to communicate with each other
   * **Message Loss**: Selective dropping of messages between nodes
   * **Performance Degradation**: Nodes experiencing significant slowdowns

2. **Recovery Process Validation**:
   * **Backup Promotion**: Verifying backup nodes properly take over for failed primaries
   * **State Reconstruction**: Validating that recovered nodes rebuild their state correctly
   * **Registry Reconciliation**: Testing registry service's ability to track node changes
   * **Transaction Resolution**: Ensuring in-flight transactions are properly resolved

3. **Chaos Testing**:
   * **Random Node Failures**: Introducing random node crashes during system operation
   * **Cascading Failures**: Testing scenarios where one failure triggers others
   * **Mixed Failure Modes**: Combining different types of failures simultaneously
   * **Recovery Under Load**: Validating recovery processes while the system is under stress

### Performance Testing

Our performance testing evaluates the system's efficiency and scalability:

1. **Throughput Measurement**:
   * **Transaction Rate**: Maximum number of transactions processed per second
   * **Scaling Behavior**: How throughput changes with increasing node count
   * **Workload Patterns**: Performance under different transaction patterns
   * **Resource Utilization**: CPU, memory, network, and disk usage during operation

2. **Latency Analysis**:
   * **End-to-End Latency**: Time from transaction request to completion
   * **Component Breakdown**: Identifying which components contribute most to latency
   * **Tail Latency**: Examination of worst-case response times
   * **Latency Under Load**: How response times change as system load increases

3. **Bottleneck Identification**:
   * **Resource Contention**: Finding shared resources that limit performance
   * **Communication Patterns**: Analyzing network traffic for optimization opportunities
   * **Serialization Points**: Identifying workflow steps that cannot be parallelized
   * **Component Profiling**: Detailed performance analysis of key system components

### Testing Tools and Infrastructure

To support our testing strategy, we will utilize:

1. **Testing Frameworks**:
   * **JUnit/PyTest**: For unit and integration testing
   * **Load Generation Tools**: For simulating client requests at volume
   * **Fault Injection Frameworks**: For simulating various failure scenarios
   * **Distributed Tracing**: For tracking request flow through the system

2. **Monitoring and Validation**:
   * **Log Analysis**: Automated parsing and verification of system logs
   * **State Consistency Checkers**: Tools to verify data consistency across nodes
   * **Performance Metrics Collection**: Gathering and analyzing system performance data
   * **Visualization Dashboards**: For real-time monitoring during tests

## Implementation Plan

Our implementation strategy follows a phased approach, starting with core functionality and progressively adding more advanced features:

### Base Functionality (Core Requirements)

1. **Node Implementation**:
   * **Account Node**: Complete account data management with JSON persistence
   * **Transaction Coordinator**: Basic coordination capabilities for transfers
   * **Registry Service**: Simple node registration and discovery
   * **Backup Nodes**: Basic replication from primary nodes

2. **Core Protocols**:
   * **TCP/IP Communication**: Socket-based messaging between nodes
   * **Transaction Protocol**: Basic transaction workflow implementation
   * **Two-Phase Locking**: Concurrency control for account access
   * **Two-Phase Commit**: Atomic transaction processing across nodes

3. **Fault Tolerance Basics**:
   * **Heartbeat Mechanism**: Simple failure detection
   * **Primary-Backup Replication**: Basic state replication
   * **Failure Recovery**: Elementary node recovery procedures

4. **Client Interface**:
   * **Command-Line Interface**: For initiating transfers and queries
   * **Basic Visualization**: Simple display of system state
   * **Transaction Monitoring**: Tracking the status of transactions

This core functionality will demonstrate the essential distributed systems concepts required by the project:

* Distributed communication through the messaging system
* Concurrency control via two-phase locking
* Consistency management using two-phase commit
* Basic fault tolerance with replication and recovery

### Stretch Goals (If Time Permits)

If we complete the base functionality ahead of schedule, we will implement these advanced features:

1. **Enhanced Visualization and Monitoring**:
   * **Interactive Dashboard**: Web-based UI for system monitoring
   * **Real-Time Transaction Visualization**: Graphical display of transactions in progress
   * **Performance Metrics**: Comprehensive statistics collection and display
   * **Alerting System**: Notifications for system issues

2. **Advanced Failure Scenarios**:
   * **Byzantine Fault Handling**: Dealing with nodes that behave incorrectly
   * **Network Partition Recovery**: Sophisticated reconciliation after network partitions
   * **Coordinator Failure Recovery**: More robust handling of coordinator failures
   * **Registry Service Redundancy**: Fault-tolerant registry service

3. **Dynamic System Management**:
   * **Runtime Node Addition**: Adding new nodes without system restart
   * **Controlled Node Removal**: Graceful removal of nodes from the system
   * **Dynamic Load Balancing**: Redistribution of accounts for balanced load
   * **Configuration Updates**: Changing system parameters without restart

4. **Transaction Enhancements**:
   * **Scheduled Transactions**: Transfers executed at specified future times
   * **Conditional Transfers**: Transfers that execute only if conditions are met
   * **Multi-Account Transactions**: Operations involving more than two accounts
   * **Transaction Rollback**: More sophisticated recovery from failed transactions

5. **Performance Optimizations**:
   * **Protocol Efficiency Improvements**: Reducing message overhead
   * **Caching Strategies**: Intelligent caching of frequently accessed data
   * **Batch Processing**: Grouping related operations for efficiency
   * **Asynchronous Processing**: Using async patterns for non-critical operations

### Development Methodology

We will follow these development practices:

1. **Iterative Development**:
   * Implementing features in small, testable increments
   * Regular integration of components to validate system behavior
   * Continuous testing throughout the development process
   * Weekly demonstrations of progress

2. **Testing-Driven Development**:
   * Writing tests before implementing features
   * Automated testing for regression prevention
   * Comprehensive test coverage for core functionality
   * Special attention to edge cases and failure scenarios

3. **Documentation**:
   * Detailed API documentation for all components
   * Architecture documentation explaining design decisions
   * Operational guides for running and testing the system
   * Implementation notes for future reference

## Timeline

Our implementation will follow this schedule:

- **Week 5-6: Core Node Implementation and Communication Protocol**
  * Account node with basic functionality
  * Transaction coordinator with 2PC implementation
  * Socket-based communication between nodes
  * Registry service for node discovery
  * Initial unit tests for components

- **Week 6-7: Concurrency Control and Consistency Mechanisms**
  * Two-phase locking implementation
  * Two-phase commit protocol
  * Transaction logging for recovery
  * Integration tests for distributed transactions
  * Basic client interface

- **Week 7-8: Fault Tolerance and Testing Framework**
  * Primary-backup replication
  * Heartbeat mechanism for failure detection
  * Node failure and recovery procedures
  * Comprehensive testing framework
  * System monitoring tools

- **Week 8: Final Testing, Optimization, and Presentation Preparation**
  * Performance testing and optimization
  * Bug fixes and stability improvements
  * Final documentation completion
  * Demonstration scenario preparation
  * Presentation development

This timeline ensures that we focus first on the core distributed systems concepts required by the project, and then enhance the system with additional features as time permits.

The phased approach allows us to have a working system at each stage, with increasing sophistication and robustness as we progress.
