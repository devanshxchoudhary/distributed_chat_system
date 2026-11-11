# Distributed Chat System

A fault-tolerant, multi-node distributed chat application built with Python. The system uses leader election, replicated logs, quorum-based commitment, and dynamic cluster membership to maintain consistent message ordering across multiple nodes.

The project demonstrates fundamental distributed systems concepts through a working chat application that can run across multiple processes and physical machines, including Windows and macOS.

## Key Features

- **Leader Election:** Automatically elects a cluster leader using randomized election timeouts and voting.
- **Message Replication:** Replicates chat messages across distributed nodes.
- **Quorum-Based Consensus:** Commits messages only after they are durably stored by a majority of voters.
- **Fault Tolerance:** Automatically elects a new leader when the current leader crashes.
- **Message Ordering:** Maintains consistent, sequential message delivery across connected clients.
- **Dynamic Membership:** Supports joining new nodes as learners and promoting them to voting members.
- **Persistent Storage:** Uses SQLite databases to maintain individual node logs and recover after restarts.
- **Client Reconnection:** Supports multiple WebSocket server endpoints for reconnecting through available nodes.
- **Cross-Platform Support:** Runs on Windows and macOS.

## Architecture

The system consists of three main components:

1. **Distributed Nodes:** Independent node processes responsible for elections, replication, and maintaining the cluster.
2. **Chat Clients:** Users connect through WebSocket endpoints to send and receive messages.
3. **Persistent Storage:** Each node maintains its own SQLite database containing its local replicated state.

### Message Flow

1. A client sends a message to any available node.
2. The receiving node forwards the message to the current leader if necessary.
3. The leader replicates the message to the other nodes.
4. The message is committed after receiving confirmation from a voting majority.
5. Committed messages are delivered to connected clients in the same order.

### Cluster Configuration

The initial cluster consists of three voting nodes:

| Node | Role | Default TCP Port | WebSocket Port |
|---|---|---:|---:|
| Node A | Voter | 9001 | 8001 |
| Node B | Voter | 9002 | 8002 |
| Node C | Voter | 9003 | 8003 |
| Node D | Joining node | 9004 | 8004 |

Node D initially joins as a learner and can subsequently be promoted to a voting member.

## Technology Stack

- **Language:** Python 3.12
- **Package Management:** uv
- **Networking:** TCP and WebSockets
- **Database:** SQLite
- **Configuration:** TOML
- **Testing:** pytest
- **Code Quality:** Ruff and mypy

## Getting Started

### Prerequisites

- Python 3.12
- Git
- uv package manager
- Visual Studio Code (recommended)

### 1. Clone the repository

```bash
git clone https://github.com/YOUR-USERNAME/distributed-chat.git
cd distributed-chat
```

### 2. Install uv

**Windows (PowerShell):**

```powershell
winget install astral-sh.uv
```

**macOS (Terminal):**

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
source "$HOME/.local/bin/env"
```

If uv is already installed, skip this step.

### 3. Install dependencies

```bash
uv python install 3.12
uv sync
```

This installs the required dependencies using the project's `uv.lock` file.

### 4. Run automated tests

```bash
uv run pytest -q
```

## Running the Application

### Option 1: Run all three nodes on one machine

First, configure the node addresses in:

- `config/node-a.toml`
- `config/node-b.toml`
- `config/node-c.toml`
- `config/node-d.toml`

For a local setup, use `127.0.0.1` for the advertised addresses and seeds.

Start each node in a separate terminal.

**macOS:**

```bash
./scripts/start-node.sh a
./scripts/start-node.sh b
./scripts/start-node.sh c
```

**Windows PowerShell:**

```powershell
.\scripts\start-node.ps1 a
.\scripts\start-node.ps1 b
.\scripts\start-node.ps1 c
```

Wait for the cluster to elect a leader.

### 5. Start chat clients

Open separate terminal windows for the clients.

**Client Alice:**

```bash
uv run python -m dchat.client --user alice --server ws://127.0.0.1:8001/ws --server ws://127.0.0.1:8002/ws --server ws://127.0.0.1:8003/ws
```

**Client Bob:**

```bash
uv run python -m dchat.client --user bob --server ws://127.0.0.1:8002/ws --server ws://127.0.0.1:8003/ws
```

Send messages from both clients and verify that they receive the same messages with matching sequential message numbers.

## Running Across Two Machines

The application also supports a distributed deployment across two physical laptops.

Example deployment:

- **Laptop 1:** Node A and Node B.
- **Laptop 2:** Node C and Node D.

Both machines must be on a network that permits direct communication.

1. Find the Wi-Fi IPv4 address of each laptop.
2. Update the advertised addresses, seeds, and bootstrap voter configuration in all four TOML files on both machines.
3. Keep the initial voter configuration identical across Nodes A, B, and C.
4. Allow TCP ports `9001–9004` and WebSocket ports `8001–8004` through the firewall.
5. Start Nodes A and B on Laptop 1.
6. Start Node C on Laptop 2.
7. Wait for leader election and cluster stabilization.
8. Connect the chat clients using the actual laptop IP addresses.

**Important:** Never use `127.0.0.1` or `0.0.0.0` as an advertised address when communicating between different machines.

## Fault Tolerance Demonstration

One of the main objectives of this project is to demonstrate how a distributed cluster handles node failures.

### Leader Failure

1. Start the three-node cluster.
2. Identify the current leader.
3. Connect Alice and Bob.
4. Exchange messages and observe their sequence numbers.
5. Forcefully terminate the current leader.

**macOS:**

```bash
./scripts/kill-node.sh a
```

**Windows:**

```powershell
.\scripts\kill-node.ps1 a
```

Replace `a` with the actual leader's node identifier.

### Expected Behaviour

- The failed leader stops sending heartbeats.
- The remaining nodes detect the failure.
- A new election begins.
- Another node becomes leader.
- Clients can continue exchanging messages.
- The failed node can restart and catch up using its persisted SQLite database.

## Dynamic Node Membership

After Nodes A, B, and C are running and a stable leader has been elected, Node D can join the cluster.

```bash
./scripts/start-node.sh d
```

Node D initially joins as a learner.

The leader replicates the existing committed log to Node D. Once it catches up, the leader can promote it to a voting member.

This demonstrates dynamic membership changes without manually adding the new node to the initial bootstrap voter configuration.

## Testing and Code Quality

Run the following commands from the project root.

**Automated tests:**

```bash
uv run pytest -q
```

**Linting:**

```bash
uv run ruff check --fix .
```

**Formatting:**

```bash
uv run ruff format .
```

**Static type checking:**

```bash
uv run mypy src
```

### Test Coverage Areas

The test suite includes:

- Message framing and validation.
- Log replication and persistent storage.
- Leader election and quorum handling.
- Message ordering and deduplication.
- Cluster membership and learner promotion.
- Randomized node crashes and restarts.
- Network partitions and recovery.
- TCP and SQLite integration.
- WebSocket client sessions and reconnection.
- Leader failure and recovery across separate processes.

## Fault Tolerance Limitations

The system uses majority-based consensus.

| Cluster Size | Required Majority | Maximum Node Failures |
|---|---:|---:|
| 3 voters | 2 | 1 |
| 4 voters | 3 | 1 |

If a voting majority becomes unavailable, the cluster stops accepting committed writes to preserve consistency.

Current implementation limitations:

- No TLS or authentication.
- No node removal functionality.
- No snapshots or log compaction.
- New or restarted nodes catch up by replaying the replicated log.

## Project Objectives

This project explores practical implementations of distributed systems concepts, including:

- Distributed coordination.
- Leader election.
- Consensus and quorum management.
- Replicated state and log consistency.
- Fault detection and recovery.
- Dynamic cluster membership.
- Network communication.
- Persistent distributed storage.

## License

Add your preferred open-source license before distributing this project. 
