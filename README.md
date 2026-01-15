# Thunderbolt: Distributed Command Execution for Compute Clusters

Thunderbolt is a lightweight distributed execution framework for running commands across multiple nodes in parallel. Built with FastAPI and WebSockets, it provides a simple master-slave architecture for cluster management, monitoring, and automation.

## Features

- **Parallel Execution**: Run commands across multiple nodes simultaneously with aggregated results
- **Health Monitoring**: Automatic periodic health checks with configurable intervals and failure thresholds
- **WebSocket Communication**: Persistent bidirectional connections for low-latency command dispatch
- **Flexible Integration**: Use standalone or embed into existing FastAPI applications
- **Timeout Management**: Per-command timeout configuration with automatic process termination
- **Automatic Reconnection**: Slaves reconnect to master on connection loss with exponential backoff
- **Node Discovery**: Query connected nodes and their health status
- **Hostname-based Targeting**: Execute commands on specific nodes or all nodes

## Installation

```bash
git clone https://github.com/AnshKetchum/thunderbolt.git
cd thunderbolt
pip install -e .
```

## Quick Start

### Running the Master

Start the master node using the CLI:

```bash
thunderbolt-master --port 8000 --api-port 8001
```

Or programmatically:

```python
from thunderbolt.master import ThunderboltMaster

master = ThunderboltMaster(port=8000)
master.run(host="0.0.0.0", port=8001)
```

### Running Slave Nodes

Start slave nodes using the CLI:

```bash
thunderbolt-slave --master-ip 10.0.0.1 --port 8000 --hostname node-01
```

Or programmatically:

```python
from thunderbolt.slave import ThunderboltSlave

slave = ThunderboltSlave(
    master_ip="10.0.0.1",
    port=8000,
    hostname="node-01"
)
slave.run()
```

### Using the Client API

```python
from thunderbolt.api import ThunderboltAPI

with ThunderboltAPI(host="localhost", port=8001) as api:
    # List connected nodes
    nodes = api.list_nodes()
    print(f"Connected nodes: {nodes['total']}")
    
    # Run command on all nodes
    result = api.run_on_all_nodes("nvidia-smi --query-gpu=name --format=csv,noheader")
    for host, res in result['results'].items():
        if res['status'] == 'success':
            print(f"{host}: {res['stdout'].strip()}")
    
    # Run command on specific nodes
    result = api.run_command(
        command="df -h /workspace",
        nodes=["node-01", "node-02"],
        timeout=10
    )
```

## Architecture

Thunderbolt uses a master-slave architecture with WebSocket-based communication:
Here's a basic diagram of the architecture 

![Architecture Diagram](./docs/arch-diagram.jpg)

**Components:**

- **Master Node**: Central coordinator that manages slave connections via WebSocket (port 8000) and exposes a REST API (port 8001) for command execution
- **Slave Nodes**: Worker nodes that maintain persistent WebSocket connections and execute commands
- **Client API**: Python SDK for interacting with the master's REST API

**Communication Flow:**

1. Slaves establish WebSocket connections and register with the master
2. Master performs periodic health checks on all connected slaves
3. Clients send command execution requests to the master's REST API
4. Master dispatches commands to target slaves via WebSocket
5. Slaves execute commands asynchronously and return results
6. Master aggregates results and returns them to the client

## REST API Reference

### Base URL
```
http://<master-host>:<api-port>
```

Default ports: WebSocket on 8000, REST API on 8001

---

### GET /

Get master status information.

**Response:**
```json
{
  "message": "Master Command Runner",
  "connected_slaves": 5
}
```

---

### GET /health

Health check endpoint for the master service.

**Response:**
```json
{
  "status": "healthy",
  "connected_slaves": 5
}
```

---

### GET /nodes

List all connected slave nodes with their health status.

**Response:**
```json
{
  "total": 3,
  "nodes": [
    {
      "hostname": "node-01",
      "last_seen": "2024-01-14T10:30:45.123456",
      "failed_healthchecks": 0
    },
    {
      "hostname": "node-02",
      "last_seen": "2024-01-14T10:30:44.987654",
      "failed_healthchecks": 1
    }
  ]
}
```

---

### POST /run

Execute a command on specified nodes.

**Request:**
```json
{
  "command": "df -h /workspace",
  "nodes": ["node-01", "node-02"],
  "timeout": 30,
  "use_sudo": false
}
```

**Parameters:**
- `command` (string, required): Shell command to execute
- `nodes` (array, required): List of hostnames to target
- `timeout` (integer, optional): Command timeout in seconds (default: 30)
- `use_sudo` (boolean, optional): Run with sudo privileges (default: false)

**Response:**
```json
{
  "command": "df -h /workspace",
  "total_nodes": 2,
  "responses_received": 2,
  "results": {
    "node-01": {
      "type": "command_result",
      "hostname": "node-01",
      "command_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
      "stdout": "Filesystem      Size  Used Avail Use% Mounted on\n/dev/sda1       100G   45G   55G  45% /workspace\n",
      "stderr": "",
      "returncode": 0,
      "status": "success"
    },
    "node-02": {
      "type": "command_result",
      "hostname": "node-02",
      "command_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
      "stdout": "Filesystem      Size  Used Avail Use% Mounted on\n/dev/sdb1       200G   80G  120G  40% /workspace\n",
      "stderr": "",
      "returncode": 0,
      "status": "success"
    }
  }
}
```

**Status Values:**
- `success`: Command executed successfully (returncode 0)
- `error`: Command failed (non-zero returncode)
- `timeout`: Command exceeded timeout limit

**Error Response (404):**
```json
{
  "detail": "Nodes not found: ['node-99']"
}
```

## Python Client API

### ThunderboltAPI

Main client class for interacting with the Thunderbolt master.

#### Constructor

```python
ThunderboltAPI(host: str = "localhost", port: int = 8001, base_path: str = "")
```

**Parameters:**
- `host`: Master API hostname or IP address
- `port`: Master API port (default: 8001)
- `base_path`: Base path prefix for API endpoints (e.g., "/thunderbolt")

---

#### list_nodes()

Get list of all connected nodes.

**Returns:** Dictionary with node information

**Example:**
```python
nodes = api.list_nodes()
print(f"Connected nodes: {nodes['total']}")
for node in nodes['nodes']:
    print(f"  {node['hostname']} - Failed checks: {node['failed_healthchecks']}")
```

---

#### run_command(command, nodes, timeout=30, use_sudo=False)

Execute a command on specific nodes.

**Parameters:**
- `command` (str): Shell command to execute
- `nodes` (List[str]): List of hostnames to target
- `timeout` (int): Command timeout in seconds (default: 30)
- `use_sudo` (bool): Run with sudo privileges (default: False)

**Returns:** Dictionary with command results

**Example:**
```python
result = api.run_command(
    command="nvidia-smi",
    nodes=["gpu-node-01", "gpu-node-02"],
    timeout=10
)

for hostname, res in result['results'].items():
    if res['status'] == 'success':
        print(f"{hostname}: {res['stdout']}")
    else:
        print(f"{hostname}: Error - {res['stderr']}")
```

---

#### run_on_all_nodes(command, timeout=30, use_sudo=False)

Execute a command on all connected nodes.

**Parameters:**
- `command` (str): Shell command to execute
- `timeout` (int): Command timeout in seconds (default: 30)
- `use_sudo` (bool): Run with sudo privileges (default: False)

**Returns:** Dictionary with command results from all nodes

**Raises:** `ValueError` if no nodes are connected

**Example:**
```python
result = api.run_on_all_nodes("uptime")
for hostname, res in result['results'].items():
    print(f"{hostname}: {res['stdout'].strip()}")
```

---

#### get_node_hostnames()

Get list of all connected node hostnames.

**Returns:** List of hostname strings

**Example:**
```python
hostnames = api.get_node_hostnames()
print(f"Available nodes: {', '.join(hostnames)}")
```

---

#### health()

Check master server health.

**Returns:** Dictionary with health status

**Example:**
```python
health = api.health()
print(f"Master status: {health['status']}, Slaves: {health['connected_slaves']}")
```

---

#### close()

Close the HTTP session. Called automatically when using context manager.

## Configuration

### Master Configuration

**CLI Options:**
```bash
thunderbolt-master --port 8000 \
                  --api-port 8001 \
                  --host 0.0.0.0 \
                  --health-check-interval 10 \
                  --max-failed-healthchecks 15
```

**Python API:**
```python
from thunderbolt.master import ThunderboltMaster

master = ThunderboltMaster(
    port=8000,                      # WebSocket port for slave connections
    health_check_interval=10,       # Seconds between health checks
    max_failed_healthchecks=15,     # Failed checks before disconnect
    no_app=False,                   # Set True to use router only
    routes_prefix="/thunderbolt"    # API route prefix (optional)
)
```

### Slave Configuration

**CLI Options:**
```bash
thunderbolt-slave --master-ip 10.0.0.1 \
                 --port 8000 \
                 --hostname custom-node-name
```

**Python API:**
```python
from thunderbolt.slave import ThunderboltSlave

slave = ThunderboltSlave(
    master_ip="10.0.0.1",           # Master IP address
    port=8000,                      # Master WebSocket port
    hostname="node-01",             # Custom hostname (optional)
    reconnect_delay=5,              # Initial reconnect delay in seconds
    reconnect_backoff_max=60        # Max reconnect delay in seconds
)
```

### Environment Variables

**Master:**
- `PORT`: WebSocket port for slave connections (default: 8000)

**Slave:**
- `MASTER_IP`: Master node IP address (default: "localhost")
- `PORT`: Master WebSocket port to connect to (default: 8000)

## Advanced Integration

### Embedding in Existing FastAPI App

You can embed Thunderbolt into your existing FastAPI application:

```python
from fastapi import FastAPI
from thunderbolt.master import ThunderboltMaster
from contextlib import asynccontextmanager

# Create master without its own app
master = ThunderboltMaster(port=8000, no_app=True, routes_prefix="/cluster")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start Thunderbolt background tasks
    master.start_background_tasks()
    yield
    # Shutdown Thunderbolt
    await master.shutdown()

# Your existing FastAPI app
app = FastAPI(lifespan=lifespan)

# Include Thunderbolt routes
app.include_router(master.router)

# Your existing routes
@app.get("/")
async def root():
    return {"message": "My application"}
```

Now Thunderbolt API will be available at `/cluster/nodes`, `/cluster/run`, etc.

## Use Cases

- **Cluster Management**: Deploy commands across compute clusters for maintenance and monitoring
- **Distributed Training**: Monitor and manage multi-node machine learning training jobs
- **Configuration Management**: Update configurations on multiple servers in parallel
- **Health Monitoring**: Collect system metrics from all nodes periodically
- **Resource Monitoring**: Query GPU/CPU/memory status across training clusters
- **Deployment Automation**: Execute deployment scripts across staging/production servers
- **Log Collection**: Gather logs from multiple nodes simultaneously

## Testing

Thunderbolt includes a comprehensive integration test suite using Docker containers:

```bash
# Install test dependencies
pip install -r requirements-test.txt

# Run tests
pytest tests/test_protocol.py -v -s
```

The test suite automatically:
- Spins up Docker containers for master and slave nodes
- Tests all API endpoints and communication protocols
- Verifies timeout handling, error cases, and edge conditions
- Cleans up all resources after tests complete

See `tests/README.md` for detailed testing documentation.

## License

MIT License
