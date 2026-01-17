from fastapi import FastAPI, HTTPException, APIRouter
from pydantic import BaseModel
import asyncio
import websockets
from websockets.server import serve
import json
import uuid
from typing import List, Dict, Optional, Set
from datetime import datetime
import os
from contextlib import asynccontextmanager
from pathlib import Path
import shutil


class ThunderboltMaster:
    """Master node that manages slave connections and executes commands."""
    
    def __init__(
        self,
        port: Optional[int] = None,
        health_check_port: Optional[int] = None,
        health_check_interval: int = 10,
        max_failed_healthchecks: int = 15,
        no_app: bool = False, 
        routes_prefix=None,
        max_concurrent_sends: int = 2048,
        shared_dir: Optional[str] = None,
        shared_dir_poll_interval: float = 0.5,
        shared_dir_threshold: int = 10  # Use shared dir when >= this many nodes
    ):
        self.port = port or int(os.getenv("PORT", 8000))
        self.health_check_port = health_check_port or (self.port + 100)
        self.health_check_interval = health_check_interval
        self.max_failed_healthchecks = max_failed_healthchecks
        self.no_app = no_app
        self.max_concurrent_sends = max_concurrent_sends
        
        # Shared directory configuration
        self.shared_dir = Path(shared_dir) if shared_dir else None
        self.shared_dir_poll_interval = shared_dir_poll_interval
        self.shared_dir_threshold = shared_dir_threshold
        self.jobs_file = None
        
        if self.shared_dir:
            self._setup_shared_directory()
        
        # Store connected slaves
        self.slaves: Dict[str, dict] = {}
        
        # Store pending command responses
        self.pending_commands: Dict[str, dict] = {}
        
        # Track background tasks
        self.background_tasks: List[asyncio.Task] = []
        self.command_server_obj = None
        self.health_server_obj = None
        self._shutdown_event = asyncio.Event()
        self.routes_prefix = routes_prefix
        
        # Semaphore for rate limiting concurrent sends
        self._send_semaphore = asyncio.Semaphore(max_concurrent_sends)
        
        # Lock for thread-safe slave dict modifications
        self._slaves_lock = asyncio.Lock()
        
        # Create router
        self.router = self._create_router()
        
        # Only create app if no_app is False
        self.app = None if no_app else self._create_app()
    
    def _setup_shared_directory(self):
        """Initialize shared directory structure."""
        try:
            self.shared_dir.mkdir(parents=True, exist_ok=True)
            self.jobs_file = self.shared_dir / "jobs.json"
            
            # Initialize empty jobs file if it doesn't exist
            if not self.jobs_file.exists():
                with open(self.jobs_file, 'w') as f:
                    json.dump({}, f)
            
            print(f"[Thunderbolt] Shared directory initialized: {self.shared_dir}")
            print(f"[Thunderbolt] Jobs file: {self.jobs_file}")
            
        except Exception as e:
            print(f"[Thunderbolt] Failed to setup shared directory: {e}")
            raise
    
    def _should_use_shared_dir(self, num_nodes: int) -> bool:
        """Determine if shared directory should be used based on node count."""
        return self.shared_dir is not None and num_nodes >= self.shared_dir_threshold
    
    async def _execute_via_shared_dir(
        self,
        command: str,
        nodes: List[str],
        timeout: int,
        use_sudo: bool
    ) -> Dict:
        """Execute command via shared directory broadcast."""
        command_id = str(uuid.uuid4())
        
        # Create job entry
        job_data = {
            "type": "command",
            "command_id": command_id,
            "command": command,
            "timeout": timeout,
            "use_sudo": use_sudo,
            "nodes": nodes,
            "created_at": datetime.now().isoformat()
        }
        
        # Write job to jobs.json
        try:
            # Acquire file lock by reading, updating, and writing atomically
            jobs = {}
            if self.jobs_file.exists():
                with open(self.jobs_file, 'r') as f:
                    try:
                        jobs = json.load(f)
                    except json.JSONDecodeError:
                        jobs = {}
            
            jobs[command_id] = job_data
            
            # Write atomically using temp file
            temp_file = self.jobs_file.with_suffix('.tmp')
            with open(temp_file, 'w') as f:
                json.dump(jobs, f, indent=2)
            temp_file.replace(self.jobs_file)
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to write job to shared directory: {e}"
            )
        
        print(f"[Thunderbolt] Broadcast job {command_id} to {len(nodes)} nodes via shared dir")
        
        # Poll for completion
        start_time = datetime.now()
        results = {}
        completed_nodes = set()
        
        while len(completed_nodes) < len(nodes):
            # Check timeout
            elapsed = (datetime.now() - start_time).total_seconds()
            if elapsed > timeout + 10:
                print(f"[Thunderbolt] Job {command_id} timed out")
                break
            
            # Poll each node's result directory
            for hostname in nodes:
                if hostname in completed_nodes:
                    continue
                
                node_dir = self.shared_dir / hostname
                result_file = node_dir / f"{command_id}.json"
                
                if result_file.exists():
                    try:
                        with open(result_file, 'r') as f:
                            result_data = json.load(f)
                        results[hostname] = result_data
                        completed_nodes.add(hostname)
                    except Exception as e:
                        print(f"[Thunderbolt] Error reading result from {hostname}: {e}")
            
            # Sleep before next poll
            await asyncio.sleep(self.shared_dir_poll_interval)
        
        # Cleanup: remove job from jobs.json
        # try:
        #     jobs = {}
        #     if self.jobs_file.exists():
        #         with open(self.jobs_file, 'r') as f:
        #             jobs = json.load(f)
            
        #     if command_id in jobs:
        #         del jobs[command_id]
        #         temp_file = self.jobs_file.with_suffix('.tmp')
        #         with open(temp_file, 'w') as f:
        #             json.dump(jobs, f, indent=2)
        #         temp_file.replace(self.jobs_file)
            
        #     # Optionally cleanup result files
        #     for hostname in nodes:
        #         node_dir = self.shared_dir / hostname
        #         result_file = node_dir / f"{command_id}.json"
        #         if result_file.exists():
        #             result_file.unlink()
                    
        # except Exception as e:
        #     print(f"[Thunderbolt] Error cleaning up job {command_id}: {e}")
        
        return {
            "command": command,
            "total_nodes": len(nodes),
            "responses_received": len(results),
            "method": "shared_directory",
            "results": results
        }
    
    async def _execute_via_websocket(
        self,
        command: str,
        nodes: List[str],
        timeout: int,
        use_sudo: bool
    ) -> Dict:
        """Execute command via direct WebSocket connections."""
        command_id = str(uuid.uuid4())
        
        # Prepare pending command tracking
        self.pending_commands[command_id] = {
            "responses": {},
            "total_nodes": len(nodes),
            "received": 0,
            "event": asyncio.Event()
        }
        
        # Send command to all specified nodes in parallel
        command_msg = json.dumps({
            "type": "command",
            "command_id": command_id,
            "command": command,
            "timeout": timeout,
            "use_sudo": use_sudo
        })
        
        # Batch send with rate limiting
        send_tasks = []
        for hostname in nodes:
            slave_info = self.slaves.get(hostname)
            if slave_info and slave_info.get("command_ws"):
                send_tasks.append(
                    self._send_with_semaphore(
                        slave_info["command_ws"], 
                        command_msg,
                        hostname
                    )
                )
        
        # Send all commands in parallel with rate limiting
        send_results = await asyncio.gather(*send_tasks, return_exceptions=True)
        
        # Count failed sends
        failed_sends = sum(1 for r in send_results if isinstance(r, Exception))
        if failed_sends > 0:
            print(f"[Thunderbolt] {failed_sends}/{len(send_tasks)} command sends failed")
        
        # Wait for all responses (with timeout)
        try:
            await asyncio.wait_for(
                self.pending_commands[command_id]["event"].wait(),
                timeout=timeout + 5
            )
        except asyncio.TimeoutError:
            pass
        
        # Collect results
        results = self.pending_commands[command_id]["responses"]
        received = self.pending_commands[command_id]["received"]
        
        # Cleanup
        del self.pending_commands[command_id]
        
        return {
            "command": command,
            "total_nodes": len(nodes),
            "responses_received": received,
            "failed_sends": failed_sends,
            "method": "websocket",
            "results": results
        }
    
    def _create_router(self) -> APIRouter:
        """Create the FastAPI router with all endpoints."""

        router = None
        if isinstance(self.routes_prefix, str) and self.routes_prefix:
            router = APIRouter(prefix=self.routes_prefix)
        else:
            router = APIRouter()
        
        
        class CommandRequest(BaseModel):
            command: str
            nodes: List[str]
            timeout: Optional[int] = 30
            use_sudo: Optional[bool] = False
            force_method: Optional[str] = None  # 'websocket' or 'shared_dir'
        
        class BatchedCommandRequest(BaseModel):
            commands: List[Dict]
            force_method: Optional[str] = None
        
        @router.post("/run")
        async def run_command(request: CommandRequest):
            """Execute a command on specified nodes in parallel."""
            # Validate nodes
            invalid_nodes = [node for node in request.nodes if node not in self.slaves]
            if invalid_nodes:
                raise HTTPException(
                    status_code=404,
                    detail=f"Nodes not found: {invalid_nodes}"
                )
            
            # Determine execution method
            use_shared_dir = False
            if request.force_method == "shared_dir":
                if not self.shared_dir:
                    raise HTTPException(
                        status_code=400,
                        detail="Shared directory not configured"
                    )
                use_shared_dir = True
            elif request.force_method == "websocket":
                use_shared_dir = False
            else:
                # Auto-select based on node count
                use_shared_dir = self._should_use_shared_dir(len(request.nodes))
            
            # Execute command using appropriate method
            if use_shared_dir:
                return await self._execute_via_shared_dir(
                    request.command,
                    request.nodes,
                    request.timeout,
                    request.use_sudo
                )
            else:
                return await self._execute_via_websocket(
                    request.command,
                    request.nodes,
                    request.timeout,
                    request.use_sudo
                )
        
        @router.post("/run_batched")
        async def run_batched_commands(request: BatchedCommandRequest):
            """Execute different commands on different nodes in parallel."""
            if not request.commands:
                return {
                    "total_commands": 0,
                    "total_nodes": 0,
                    "results": {}
                }
            
            # Organize commands by node
            node_queues = {}
            for cmd_spec in request.commands:
                node = cmd_spec.get("node")
                command = cmd_spec.get("command")
                timeout = cmd_spec.get("timeout", 30)
                use_sudo = cmd_spec.get("use_sudo", False)
                
                if not node or not command:
                    continue
                    
                if node not in node_queues:
                    node_queues[node] = []
                
                node_queues[node].append({
                    "command": command,
                    "timeout": timeout,
                    "use_sudo": use_sudo
                })
            
            # Validate all nodes exist
            invalid_nodes = [node for node in node_queues.keys() if node not in self.slaves]
            if invalid_nodes:
                raise HTTPException(
                    status_code=404,
                    detail=f"Nodes not found: {invalid_nodes}"
                )
            
            # Determine execution method
            use_shared_dir = False
            if request.force_method == "shared_dir":
                if not self.shared_dir:
                    raise HTTPException(
                        status_code=400,
                        detail="Shared directory not configured"
                    )
                use_shared_dir = True
            elif request.force_method == "websocket":
                use_shared_dir = False
            else:
                # For batched, consider total command count
                total_commands = len(request.commands)
                use_shared_dir = self._should_use_shared_dir(total_commands)
            
            if use_shared_dir:
                # Execute batched commands via shared directory
                return await self._execute_batched_via_shared_dir(node_queues)
            else:
                # Execute batched commands via websocket
                return await self._execute_batched_via_websocket(node_queues)
        
        @router.get("/nodes")
        async def list_nodes():
            """List all connected slave nodes."""
            return {
                "total": len(self.slaves),
                "nodes": [
                    {
                        "hostname": hostname,
                        "last_seen": info["last_seen"].isoformat(),
                        "failed_healthchecks": info["failed_healthchecks"],
                        "command_connected": info.get("command_ws") is not None,
                        "health_connected": info.get("health_ws") is not None
                    }
                    for hostname, info in self.slaves.items()
                ]
            }
        
        @router.get("/")
        async def root():
            return {
                "message": "Thunderbolt Master Command Runner",
                "connected_slaves": len(self.slaves),
                "command_port": self.port,
                "health_check_port": self.health_check_port,
                "shared_directory": str(self.shared_dir) if self.shared_dir else None
            }
        
        @router.get("/health")
        async def health():
            return {
                "status": "healthy", 
                "connected_slaves": len(self.slaves),
                "pending_commands": len(self.pending_commands)
            }
        
        return router
    
    async def _execute_batched_via_shared_dir(self, node_queues: Dict[str, List[dict]]) -> Dict:
        """Execute batched commands via shared directory."""
        # Create a single broadcast job with all commands
        command_id = str(uuid.uuid4())
        
        job_data = {
            "type": "batched_command",
            "command_id": command_id,
            "node_commands": node_queues,
            "created_at": datetime.now().isoformat()
        }
        
        # Write job to jobs.json
        try:
            jobs = {}
            if self.jobs_file.exists():
                with open(self.jobs_file, 'r') as f:
                    try:
                        jobs = json.load(f)
                    except json.JSONDecodeError:
                        jobs = {}
            
            jobs[command_id] = job_data
            
            temp_file = self.jobs_file.with_suffix('.tmp')
            with open(temp_file, 'w') as f:
                json.dump(jobs, f, indent=2)
            temp_file.replace(self.jobs_file)
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to write batched job: {e}"
            )
        
        # Determine max timeout
        max_timeout = max(
            max(cmd["timeout"] for cmd in cmds)
            for cmds in node_queues.values()
        )
        
        # Poll for completion
        start_time = datetime.now()
        results = {}
        completed_nodes = set()
        
        while len(completed_nodes) < len(node_queues):
            elapsed = (datetime.now() - start_time).total_seconds()
            if elapsed > max_timeout + 10:
                break
            
            for hostname in node_queues.keys():
                if hostname in completed_nodes:
                    continue
                
                node_dir = self.shared_dir / hostname
                result_file = node_dir / f"{command_id}.json"
                
                if result_file.exists():
                    try:
                        with open(result_file, 'r') as f:
                            result_data = json.load(f)
                        results[hostname] = result_data
                        completed_nodes.add(hostname)
                    except Exception as e:
                        print(f"[Thunderbolt] Error reading batched result from {hostname}: {e}")
            
            await asyncio.sleep(self.shared_dir_poll_interval)
        
        # Cleanup
        try:
            jobs = {}
            if self.jobs_file.exists():
                with open(self.jobs_file, 'r') as f:
                    jobs = json.load(f)
            
            if command_id in jobs:
                del jobs[command_id]
                temp_file = self.jobs_file.with_suffix('.tmp')
                with open(temp_file, 'w') as f:
                    json.dump(jobs, f, indent=2)
                temp_file.replace(self.jobs_file)
            
            for hostname in node_queues.keys():
                node_dir = self.shared_dir / hostname
                result_file = node_dir / f"{command_id}.json"
                if result_file.exists():
                    result_file.unlink()
                    
        except Exception as e:
            print(f"[Thunderbolt] Error cleaning up batched job {command_id}: {e}")
        
        return {
            "total_commands": sum(len(cmds) for cmds in node_queues.values()),
            "total_nodes": len(node_queues),
            "method": "shared_directory",
            "results": results
        }
    
    async def _execute_batched_via_websocket(self, node_queues: Dict[str, List[dict]]) -> Dict:
        """Execute batched commands via websocket."""
        async def execute_node_queue(hostname: str, commands: List[dict]):
            """Execute a queue of commands for a single node sequentially."""
            node_results = []
            slave_info = self.slaves.get(hostname)
            
            if not slave_info or not slave_info.get("command_ws"):
                return [(cmd["command"], {
                    "success": False,
                    "error": f"Node {hostname} not connected"
                }) for cmd in commands]
            
            for cmd_spec in commands:
                command_id = str(uuid.uuid4())
                
                self.pending_commands[command_id] = {
                    "responses": {},
                    "total_nodes": 1,
                    "received": 0,
                    "event": asyncio.Event()
                }
                
                command_msg = json.dumps({
                    "type": "command",
                    "command_id": command_id,
                    "command": cmd_spec["command"],
                    "timeout": cmd_spec["timeout"],
                    "use_sudo": cmd_spec["use_sudo"]
                })
                
                try:
                    async with self._send_semaphore:
                        await slave_info["command_ws"].send(command_msg)
                    
                    await asyncio.wait_for(
                        self.pending_commands[command_id]["event"].wait(),
                        timeout=cmd_spec["timeout"] + 5
                    )
                    
                    result = self.pending_commands[command_id]["responses"].get(
                        hostname,
                        {"success": False, "error": "No response received"}
                    )
                    
                except asyncio.TimeoutError:
                    result = {"success": False, "error": "Command timeout"}
                except Exception as e:
                    result = {"success": False, "error": f"Send failed: {str(e)}"}
                finally:
                    if command_id in self.pending_commands:
                        del self.pending_commands[command_id]
                
                node_results.append((cmd_spec["command"], result))
            
            return node_results
        
        tasks = [
            execute_node_queue(hostname, commands)
            for hostname, commands in node_queues.items()
        ]
        
        results_by_node = await asyncio.gather(*tasks, return_exceptions=True)
        
        formatted_results = {}
        for hostname, results in zip(node_queues.keys(), results_by_node):
            if isinstance(results, Exception):
                formatted_results[hostname] = {
                    "error": f"Node execution failed: {str(results)}",
                    "commands": []
                }
            else:
                formatted_results[hostname] = {
                    "commands": [
                        {"command": cmd, "result": result}
                        for cmd, result in results
                    ]
                }
        
        return {
            "total_commands": sum(len(cmds) for cmds in node_queues.values()),
            "total_nodes": len(node_queues),
            "method": "websocket",
            "results": formatted_results
        }
    
    async def _send_with_semaphore(self, websocket, message: str, hostname: str):
        """Send message with semaphore-based rate limiting."""
        async with self._send_semaphore:
            try:
                await websocket.send(message)
            except Exception as e:
                print(f"[Thunderbolt] Failed to send to {hostname}: {e}")
                raise
    
    async def handle_command_connection(self, websocket):
        """Handle incoming slave command connections."""
        hostname = None
        try:
            registration_msg = await websocket.recv()
            registration = json.loads(registration_msg)
            
            if registration.get("type") != "register":
                await websocket.send(json.dumps({
                    "status": "error",
                    "message": "First message must be registration"
                }))
                return
            
            hostname = registration["hostname"]
            api_key = registration["api_key"]
            
            async with self._slaves_lock:
                if hostname not in self.slaves:
                    self.slaves[hostname] = {
                        "command_ws": websocket,
                        "health_ws": None,
                        "api_key": api_key,
                        "last_seen": datetime.now(),
                        "failed_healthchecks": 0
                    }
                else:
                    self.slaves[hostname]["command_ws"] = websocket
                    self.slaves[hostname]["api_key"] = api_key
            
            await websocket.send(json.dumps({
                "status": "registered",
                "hostname": hostname,
                "connection_type": "command"
            }))
            
            print(f"[Thunderbolt] Slave command channel registered: {hostname}")
            
            async for message in websocket:
                data = json.loads(message)
                
                if data.get("type") == "command_result":
                    command_id = data.get("command_id")
                    if command_id in self.pending_commands:
                        pending = self.pending_commands[command_id]
                        pending["responses"][hostname] = data
                        pending["received"] += 1
                        
                        if pending["received"] >= pending["total_nodes"]:
                            pending["event"].set()
        
        except websockets.exceptions.ConnectionClosed:
            print(f"[Thunderbolt] Slave command channel disconnected: {hostname}")
        except asyncio.CancelledError:
            print(f"[Thunderbolt] Command connection cancelled for: {hostname}")
            raise
        except Exception as e:
            print(f"[Thunderbolt] Error handling slave command {hostname}: {e}")
        finally:
            if hostname:
                async with self._slaves_lock:
                    if hostname in self.slaves:
                        self.slaves[hostname]["command_ws"] = None
                        if not self.slaves[hostname].get("health_ws"):
                            del self.slaves[hostname]
                            print(f"[Thunderbolt] Removed slave: {hostname}")
    
    async def handle_health_connection(self, websocket):
        """Handle incoming slave health check connections."""
        hostname = None
        try:
            registration_msg = await websocket.recv()
            registration = json.loads(registration_msg)
            
            if registration.get("type") != "register":
                await websocket.send(json.dumps({
                    "status": "error",
                    "message": "First message must be registration"
                }))
                return
            
            hostname = registration["hostname"]
            
            async with self._slaves_lock:
                if hostname not in self.slaves:
                    self.slaves[hostname] = {
                        "command_ws": None,
                        "health_ws": websocket,
                        "api_key": registration.get("api_key"),
                        "last_seen": datetime.now(),
                        "failed_healthchecks": 0
                    }
                else:
                    self.slaves[hostname]["health_ws"] = websocket
            
            await websocket.send(json.dumps({
                "status": "registered",
                "hostname": hostname,
                "connection_type": "health"
            }))
            
            print(f"[Thunderbolt] Slave health channel registered: {hostname}")
            
            async for message in websocket:
                data = json.loads(message)
                
                if data.get("type") == "healthcheck_response":
                    async with self._slaves_lock:
                        if hostname in self.slaves:
                            self.slaves[hostname]["last_seen"] = datetime.now()
                            self.slaves[hostname]["failed_healthchecks"] = 0
        
        except websockets.exceptions.ConnectionClosed:
            print(f"[Thunderbolt] Slave health channel disconnected: {hostname}")
        except asyncio.CancelledError:
            print(f"[Thunderbolt] Health connection cancelled for: {hostname}")
            raise
        except Exception as e:
            print(f"[Thunderbolt] Error handling slave health {hostname}: {e}")
        finally:
            if hostname:
                async with self._slaves_lock:
                    if hostname in self.slaves:
                        self.slaves[hostname]["health_ws"] = None
                        if not self.slaves[hostname].get("command_ws"):
                            del self.slaves[hostname]
                            print(f"[Thunderbolt] Removed slave: {hostname}")
    
    async def health_check_loop(self):
        """Periodically health check all slaves in parallel."""
        try:
            while not self._shutdown_event.is_set():
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(),
                        timeout=self.health_check_interval
                    )
                    break
                except asyncio.TimeoutError:
                    pass
                
                health_check_msg = json.dumps({"type": "healthcheck"})
                
                async with self._slaves_lock:
                    slaves_snapshot = list(self.slaves.items())
                
                health_tasks = []
                for hostname, slave_info in slaves_snapshot:
                    health_tasks.append(
                        self._send_health_check(hostname, slave_info, health_check_msg)
                    )
                
                results = await asyncio.gather(*health_tasks, return_exceptions=True)
                
                disconnected_slaves = []
                async with self._slaves_lock:
                    for (hostname, _), result in zip(slaves_snapshot, results):
                        if hostname not in self.slaves:
                            continue
                            
                        if isinstance(result, Exception):
                            print(f"[Thunderbolt] Health check failed for {hostname}: {result}")
                            disconnected_slaves.append(hostname)
                        elif result is False:
                            print(f"[Thunderbolt] Slave {hostname} failed {self.max_failed_healthchecks} health checks. Disconnecting.")
                            disconnected_slaves.append(hostname)
                
                for hostname in disconnected_slaves:
                    async with self._slaves_lock:
                        if hostname in self.slaves:
                            slave_info = self.slaves[hostname]
                            close_tasks = []
                            if slave_info.get("command_ws"):
                                close_tasks.append(slave_info["command_ws"].close())
                            if slave_info.get("health_ws"):
                                close_tasks.append(slave_info["health_ws"].close())
                            
                            if close_tasks:
                                await asyncio.gather(*close_tasks, return_exceptions=True)
                            
                            del self.slaves[hostname]
        
        except asyncio.CancelledError:
            print("[Thunderbolt] Health check loop cancelled")
            raise
        except Exception as e:
            print(f"[Thunderbolt] Health check loop error: {e}")
    
    async def _send_health_check(self, hostname: str, slave_info: dict, msg: str) -> bool:
        """Send health check to a single slave. Returns False if max failures reached."""
        try:
            health_ws = slave_info.get("health_ws")
            if not health_ws:
                return False
            
            await health_ws.send(msg)
            
            async with self._slaves_lock:
                if hostname in self.slaves:
                    self.slaves[hostname]["failed_healthchecks"] += 1
                    
                    if self.slaves[hostname]["failed_healthchecks"] >= self.max_failed_healthchecks:
                        return False
            
            return True
            
        except Exception as e:
            raise e
    
    async def command_server(self):
        """Run the WebSocket server for command connections."""
        try:
            self.command_server_obj = await serve(
                self.handle_command_connection, 
                "0.0.0.0", 
                self.port,
                ping_interval=30,
                ping_timeout=10,
                max_size=10 * 1024 * 1024
            )
            print(f"[Thunderbolt] Command WebSocket server listening on port {self.port}")
            
            await self._shutdown_event.wait()
            
            self.command_server_obj.close()
            await self.command_server_obj.wait_closed()
            print("[Thunderbolt] Command WebSocket server closed")
            
        except asyncio.CancelledError:
            print("[Thunderbolt] Command WebSocket server cancelled")
            if self.command_server_obj:
                self.command_server_obj.close()
                await self.command_server_obj.wait_closed()
            raise
        except Exception as e:
            print(f"[Thunderbolt] Command WebSocket server error: {e}")
            raise
    
    async def health_server(self):
        """Run the WebSocket server for health check connections."""
        try:
            self.health_server_obj = await serve(
                self.handle_health_connection, 
                "0.0.0.0", 
                self.health_check_port,
                ping_interval=20,
                ping_timeout=10
            )
            print(f"[Thunderbolt] Health WebSocket server listening on port {self.health_check_port}")
            
            await self._shutdown_event.wait()
            
            self.health_server_obj.close()
            await self.health_server_obj.wait_closed()
            print("[Thunderbolt] Health WebSocket server closed")
            
        except asyncio.CancelledError:
            print("[Thunderbolt] Health WebSocket server cancelled")
            if self.health_server_obj:
                self.health_server_obj.close()
                await self.health_server_obj.wait_closed()
            raise
        except Exception as e:
            print(f"[Thunderbolt] Health WebSocket server error: {e}")
            raise
    
    def start_background_tasks(self) -> List[asyncio.Task]:
        """Start websocket servers and health check loop as background tasks."""
        print("[Thunderbolt] Starting background tasks...")
        
        self._shutdown_event.clear()
        
        command_ws_task = asyncio.create_task(
            self.command_server(),
            name="thunderbolt-command-websocket"
        )
        health_ws_task = asyncio.create_task(
            self.health_server(),
            name="thunderbolt-health-websocket"
        )
        health_task = asyncio.create_task(
            self.health_check_loop(),
            name="thunderbolt-health-check"
        )
        
        self.background_tasks = [command_ws_task, health_ws_task, health_task]
        
        print(f"[Thunderbolt] Started {len(self.background_tasks)} background tasks")
        return self.background_tasks
    
    async def shutdown(self):
        """Gracefully shutdown the master server."""
        print("[Thunderbolt] Initiating shutdown...")
        
        self._shutdown_event.set()
        
        close_tasks = []
        async with self._slaves_lock:
            for hostname, slave_info in list(self.slaves.items()):
                try:
                    if slave_info.get("command_ws"):
                        close_tasks.append(slave_info["command_ws"].close())
                    if slave_info.get("health_ws"):
                        close_tasks.append(slave_info["health_ws"].close())
                except Exception as e:
                    print(f"[Thunderbolt] Error closing connection to {hostname}: {e}")
        
        if close_tasks:
            await asyncio.gather(*close_tasks, return_exceptions=True)
        
        for task in self.background_tasks:
            if not task.done():
                task.cancel()
        
        if self.background_tasks:
            await asyncio.gather(*self.background_tasks, return_exceptions=True)
        
        self.slaves.clear()
        self.background_tasks.clear()
        
        print("[Thunderbolt] Shutdown complete")
    
    def _create_app(self) -> FastAPI:
        """Create the FastAPI app with lifespan management."""
        @asynccontextmanager
        async def lifespan(app: FastAPI):
            self.start_background_tasks()
            yield
            await self.shutdown()
        
        app = FastAPI(lifespan=lifespan)
        app.include_router(self.router)
        return app
    
    def run(self, host: str = "0.0.0.0", port: Optional[int] = None):
        """Run the master server."""
        if self.no_app:
            raise RuntimeError("Cannot call run() when no_app=True. Use the router in your own FastAPI app.")
        
        import uvicorn
        api_port = port or (self.port + 1)
        print(f"[Thunderbolt] Starting ThunderBolt Master")
        print(f"  - API Server: {host}:{api_port}")
        print(f"  - Command WebSocket: {host}:{self.port}")
        print(f"  - Health WebSocket: {host}:{self.health_check_port}")
        if self.shared_dir:
            print(f"  - Shared Directory: {self.shared_dir}")
            print(f"  - Shared Dir Threshold: {self.shared_dir_threshold} nodes")
        uvicorn.run(self.app, host=host, port=api_port)


if __name__ == "__main__":
    master = ThunderboltMaster()
    master.run()