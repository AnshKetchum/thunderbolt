import asyncio
import websockets
import json
import subprocess
import socket
import os
from typing import Optional
import signal
import sys
from pathlib import Path
from datetime import datetime


class ThunderboltSlave:
    """Slave node that connects to master and executes commands."""
    
    def __init__(
        self,
        master_host: str,
        command_port: int = 8000,
        health_port: int = 8100,
        api_key: Optional[str] = None,
        hostname: Optional[str] = None,
        reconnect_interval: int = 5,
        max_reconnect_attempts: int = -1,
        shared_dir: Optional[str] = None,
        shared_dir_poll_interval: float = 0.5
    ):
        self.master_host = master_host
        self.command_port = command_port
        self.health_port = health_port
        self.api_key = api_key or os.getenv("THUNDERBOLT_API_KEY", "default-key")
        self.hostname = hostname or socket.gethostname()
        self.reconnect_interval = reconnect_interval
        self.max_reconnect_attempts = max_reconnect_attempts
        
        # Shared directory configuration
        self.shared_dir = Path(shared_dir) if shared_dir else None
        self.shared_dir_poll_interval = shared_dir_poll_interval
        self.node_dir = None
        self.jobs_file = None
        self.processed_jobs = set()  # Track already processed job IDs
        
        if self.shared_dir:
            self._setup_shared_directory()
        
        # WebSocket connections
        self.command_ws: Optional[websockets.WebSocketClientProtocol] = None
        self.health_ws: Optional[websockets.WebSocketClientProtocol] = None
        
        # Control flags
        self._shutdown_event = asyncio.Event()
        self._command_connected = asyncio.Event()
        self._health_connected = asyncio.Event()
        
        # Task tracking
        self.tasks = []
    
    def _setup_shared_directory(self):
        """Initialize slave's directory in shared storage."""
        try:
            # Create node-specific directory
            self.node_dir = self.shared_dir / self.hostname
            self.node_dir.mkdir(parents=True, exist_ok=True)
            
            # Reference to jobs file
            self.jobs_file = self.shared_dir / "jobs.json"
            
            print(f"[Slave] Shared directory initialized: {self.node_dir}")
            print(f"[Slave] Watching jobs file: {self.jobs_file}")
            
        except Exception as e:
            print(f"[Slave] Failed to setup shared directory: {e}")
            raise
    
    async def poll_shared_directory(self):
        """Poll shared directory for new jobs."""
        if not self.shared_dir or not self.jobs_file:
            return
        
        print("[Slave] Starting shared directory polling...")
        
        try:
            while not self._shutdown_event.is_set():
                try:
                    # Read jobs file
                    if not self.jobs_file.exists():
                        await asyncio.sleep(self.shared_dir_poll_interval)
                        continue
                    
                    with open(self.jobs_file, 'r') as f:
                        try:
                            jobs = json.load(f)
                        except json.JSONDecodeError:
                            jobs = {}
                    
                    # Process each job
                    for command_id, job_data in jobs.items():
                        # Skip if already processed
                        if command_id in self.processed_jobs:
                            continue
                        
                        # Check if this node should execute the job
                        job_type = job_data.get("type")
                        
                        if job_type == "command":
                            # Simple command for specific nodes
                            nodes = job_data.get("nodes", [])
                            if self.hostname not in nodes:
                                continue
                            
                            # Check if we've already completed this job
                            result_file = self.node_dir / f"{command_id}.json"
                            if result_file.exists():
                                self.processed_jobs.add(command_id)
                                continue
                            
                            # Execute command
                            print(f"[Slave] Processing shared dir job {command_id}")
                            asyncio.create_task(
                                self._execute_shared_dir_command(
                                    command_id,
                                    job_data.get("command"),
                                    job_data.get("timeout", 30),
                                    job_data.get("use_sudo", False)
                                )
                            )
                            
                            # Mark as processed
                            self.processed_jobs.add(command_id)
                        
                        elif job_type == "batched_command":
                            # Batched commands - check if this node has commands
                            node_commands = job_data.get("node_commands", {})
                            if self.hostname not in node_commands:
                                continue
                            
                            # Check if we've already completed this job
                            result_file = self.node_dir / f"{command_id}.json"
                            if result_file.exists():
                                self.processed_jobs.add(command_id)
                                continue
                            
                            # Execute batched commands for this node
                            print(f"[Slave] Processing batched shared dir job {command_id}")
                            asyncio.create_task(
                                self._execute_shared_dir_batched(
                                    command_id,
                                    node_commands[self.hostname]
                                )
                            )
                            
                            # Mark as processed
                            self.processed_jobs.add(command_id)
                    
                    # Clean up old processed job IDs (keep last 1000)
                    if len(self.processed_jobs) > 1000:
                        # Remove oldest entries (keep most recent 500)
                        current_job_ids = set(jobs.keys())
                        self.processed_jobs = current_job_ids.union(
                            list(self.processed_jobs)[-500:]
                        )
                    
                except Exception as e:
                    print(f"[Slave] Error polling shared directory: {e}")
                
                # Wait before next poll
                await asyncio.sleep(self.shared_dir_poll_interval)
        
        except asyncio.CancelledError:
            print("[Slave] Shared directory polling cancelled")
            raise
    
    async def _execute_shared_dir_command(
        self,
        command_id: str,
        command: str,
        timeout: int,
        use_sudo: bool
    ):
        """Execute a command from shared directory and write result."""
        result = {
            "type": "command_result",
            "command_id": command_id,
            "hostname": self.hostname,
            "command": command,
            "timestamp": datetime.now().isoformat()
        }
        
        try:
            # Prepare command
            if use_sudo:
                full_command = f"sudo {command}"
            else:
                full_command = command
            
            # Execute with timeout
            process = await asyncio.create_subprocess_shell(
                full_command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            try:
                stdout, stderr = await asyncio.wait_for(
                    process.communicate(),
                    timeout=timeout
                )
                
                result.update({
                    "success": True,
                    "exit_code": process.returncode,
                    "stdout": stdout.decode('utf-8', errors='replace'),
                    "stderr": stderr.decode('utf-8', errors='replace')
                })
                
            except asyncio.TimeoutError:
                # Kill the process
                try:
                    process.kill()
                    await process.wait()
                except:
                    pass
                
                result.update({
                    "success": False,
                    "error": f"Command timed out after {timeout}s"
                })
        
        except Exception as e:
            result.update({
                "success": False,
                "error": str(e)
            })
        
        # Write result to node directory
        try:
            result_file = self.node_dir / f"{command_id}.json"
            with open(result_file, 'w') as f:
                json.dump(result, f, indent=2)
            print(f"[Slave] Wrote result for job {command_id}")
        except Exception as e:
            print(f"[Slave] Failed to write result for job {command_id}: {e}")
    
    async def _execute_shared_dir_batched(
        self,
        command_id: str,
        commands: list
    ):
        """Execute batched commands from shared directory and write results."""
        results = {
            "type": "batched_command_result",
            "command_id": command_id,
            "hostname": self.hostname,
            "timestamp": datetime.now().isoformat(),
            "commands": []
        }
        
        # Execute each command sequentially
        for cmd_spec in commands:
            command = cmd_spec.get("command")
            timeout = cmd_spec.get("timeout", 30)
            use_sudo = cmd_spec.get("use_sudo", False)
            
            cmd_result = {
                "command": command
            }
            
            try:
                # Prepare command
                if use_sudo:
                    full_command = f"sudo {command}"
                else:
                    full_command = command
                
                # Execute with timeout
                process = await asyncio.create_subprocess_shell(
                    full_command,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                
                try:
                    stdout, stderr = await asyncio.wait_for(
                        process.communicate(),
                        timeout=timeout
                    )
                    
                    cmd_result.update({
                        "result": {
                            "success": True,
                            "exit_code": process.returncode,
                            "stdout": stdout.decode('utf-8', errors='replace'),
                            "stderr": stderr.decode('utf-8', errors='replace')
                        }
                    })
                    
                except asyncio.TimeoutError:
                    # Kill the process
                    try:
                        process.kill()
                        await process.wait()
                    except:
                        pass
                    
                    cmd_result.update({
                        "result": {
                            "success": False,
                            "error": f"Command timed out after {timeout}s"
                        }
                    })
            
            except Exception as e:
                cmd_result.update({
                    "result": {
                        "success": False,
                        "error": str(e)
                    }
                })
            
            results["commands"].append(cmd_result)
        
        # Write results to node directory
        try:
            result_file = self.node_dir / f"{command_id}.json"
            with open(result_file, 'w') as f:
                json.dump(results, f, indent=2)
            print(f"[Slave] Wrote batched results for job {command_id}")
        except Exception as e:
            print(f"[Slave] Failed to write batched results for job {command_id}: {e}")
    
    async def connect_command_channel(self):
        """Connect to master's command channel with reconnection logic."""
        attempt = 0
        
        while not self._shutdown_event.is_set():
            if self.max_reconnect_attempts > 0 and attempt >= self.max_reconnect_attempts:
                print(f"[Slave] Max reconnection attempts ({self.max_reconnect_attempts}) reached for command channel")
                break
                
            try:
                attempt += 1
                uri = f"ws://{self.master_host}:{self.command_port}"
                print(f"[Slave] Connecting to command channel: {uri} (attempt {attempt})")
                
                async with websockets.connect(uri, ping_interval=30, ping_timeout=10) as websocket:
                    self.command_ws = websocket
                    
                    # Register with master
                    registration = {
                        "type": "register",
                        "hostname": self.hostname,
                        "api_key": self.api_key
                    }
                    await websocket.send(json.dumps(registration))
                    
                    # Wait for acknowledgment
                    ack_msg = await websocket.recv()
                    ack = json.loads(ack_msg)
                    
                    if ack.get("status") == "registered":
                        print(f"[Slave] Command channel registered: {self.hostname}")
                        self._command_connected.set()
                        attempt = 0
                        
                        # Handle incoming commands
                        await self.handle_commands(websocket)
                    else:
                        print(f"[Slave] Registration failed: {ack.get('message')}")
                
            except websockets.exceptions.ConnectionClosed:
                print("[Slave] Command channel connection closed")
                self._command_connected.clear()
            except asyncio.CancelledError:
                print("[Slave] Command channel connection cancelled")
                self._command_connected.clear()
                raise
            except Exception as e:
                print(f"[Slave] Command channel error: {e}")
                self._command_connected.clear()
            finally:
                self.command_ws = None
            
            # Wait before reconnecting
            if not self._shutdown_event.is_set():
                print(f"[Slave] Reconnecting command channel in {self.reconnect_interval}s...")
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(),
                        timeout=self.reconnect_interval
                    )
                except asyncio.TimeoutError:
                    pass
    
    async def connect_health_channel(self):
        """Connect to master's health check channel with reconnection logic."""
        attempt = 0
        
        while not self._shutdown_event.is_set():
            if self.max_reconnect_attempts > 0 and attempt >= self.max_reconnect_attempts:
                print(f"[Slave] Max reconnection attempts ({self.max_reconnect_attempts}) reached for health channel")
                break
                
            try:
                attempt += 1
                uri = f"ws://{self.master_host}:{self.health_port}"
                print(f"[Slave] Connecting to health channel: {uri} (attempt {attempt})")
                
                async with websockets.connect(uri, ping_interval=20, ping_timeout=10) as websocket:
                    self.health_ws = websocket
                    
                    # Register with master
                    registration = {
                        "type": "register",
                        "hostname": self.hostname,
                        "api_key": self.api_key
                    }
                    await websocket.send(json.dumps(registration))
                    
                    # Wait for acknowledgment
                    ack_msg = await websocket.recv()
                    ack = json.loads(ack_msg)
                    
                    if ack.get("status") == "registered":
                        print(f"[Slave] Health channel registered: {self.hostname}")
                        self._health_connected.set()
                        attempt = 0
                        
                        # Handle health checks
                        await self.handle_health_checks(websocket)
                    else:
                        print(f"[Slave] Health registration failed: {ack.get('message')}")
                
            except websockets.exceptions.ConnectionClosed:
                print("[Slave] Health channel connection closed")
                self._health_connected.clear()
            except asyncio.CancelledError:
                print("[Slave] Health channel connection cancelled")
                self._health_connected.clear()
                raise
            except Exception as e:
                print(f"[Slave] Health channel error: {e}")
                self._health_connected.clear()
            finally:
                self.health_ws = None
            
            # Wait before reconnecting
            if not self._shutdown_event.is_set():
                print(f"[Slave] Reconnecting health channel in {self.reconnect_interval}s...")
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(),
                        timeout=self.reconnect_interval
                    )
                except asyncio.TimeoutError:
                    pass
    
    async def handle_commands(self, websocket):
        """Handle incoming commands from master."""
        try:
            async for message in websocket:
                data = json.loads(message)
                
                if data.get("type") == "command":
                    # Execute command asynchronously
                    asyncio.create_task(
                        self.execute_command(
                            websocket,
                            data.get("command_id"),
                            data.get("command"),
                            data.get("timeout", 30),
                            data.get("use_sudo", False)
                        )
                    )
        
        except websockets.exceptions.ConnectionClosed:
            pass
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[Slave] Error handling commands: {e}")
    
    async def handle_health_checks(self, websocket):
        """Handle incoming health checks from master."""
        try:
            async for message in websocket:
                data = json.loads(message)
                
                if data.get("type") == "healthcheck":
                    # Respond immediately
                    response = {
                        "type": "healthcheck_response",
                        "hostname": self.hostname,
                        "status": "healthy"
                    }
                    await websocket.send(json.dumps(response))
        
        except websockets.exceptions.ConnectionClosed:
            pass
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[Slave] Error handling health checks: {e}")
    
    async def execute_command(
        self,
        websocket,
        command_id: str,
        command: str,
        timeout: int,
        use_sudo: bool
    ):
        """Execute a command and send result back to master."""
        result = {
            "type": "command_result",
            "command_id": command_id,
            "hostname": self.hostname,
            "command": command
        }
        
        try:
            # Prepare command
            if use_sudo:
                full_command = f"sudo {command}"
            else:
                full_command = command
            
            # Execute with timeout
            process = await asyncio.create_subprocess_shell(
                full_command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            try:
                stdout, stderr = await asyncio.wait_for(
                    process.communicate(),
                    timeout=timeout
                )
                
                result.update({
                    "success": True,
                    "exit_code": process.returncode,
                    "stdout": stdout.decode('utf-8', errors='replace'),
                    "stderr": stderr.decode('utf-8', errors='replace')
                })
                
            except asyncio.TimeoutError:
                # Kill the process
                try:
                    process.kill()
                    await process.wait()
                except:
                    pass
                
                result.update({
                    "success": False,
                    "error": f"Command timed out after {timeout}s"
                })
        
        except Exception as e:
            result.update({
                "success": False,
                "error": str(e)
            })
        
        # Send result back to master
        try:
            await websocket.send(json.dumps(result))
        except Exception as e:
            print(f"[Slave] Failed to send command result: {e}")
    
    async def start(self):
        """Start the slave and connect to master."""
        print(f"[Slave] Starting slave: {self.hostname}")
        print(f"[Slave] Master: {self.master_host}")
        print(f"[Slave] Command port: {self.command_port}")
        print(f"[Slave] Health port: {self.health_port}")
        if self.shared_dir:
            print(f"[Slave] Shared directory: {self.shared_dir}")
        
        # Clear shutdown event
        self._shutdown_event.clear()
        
        # Create connection tasks
        command_task = asyncio.create_task(
            self.connect_command_channel(),
            name="command-channel"
        )
        health_task = asyncio.create_task(
            self.connect_health_channel(),
            name="health-channel"
        )
        
        self.tasks = [command_task, health_task]
        
        # Add shared directory polling if configured
        if self.shared_dir:
            poll_task = asyncio.create_task(
                self.poll_shared_directory(),
                name="shared-dir-polling"
            )
            self.tasks.append(poll_task)
        
        # Wait for both connections to establish
        try:
            await asyncio.wait_for(
                asyncio.gather(
                    self._command_connected.wait(),
                    self._health_connected.wait()
                ),
                timeout=30
            )
            print("[Slave] Both channels connected successfully")
        except asyncio.TimeoutError:
            print("[Slave] Warning: Failed to establish all connections within 30s")
        
        # Wait for shutdown
        await self._shutdown_event.wait()
    
    async def shutdown(self):
        """Gracefully shutdown the slave."""
        print("[Slave] Initiating shutdown...")
        
        # Signal shutdown
        self._shutdown_event.set()
        
        # Close connections
        close_tasks = []
        if self.command_ws:
            close_tasks.append(self.command_ws.close())
        if self.health_ws:
            close_tasks.append(self.health_ws.close())
        
        if close_tasks:
            await asyncio.gather(*close_tasks, return_exceptions=True)
        
        # Cancel all tasks
        for task in self.tasks:
            if not task.done():
                task.cancel()
        
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        
        print("[Slave] Shutdown complete")
    
    def run(self):
        """Run the slave with signal handling."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Signal handlers
        def signal_handler(sig, frame):
            print(f"\n[Slave] Received signal {sig}")
            loop.create_task(self.shutdown())
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        try:
            loop.run_until_complete(self.start())
        except KeyboardInterrupt:
            print("\n[Slave] Keyboard interrupt received")
        finally:
            loop.run_until_complete(self.shutdown())
            loop.close()