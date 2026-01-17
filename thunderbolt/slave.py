import asyncio
import websockets
import json
import subprocess
import socket
import os
from typing import Optional
import signal
import sys


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
        max_reconnect_attempts: int = -1  # -1 for infinite
    ):
        self.master_host = master_host
        self.command_port = command_port
        self.health_port = health_port
        self.api_key = api_key or os.getenv("THUNDERBOLT_API_KEY", "default-key")
        self.hostname = hostname or socket.gethostname()
        self.reconnect_interval = reconnect_interval
        self.max_reconnect_attempts = max_reconnect_attempts
        
        # WebSocket connections
        self.command_ws: Optional[websockets.WebSocketClientProtocol] = None
        self.health_ws: Optional[websockets.WebSocketClientProtocol] = None
        
        # Control flags
        self._shutdown_event = asyncio.Event()
        self._command_connected = asyncio.Event()
        self._health_connected = asyncio.Event()
        
        # Task tracking
        self.tasks = []
        
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
                        attempt = 0  # Reset attempt counter on successful connection
                        
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
                        attempt = 0  # Reset attempt counter on successful connection
                        
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

