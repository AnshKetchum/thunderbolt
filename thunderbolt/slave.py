import asyncio
import websockets
import json
import os
import secrets
import socket
import subprocess
import signal
from typing import Optional


class ThunderboltSlave:
    """Slave node that connects to master and executes commands."""
    
    def __init__(
        self,
        master_ip: Optional[str] = None,
        port: Optional[int] = None,
        hostname: Optional[str] = None,
        reconnect_delay: int = 5,
        reconnect_backoff_max: int = 60
    ):
        self.master_ip = master_ip or os.getenv("MASTER_IP", "localhost")
        self.port = port or int(os.getenv("PORT", 8000))
        self.hostname = hostname or socket.gethostname()
        self.api_key = secrets.token_urlsafe(32)
        self.reconnect_delay = reconnect_delay
        self.reconnect_backoff_max = reconnect_backoff_max
        self._shutdown_event = asyncio.Event()
        self._current_reconnect_delay = reconnect_delay
        
    async def execute_command(self, command: str, timeout: int, use_sudo: bool):
        """Execute a command and return the result."""
        try:
            cmd = f"sudo {command}" if use_sudo else command
            
            process = await asyncio.create_subprocess_shell(
                cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=os.environ.copy()
            )
            
            try:
                stdout, stderr = await asyncio.wait_for(
                    process.communicate(),
                    timeout=timeout
                )
                
                return {
                    "stdout": stdout.decode('utf-8', errors='replace'),
                    "stderr": stderr.decode('utf-8', errors='replace'),
                    "returncode": process.returncode,
                    "status": "success" if process.returncode == 0 else "error"
                }
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
                return {
                    "stdout": "",
                    "stderr": f"Command timed out after {timeout} seconds",
                    "returncode": -1,
                    "status": "timeout"
                }
        except Exception as e:
            return {
                "stdout": "",
                "stderr": f"Error executing command: {str(e)}",
                "returncode": -1,
                "status": "error"
            }
    
    async def connect_to_master(self):
        """Connect to master and handle commands with exponential backoff."""
        uri = f"ws://{self.master_ip}:{self.port}"
        
        while not self._shutdown_event.is_set():
            try:
                print(f"[Slave] Connecting to master at {uri}...")
                
                async with websockets.connect(
                    uri,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5
                ) as websocket:
                    # Reset reconnect delay on successful connection
                    self._current_reconnect_delay = self.reconnect_delay
                    
                    # Register with master
                    registration = {
                        "type": "register",
                        "hostname": self.hostname,
                        "api_key": self.api_key
                    }
                    await websocket.send(json.dumps(registration))
                    print(f"[Slave] Sent registration: {self.hostname}")
                    
                    # Wait for acknowledgment with timeout
                    try:
                        ack = await asyncio.wait_for(websocket.recv(), timeout=10)
                        ack_data = json.loads(ack)
                        print(f"[Slave] Received from master: {ack_data}")
                        
                        if ack_data.get("status") != "registered":
                            print(f"[Slave] Registration failed: {ack_data}")
                            await asyncio.sleep(self._current_reconnect_delay)
                            continue
                        
                        print(f"[Slave] Successfully registered with master!")
                    except asyncio.TimeoutError:
                        print("[Slave] Registration timeout - no response from master")
                        continue
                    
                    # Main loop: handle commands and health checks
                    try:
                        async for message in websocket:
                            if self._shutdown_event.is_set():
                                break
                                
                            try:
                                data = json.loads(message)
                                
                                if data.get("type") == "healthcheck":
                                    # Respond to health check
                                    await websocket.send(json.dumps({
                                        "type": "healthcheck_response",
                                        "hostname": self.hostname,
                                        "status": "healthy"
                                    }))
                                
                                elif data.get("type") == "command":
                                    # Execute command in background to not block health checks
                                    asyncio.create_task(
                                        self._handle_command(websocket, data)
                                    )
                            
                            except json.JSONDecodeError as e:
                                print(f"[Slave] Failed to parse message: {e}")
                            except Exception as e:
                                print(f"[Slave] Error processing message: {e}")
                    
                    except websockets.exceptions.ConnectionClosed:
                        print("[Slave] Connection to master closed")
            
            except websockets.exceptions.WebSocketException as e:
                print(f"[Slave] WebSocket error: {e}")
            except OSError as e:
                print(f"[Slave] Network error: {e}")
            except Exception as e:
                print(f"[Slave] Unexpected error: {e}")
            
            if not self._shutdown_event.is_set():
                print(f"[Slave] Reconnecting in {self._current_reconnect_delay}s...")
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(),
                        timeout=self._current_reconnect_delay
                    )
                except asyncio.TimeoutError:
                    pass
                
                # Exponential backoff
                self._current_reconnect_delay = min(
                    self._current_reconnect_delay * 2,
                    self.reconnect_backoff_max
                )
        
        print("[Slave] Shutdown complete")
    
    async def _handle_command(self, websocket, data):
        """Handle command execution and send result back to master."""
        try:
            # Execute command
            result = await self.execute_command(
                data["command"],
                data.get("timeout", 30),
                data.get("use_sudo", False)
            )
            result["type"] = "command_result"
            result["hostname"] = self.hostname
            result["command_id"] = data.get("command_id")
            
            await websocket.send(json.dumps(result))
            
            # Log with truncated command for readability
            cmd_preview = data['command'][:50] + ('...' if len(data['command']) > 50 else '')
            status = result['status']
            print(f"[Slave] Executed command: {cmd_preview} [status: {status}]")
            
        except Exception as e:
            print(f"[Slave] Error handling command: {e}")
            # Try to send error result
            try:
                error_result = {
                    "type": "command_result",
                    "hostname": self.hostname,
                    "command_id": data.get("command_id"),
                    "stdout": "",
                    "stderr": f"Internal error: {str(e)}",
                    "returncode": -1,
                    "status": "error"
                }
                await websocket.send(json.dumps(error_result))
            except:
                pass
    
    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        def signal_handler(signum, frame):
            print(f"\n[Slave] Received signal {signum}, initiating shutdown...")
            self._shutdown_event.set()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    async def run_async(self):
        """Async entry point for the slave node."""
        print(f"[Slave] Starting slave node: {self.hostname}")
        print(f"[Slave] Master: {self.master_ip}:{self.port}")
        print(f"[Slave] Generated API key: {self.api_key[:16]}...")
        
        try:
            await self.connect_to_master()
        except asyncio.CancelledError:
            print("[Slave] Run task cancelled")
            raise
        except Exception as e:
            print(f"[Slave] Fatal error: {e}")
            raise
    
    def run(self):
        """Start the slave node."""
        # Setup signal handlers
        self._setup_signal_handlers()
        
        try:
            asyncio.run(self.run_async())
        except KeyboardInterrupt:
            print("\n[Slave] Keyboard interrupt received")
        except Exception as e:
            print(f"[Slave] Fatal error: {e}")
            raise


if __name__ == "__main__":
    slave = ThunderboltSlave()
    slave.run()