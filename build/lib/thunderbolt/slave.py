import asyncio
import websockets
import json
import os
import secrets
import socket
import subprocess
from typing import Optional

class ThunderboltSlave:
    """Slave node that connects to master and executes commands."""
    
    def __init__(
        self,
        master_ip: Optional[str] = None,
        port: Optional[int] = None,
        hostname: Optional[str] = None
    ):
        self.master_ip = master_ip or os.getenv("MASTER_IP", "localhost")
        self.port = port or int(os.getenv("PORT", 8000))
        self.hostname = hostname or socket.gethostname()
        self.api_key = secrets.token_urlsafe(32)
        
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
                    "stdout": stdout.decode(),
                    "stderr": stderr.decode(),
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
        """Connect to master and handle commands."""
        uri = f"ws://{self.master_ip}:{self.port}"
        
        while True:
            try:
                print(f"Connecting to master at {uri}...")
                async with websockets.connect(uri) as websocket:
                    # Register with master
                    registration = {
                        "type": "register",
                        "hostname": self.hostname,
                        "api_key": self.api_key
                    }
                    await websocket.send(json.dumps(registration))
                    print(f"Sent registration: {self.hostname}")
                    
                    # Wait for acknowledgment
                    ack = await websocket.recv()
                    ack_data = json.loads(ack)
                    print(f"Received from master: {ack_data}")
                    
                    if ack_data.get("status") != "registered":
                        print(f"Registration failed: {ack_data}")
                        await asyncio.sleep(5)
                        continue
                    
                    print(f"Successfully registered with master!")
                    
                    # Main loop: handle commands and health checks
                    async for message in websocket:
                        data = json.loads(message)
                        
                        if data.get("type") == "healthcheck":
                            # Respond to health check
                            await websocket.send(json.dumps({
                                "type": "healthcheck_response",
                                "hostname": self.hostname,
                                "status": "healthy"
                            }))
                        
                        elif data.get("type") == "command":
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
                            print(f"Executed command: {data['command'][:50]}...")
            
            except websockets.exceptions.ConnectionClosed:
                print("Connection to master closed. Reconnecting in 5s...")
                await asyncio.sleep(5)
            except Exception as e:
                print(f"Error: {e}. Reconnecting in 5s...")
                await asyncio.sleep(5)
    
    def run(self):
        """Start the slave node."""
        print(f"Slave node starting: {self.hostname}")
        print(f"Generated API key: {self.api_key}")
        asyncio.run(self.connect_to_master())

if __name__ == "__main__":
    slave = ThunderboltSlave()
    slave.run()