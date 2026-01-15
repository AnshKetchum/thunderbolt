from fastapi import FastAPI, HTTPException, APIRouter
from pydantic import BaseModel
import asyncio
import websockets
from websockets.server import serve
import json
import uuid
from typing import List, Dict, Optional
from datetime import datetime
import os
from contextlib import asynccontextmanager

class ThunderBoltMaster:
    """Master node that manages slave connections and executes commands."""
    
    def __init__(
        self,
        port: Optional[int] = None,
        health_check_interval: int = 10,
        max_failed_healthchecks: int = 15,
        no_app: bool = False
    ):
        self.port = port or int(os.getenv("PORT", 8000))
        self.health_check_interval = health_check_interval
        self.max_failed_healthchecks = max_failed_healthchecks
        self.no_app = no_app
        
        # Store connected slaves
        self.slaves: Dict[str, dict] = {}
        # hostname -> {"websocket": ws, "api_key": key, "last_seen": timestamp, "failed_healthchecks": 0}
        
        # Store pending command responses
        self.pending_commands: Dict[str, dict] = {}
        # command_id -> {"responses": {}, "total_nodes": int, "event": asyncio.Event()}
        
        # Create router
        self.router = self._create_router()
        
        # Only create app if no_app is False
        self.app = None if no_app else self._create_app()
    
    def _create_router(self) -> APIRouter:
        """Create the FastAPI router with all endpoints."""
        router = APIRouter()
        
        class CommandRequest(BaseModel):
            command: str
            nodes: List[str]
            timeout: Optional[int] = 30
            use_sudo: Optional[bool] = False
        
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
            
            # Generate unique command ID
            command_id = str(uuid.uuid4())
            
            # Prepare pending command tracking
            self.pending_commands[command_id] = {
                "responses": {},
                "total_nodes": len(request.nodes),
                "event": asyncio.Event()
            }
            
            # Send command to all specified nodes in parallel
            command_msg = {
                "type": "command",
                "command_id": command_id,
                "command": request.command,
                "timeout": request.timeout,
                "use_sudo": request.use_sudo
            }
            
            send_tasks = []
            for hostname in request.nodes:
                slave_info = self.slaves[hostname]
                send_tasks.append(
                    slave_info["websocket"].send(json.dumps(command_msg))
                )
            
            # Send all commands
            await asyncio.gather(*send_tasks)
            
            # Wait for all responses (with timeout)
            try:
                await asyncio.wait_for(
                    self.pending_commands[command_id]["event"].wait(),
                    timeout=request.timeout + 5
                )
            except asyncio.TimeoutError:
                pass
            
            # Collect results
            results = self.pending_commands[command_id]["responses"]
            
            # Cleanup
            del self.pending_commands[command_id]
            
            # Format response
            return {
                "command": request.command,
                "total_nodes": len(request.nodes),
                "responses_received": len(results),
                "results": results
            }
        
        @router.get("/nodes")
        async def list_nodes():
            """List all connected slave nodes."""
            return {
                "total": len(self.slaves),
                "nodes": [
                    {
                        "hostname": hostname,
                        "last_seen": info["last_seen"].isoformat(),
                        "failed_healthchecks": info["failed_healthchecks"]
                    }
                    for hostname, info in self.slaves.items()
                ]
            }
        
        @router.get("/")
        async def root():
            return {
                "message": "Master Command Runner",
                "connected_slaves": len(self.slaves)
            }
        
        @router.get("/health")
        async def health():
            return {"status": "healthy", "slaves": len(self.slaves)}
        
        return router
    
    async def handle_slave_connection(self, websocket):
        """Handle incoming slave connections."""
        hostname = None
        try:
            # Receive registration
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
            
            # Store slave info
            self.slaves[hostname] = {
                "websocket": websocket,
                "api_key": api_key,
                "last_seen": datetime.now(),
                "failed_healthchecks": 0
            }
            
            # Send acknowledgment
            await websocket.send(json.dumps({
                "status": "registered",
                "hostname": hostname
            }))
            
            print(f"Slave registered: {hostname}")
            
            # Handle incoming messages from slave
            async for message in websocket:
                data = json.loads(message)
                
                if data.get("type") == "healthcheck_response":
                    self.slaves[hostname]["last_seen"] = datetime.now()
                    self.slaves[hostname]["failed_healthchecks"] = 0
                
                elif data.get("type") == "command_result":
                    command_id = data.get("command_id")
                    if command_id in self.pending_commands:
                        self.pending_commands[command_id]["responses"][hostname] = data
                        
                        # Check if all responses received
                        if len(self.pending_commands[command_id]["responses"]) >= self.pending_commands[command_id]["total_nodes"]:
                            self.pending_commands[command_id]["event"].set()
        
        except websockets.exceptions.ConnectionClosed:
            print(f"Slave disconnected: {hostname}")
        except Exception as e:
            print(f"Error handling slave {hostname}: {e}")
        finally:
            if hostname and hostname in self.slaves:
                del self.slaves[hostname]
                print(f"Removed slave: {hostname}")
    
    async def health_check_loop(self):
        """Periodically health check all slaves."""
        while True:
            await asyncio.sleep(self.health_check_interval)
            
            disconnected_slaves = []
            
            for hostname, slave_info in list(self.slaves.items()):
                try:
                    # Send health check
                    await slave_info["websocket"].send(json.dumps({
                        "type": "healthcheck"
                    }))
                    
                    # Increment failed counter (will be reset if response received)
                    slave_info["failed_healthchecks"] += 1
                    
                    # Check if slave has failed too many health checks
                    if slave_info["failed_healthchecks"] >= self.max_failed_healthchecks:
                        print(f"Slave {hostname} failed {self.max_failed_healthchecks} health checks. Disconnecting.")
                        disconnected_slaves.append(hostname)
                        await slave_info["websocket"].close()
                
                except Exception as e:
                    print(f"Health check failed for {hostname}: {e}")
                    disconnected_slaves.append(hostname)
            
            # Remove disconnected slaves
            for hostname in disconnected_slaves:
                if hostname in self.slaves:
                    del self.slaves[hostname]
    
    async def websocket_server(self):
        """Run the WebSocket server for slave connections."""
        async with serve(self.handle_slave_connection, "0.0.0.0", self.port):
            print(f"WebSocket server listening on port {self.port}")
            await asyncio.Future()  # run forever
    
    def start_background_tasks(self):
        """Start websocket server and health check loop as background tasks."""
        asyncio.create_task(self.websocket_server())
        asyncio.create_task(self.health_check_loop())
    
    def _create_app(self) -> FastAPI:
        """Create the FastAPI app with lifespan management."""
        @asynccontextmanager
        async def lifespan(app: FastAPI):
            # Startup
            self.start_background_tasks()
            yield
            # Shutdown (if needed)
            pass
        
        app = FastAPI(lifespan=lifespan)
        app.include_router(self.router)
        return app
    
    def run(self, host: str = "0.0.0.0", port: Optional[int] = None):
        """Run the master server."""
        if self.no_app:
            raise RuntimeError("Cannot call run() when no_app=True. Use the router in your own FastAPI app.")
        
        import uvicorn
        api_port = port or (self.port + 1)
        print(f"Starting ThunderBolt Master on port {api_port} (WebSocket on {self.port})")
        uvicorn.run(self.app, host=host, port=api_port)

if __name__ == "__main__":
    master = ThunderBoltMaster()
    master.run()