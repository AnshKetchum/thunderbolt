from fastapi import FastAPI
import asyncio
import websockets
from websockets.server import serve
from typing import List, Dict, Optional
from datetime import datetime
import os
from contextlib import asynccontextmanager
from pathlib import Path

from .master_utils.routes import create_router
from .master_utils.websocket_handlers import WebSocketHandlers
from .master_utils.execution.websocket_executor import WebSocketExecutor
from .master_utils.execution.shared_dir_executor import SharedDirExecutor


class ThunderboltMaster:
    """Master node that manages slave connections and executes commands."""
    
    def __init__(
        self,
        port: Optional[int] = None,
        health_check_port: Optional[int] = None,
        health_check_interval: int = 10,
        max_failed_healthchecks: int = 15,
        no_app: bool = False, 
        routes_prefix: Optional[str] = None,
        max_concurrent_sends: int = 2048,
        shared_dir: Optional[str] = None,
        shared_dir_poll_interval: float = 0.5,
        shared_dir_threshold: int = 10
    ):
        self.port = port or int(os.getenv("PORT", 8000))
        self.health_check_port = health_check_port or (self.port + 100)
        self.health_check_interval = health_check_interval
        self.max_failed_healthchecks = max_failed_healthchecks
        self.no_app = no_app
        self.routes_prefix = routes_prefix
        
        # Shared directory configuration
        self.shared_dir = Path(shared_dir) if shared_dir else None
        self.shared_dir_poll_interval = shared_dir_poll_interval
        self.shared_dir_threshold = shared_dir_threshold
        
        # Store connected slaves
        self.slaves: Dict[str, dict] = {}
        
        # Store pending command responses
        self.pending_commands: Dict[str, dict] = {}
        
        # Track background tasks
        self.background_tasks: List[asyncio.Task] = []
        self.command_server_obj = None
        self.health_server_obj = None
        self._shutdown_event = asyncio.Event()
        
        # Semaphore for rate limiting concurrent sends
        self._send_semaphore = asyncio.Semaphore(max_concurrent_sends)
        
        # Lock for thread-safe slave dict modifications
        self._slaves_lock = asyncio.Lock()
        
        # Initialize executors
        self.ws_executor = WebSocketExecutor(
            self.slaves,
            self.pending_commands,
            self._send_semaphore
        )
        
        self.shared_dir_executor = None
        if self.shared_dir:
            self.shared_dir_executor = SharedDirExecutor(
                self.shared_dir,
                self.shared_dir_poll_interval
            )
        
        # Initialize WebSocket handlers
        self.ws_handlers = WebSocketHandlers(
            self.slaves,
            self.pending_commands,
            self._slaves_lock
        )
        
        # Create router
        self.router = create_router(self)
        
        # Only create app if no_app is False
        self.app = None if no_app else self._create_app()
    
    def _should_use_shared_dir(self, num_nodes: int) -> bool:
        """Determine if shared directory should be used based on node count."""
        return self.shared_dir_executor is not None and num_nodes >= self.shared_dir_threshold
    
    async def command_server(self):
        """Run the WebSocket server for command connections."""
        try:
            self.command_server_obj = await serve(
                self.ws_handlers.handle_command_connection, 
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
                self.ws_handlers.handle_health_connection, 
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
                
                await self.ws_handlers.perform_health_checks(
                    self.max_failed_healthchecks
                )
        
        except asyncio.CancelledError:
            print("[Thunderbolt] Health check loop cancelled")
            raise
        except Exception as e:
            print(f"[Thunderbolt] Health check loop error: {e}")
    
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
        
        # Get snapshot WITHOUT lock
        slaves_snapshot = list(self.slaves.items())
        
        # Close connections WITHOUT holding lock
        close_tasks = []
        for hostname, slave_info in slaves_snapshot:
            try:
                if slave_info.get("command_ws"):
                    close_tasks.append(slave_info["command_ws"].close())
                if slave_info.get("health_ws"):
                    close_tasks.append(slave_info["health_ws"].close())
            except Exception as e:
                print(f"[Thunderbolt] Error closing connection to {hostname}: {e}")
        
        if close_tasks:
            await asyncio.gather(*close_tasks, return_exceptions=True)
        
        # Cancel background tasks
        for task in self.background_tasks:
            if not task.done():
                task.cancel()
        
        if self.background_tasks:
            await asyncio.gather(*self.background_tasks, return_exceptions=True)
        
        # Clear everything at the end (no lock needed - single threaded at this point)
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