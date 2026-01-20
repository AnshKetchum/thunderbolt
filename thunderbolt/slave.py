import asyncio
import os
import socket
from pathlib import Path
from typing import Optional

from .slave_utils.command_channel import CommandChannel
from .slave_utils.health_channel import HealthChannel
from .slave_utils.command_executor import CommandExecutor
from .slave_utils.batch_executor import BatchExecutor
from .slave_utils.directory_manager import DirectoryManager
from .slave_utils.result_writer import ResultWriter
from .slave_utils.job_poller import JobPoller
from .slave_utils.signal_handler import SignalHandler


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
        shared_dir_poll_interval: float = 0.5,
        privileged_execution: bool = False
    ):
        self.master_host = master_host
        self.command_port = command_port
        self.health_port = health_port
        self.api_key = api_key or os.getenv("THUNDERBOLT_API_KEY", "default-key")
        self.hostname = hostname or socket.gethostname()
        self.reconnect_interval = reconnect_interval
        self.max_reconnect_attempts = max_reconnect_attempts
        self.privileged_execution = privileged_execution
        
        # Initialize components
        self.command_executor = CommandExecutor(self.hostname, privileged_execution)
        self.batch_executor = BatchExecutor(self.hostname, privileged_execution)
        
        # Shared directory components
        self.dir_manager = None
        self.result_writer = None
        self.job_poller = None
        
        if shared_dir:
            self._setup_shared_storage(Path(shared_dir), shared_dir_poll_interval)
        
        # WebSocket channels
        self.command_channel = CommandChannel(
            master_host=master_host,
            port=command_port,
            hostname=self.hostname,
            api_key=self.api_key,
            reconnect_interval=reconnect_interval,
            max_reconnect_attempts=max_reconnect_attempts,
            command_executor=self.command_executor
        )
        
        self.health_channel = HealthChannel(
            master_host=master_host,
            port=health_port,
            hostname=self.hostname,
            api_key=self.api_key,
            reconnect_interval=reconnect_interval,
            max_reconnect_attempts=max_reconnect_attempts
        )
        
        # Control
        self._shutdown_event = asyncio.Event()
        self.tasks = []
    
    def _setup_shared_storage(self, shared_dir: Path, poll_interval: float):
        """Initialize shared storage components."""
        self.dir_manager = DirectoryManager(shared_dir, self.hostname)
        self.dir_manager.setup()
        
        self.result_writer = ResultWriter(self.dir_manager)
        
        self.job_poller = JobPoller(
            directory_manager=self.dir_manager,
            result_writer=self.result_writer,
            command_executor=self.command_executor,
            batch_executor=self.batch_executor,
            poll_interval=poll_interval
        )
    
    async def start(self):
        """Start the slave and connect to master."""
        print(f"[Slave] Starting: {self.hostname}")
        print(f"[Slave] Master: {self.master_host}")
        print(f"[Slave] Command port: {self.command_port}")
        print(f"[Slave] Health port: {self.health_port}")
        if self.dir_manager:
            print(f"[Slave] Shared directory: {self.dir_manager.shared_dir}")
        
        self._shutdown_event.clear()
        
        # Start connection tasks
        command_task = asyncio.create_task(
            self.command_channel.connect(),
            name="command-channel"
        )
        health_task = asyncio.create_task(
            self.health_channel.connect(),
            name="health-channel"
        )
        
        self.tasks = [command_task, health_task]
        
        # Start shared directory polling if configured
        if self.job_poller:
            poll_task = asyncio.create_task(
                self.job_poller.start_polling(),
                name="job-poller"
            )
            self.tasks.append(poll_task)
        
        # Wait for connections
        try:
            await asyncio.wait_for(
                asyncio.gather(
                    self.command_channel._connected.wait(),
                    self.health_channel._connected.wait()
                ),
                timeout=30
            )
            print("[Slave] All channels connected")
        except asyncio.TimeoutError:
            print("[Slave] Warning: Not all connections established within 30s")
        
        # Wait for shutdown
        await self._shutdown_event.wait()
    
    async def shutdown(self):
        """Gracefully shutdown the slave."""
        print("[Slave] Initiating shutdown...")
        
        self._shutdown_event.set()
        
        # Shutdown channels
        await asyncio.gather(
            self.command_channel.shutdown(),
            self.health_channel.shutdown(),
            return_exceptions=True
        )
        
        # Shutdown job poller
        if self.job_poller:
            await self.job_poller.shutdown()
        
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
        
        # Setup signal handlers
        signal_handler = SignalHandler(self.shutdown)
        signal_handler.setup(loop)
        
        try:
            loop.run_until_complete(self.start())
        except KeyboardInterrupt:
            print("\n[Slave] Keyboard interrupt")
        finally:
            loop.run_until_complete(self.shutdown())
            loop.close()