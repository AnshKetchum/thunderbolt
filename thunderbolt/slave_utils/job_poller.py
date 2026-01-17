import asyncio
import json
from datetime import datetime


class JobPoller:
    """Polls shared directory for new jobs to execute."""
    
    def __init__(
        self,
        directory_manager,
        result_writer,
        command_executor,
        batch_executor,
        poll_interval: float = 0.5
    ):
        self.dir_mgr = directory_manager
        self.result_writer = result_writer
        self.command_executor = command_executor
        self.batch_executor = batch_executor
        self.poll_interval = poll_interval
        
        self.processed_jobs = set()
        self._shutdown_event = asyncio.Event()
    
    async def start_polling(self):
        """Start polling for new jobs."""
        print("[JobPoller] Starting...")
        
        try:
            while not self._shutdown_event.is_set():
                try:
                    await self._poll_once()
                except Exception as e:
                    print(f"[JobPoller] Error: {e}")
                
                await asyncio.sleep(self.poll_interval)
        
        except asyncio.CancelledError:
            print("[JobPoller] Cancelled")
            raise
    
    async def _poll_once(self):
        """Single poll iteration."""
        # Check if jobs file exists
        if not self.dir_mgr.jobs_file.exists():
            return
        
        # Read jobs
        with open(self.dir_mgr.jobs_file, 'r') as f:
            try:
                jobs = json.load(f)
            except json.JSONDecodeError:
                jobs = {}
        
        # Process each job
        for command_id, job_data in jobs.items():
            await self._process_job(command_id, job_data)
        
        # Clean up processed jobs list
        self._cleanup_processed_jobs(jobs)
    
    async def _process_job(self, command_id: str, job_data: dict):
        """Process a single job if applicable."""
        # Skip if already processed
        if command_id in self.processed_jobs:
            return
        
        # Check if result already exists
        if self.dir_mgr.result_exists(command_id):
            self.processed_jobs.add(command_id)
            return
        
        job_type = job_data.get("type")
        
        # Record when we first saw this job
        job_received_at = datetime.now().isoformat()
        
        if job_type == "command":
            await self._handle_simple_command(command_id, job_data, job_received_at)
        elif job_type == "batched_command":
            await self._handle_batched_command(command_id, job_data, job_received_at)
    
    async def _handle_simple_command(self, command_id: str, job_data: dict, job_received_at: str):
        """Handle a simple command job."""
        nodes = job_data.get("nodes", [])
        if self.dir_mgr.hostname not in nodes:
            return
        
        print(f"[JobPoller] Processing command job {command_id}")
        
        # Execute command
        result = await self.command_executor.execute(
            command_id=command_id,
            command=job_data.get("command"),
            timeout=job_data.get("timeout", 30),
            use_sudo=job_data.get("use_sudo", False)
        )
        
        # Write result with timing information
        await self.result_writer.write_command_result(
            command_id=command_id,
            command=job_data.get("command"),
            result=result,
            job_received_at=job_received_at
        )
        
        self.processed_jobs.add(command_id)
    
    async def _handle_batched_command(self, command_id: str, job_data: dict, job_received_at: str):
        """Handle a batched command job."""
        node_commands = job_data.get("node_commands", {})
        if self.dir_mgr.hostname not in node_commands:
            return
        
        print(f"[JobPoller] Processing batched job {command_id}")
        
        # Execute batch
        batch_result = await self.batch_executor.execute_batch(
            command_id=command_id,
            commands=node_commands[self.dir_mgr.hostname]
        )
        
        # Write result with timing information
        await self.result_writer.write_batch_result(
            command_id=command_id,
            batch_result=batch_result,
            job_received_at=job_received_at
        )
        
        self.processed_jobs.add(command_id)
    
    def _cleanup_processed_jobs(self, current_jobs: dict):
        """Clean up old processed job IDs."""
        if len(self.processed_jobs) > 1000:
            # Keep current jobs + last 500 processed
            current_ids = set(current_jobs.keys())
            self.processed_jobs = current_ids.union(
                list(self.processed_jobs)[-500:]
            )
    
    async def shutdown(self):
        """Stop polling."""
        self._shutdown_event.set()