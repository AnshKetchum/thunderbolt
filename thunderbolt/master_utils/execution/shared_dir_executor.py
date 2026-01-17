import asyncio
import json
import uuid
from typing import List, Dict
from datetime import datetime
from pathlib import Path
from fastapi import HTTPException


class SharedDirExecutor:
    """Executes commands via shared directory broadcast."""
    
    def __init__(self, shared_dir: Path, poll_interval: float):
        self.shared_dir = shared_dir
        self.poll_interval = poll_interval
        self.jobs_file = None
        
        self._setup_shared_directory()
    
    def _setup_shared_directory(self):
        """Initialize shared directory structure."""
        try:
            self.shared_dir.mkdir(parents=True, exist_ok=True)
            self.jobs_file = self.shared_dir / "jobs.json"
            
            if not self.jobs_file.exists():
                with open(self.jobs_file, 'w') as f:
                    json.dump({}, f)
            
            print(f"[Thunderbolt] Shared directory initialized: {self.shared_dir}")
            print(f"[Thunderbolt] Jobs file: {self.jobs_file}")
            
        except Exception as e:
            print(f"[Thunderbolt] Failed to setup shared directory: {e}")
            raise
    
    async def execute_command(
        self,
        command: str,
        nodes: List[str],
        timeout: int,
        use_sudo: bool
    ) -> Dict:
        """Execute command via shared directory broadcast."""
        command_id = str(uuid.uuid4())
        
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
        self._write_job(command_id, job_data)
        
        print(f"[Thunderbolt] Broadcast job {command_id} to {len(nodes)} nodes via shared dir")
        
        # Poll for completion
        results = await self._poll_results(command_id, nodes, timeout)
        
        return {
            "command": command,
            "total_nodes": len(nodes),
            "responses_received": len(results),
            "method": "shared_directory",
            "results": results
        }
    
    async def execute_batched(self, node_queues: Dict[str, List[dict]]) -> Dict:
        """Execute batched commands via shared directory."""
        command_id = str(uuid.uuid4())
        
        job_data = {
            "type": "batched_command",
            "command_id": command_id,
            "node_commands": node_queues,
            "created_at": datetime.now().isoformat()
        }
        
        # Write job
        self._write_job(command_id, job_data)
        
        # Determine max timeout
        max_timeout = max(
            max(cmd["timeout"] for cmd in cmds)
            for cmds in node_queues.values()
        )
        
        # Poll for completion
        results = await self._poll_results(command_id, list(node_queues.keys()), max_timeout)
        
        # Cleanup
        # self._cleanup_job(command_id, list(node_queues.keys()))
        
        return {
            "total_commands": sum(len(cmds) for cmds in node_queues.values()),
            "total_nodes": len(node_queues),
            "method": "shared_directory",
            "results": results
        }
    
    def _write_job(self, command_id: str, job_data: dict):
        """Write job to jobs file atomically."""
        try:
            jobs = {}
            if self.jobs_file.exists():
                with open(self.jobs_file, 'r') as f:
                    try:
                        jobs = json.load(f)
                        print(f"[Thunderbolt] Loaded {len(jobs)} existing jobs from file")
                    except json.JSONDecodeError as e:
                        print(f"[Thunderbolt] WARNING: JSON decode error in jobs.json: {e}")
                        print(f"[Thunderbolt] Resetting jobs to empty dict")
                        jobs = {}
            
            jobs[command_id] = job_data
            print(f"[Thunderbolt] Adding job {command_id}, total jobs: {len(jobs)}")
            
            temp_file = self.jobs_file.with_suffix('.tmp')
            with open(temp_file, 'w') as f:
                json.dump(jobs, f, indent=2)
            temp_file.replace(self.jobs_file)
            
            print(f"[Thunderbolt] Successfully wrote jobs.json with {len(jobs)} jobs")
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to write job to shared directory: {e}"
            ) 

    async def _poll_results(self, command_id: str, nodes: List[str], timeout: int) -> Dict:
        """Poll for command results."""
        start_time = datetime.now()
        results = {}
        completed_nodes = set()
        
        while len(completed_nodes) < len(nodes):
            elapsed = (datetime.now() - start_time).total_seconds()
            if elapsed > timeout + 10:
                print(f"[Thunderbolt] Job {command_id} timed out")
                break
            
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
            
            await asyncio.sleep(self.poll_interval)
        
        return results
    
    def _cleanup_job(self, command_id: str, nodes: List[str]):
        """Cleanup job and result files."""
        pass
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
            
        #     for hostname in nodes:
        #         node_dir = self.shared_dir / hostname
        #         result_file = node_dir / f"{command_id}.json"
        #         if result_file.exists():
        #             result_file.unlink()
                    
        # except Exception as e:
        #     print(f"[Thunderbolt] Error cleaning up job {command_id}: {e}")