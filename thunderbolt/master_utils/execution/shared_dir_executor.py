import asyncio
import json
import uuid
from typing import List, Dict
from datetime import datetime
from pathlib import Path
from fastapi import HTTPException

from .response_models import BatchedResponse, CommandResult


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
    ) -> List[CommandResult]:
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
        raw_results = await self._poll_results(command_id, nodes, timeout)

        # Reconstruct CommandResult list (preserve order of nodes)
        results: List[CommandResult] = []
        for hostname in nodes:
            if hostname in raw_results:
                rd = raw_results[hostname]
                results.append(CommandResult(
                    command_uuid=command_id,
                    node=hostname,
                    command=command,
                    stdout=rd.get("stdout"),
                    stderr=rd.get("stderr"),
                    exit_code=rd.get("exit_code"),
                    error=rd.get("error"),
                    timed_out=rd.get("timed_out", False)
                ))
            else:
                results.append(CommandResult(
                    command_uuid=command_id,
                    node=hostname,
                    command=command,
                    error="No response received",
                    timed_out=True
                ))

        # Cleanup job and result files
        self._cleanup_job(command_id, nodes)

        return results
 
    async def execute_batched(self, command_specs: List[dict]) -> BatchedResponse:
        """Execute batched commands via shared directory."""
        job_id = str(uuid.uuid4())
        
        # Build job data with command UUIDs
        job_data = {
            "type": "batched_command",
            "job_id": job_id,
            "commands": command_specs,
            "created_at": datetime.now().isoformat()
        }
        
        # Write job
        self._write_job(job_id, job_data)
        
        # Determine max timeout and collect unique nodes
        max_timeout = max(cmd["timeout"] for cmd in command_specs)
        nodes_set = {cmd["node"] for cmd in command_specs}
        
        print(f"[Thunderbolt] Broadcast batched job {job_id} with {len(command_specs)} commands to {len(nodes_set)} nodes")
        
        # Poll for completion - returns dict keyed by command_uuid
        results_dict = await self._poll_batched_results(job_id, command_specs, max_timeout)
        
        # Reconstruct results in original order
        results = []
        for cmd_spec in command_specs:
            cmd_uuid = cmd_spec["command_uuid"]
            if cmd_uuid in results_dict:
                result_data = results_dict[cmd_uuid]
                results.append(CommandResult(
                    command_uuid=cmd_uuid,
                    node=cmd_spec["node"],
                    command=cmd_spec["command"],
                    stdout=result_data.get("stdout"),
                    stderr=result_data.get("stderr"),
                    exit_code=result_data.get("exit_code"),
                    error=result_data.get("error"),
                    timed_out=result_data.get("timed_out", False)
                ))
            else:
                # Command timed out or failed
                results.append(CommandResult(
                    command_uuid=cmd_uuid,
                    node=cmd_spec["node"],
                    command=cmd_spec["command"],
                    error="No response received",
                    timed_out=True
                ))
        
        # Cleanup
        self._cleanup_job(job_id, list(nodes_set))
        
        return BatchedResponse(
            total_commands=len(command_specs),
            total_nodes=len(nodes_set),
            method="shared_directory",
            results=results
        )
    
    def _write_job(self, job_id: str, job_data: dict):
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
            
            jobs[job_id] = job_data
            print(f"[Thunderbolt] Adding job {job_id}, total jobs: {len(jobs)}")
            
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
    
    async def _poll_batched_results(
        self, 
        job_id: str, 
        command_specs: List[dict], 
        timeout: int
    ) -> Dict[str, dict]:
        """Poll for batched command results, keyed by command_uuid."""
        start_time = datetime.now()
        results = {}
        completed_uuids = set()
        total_commands = len(command_specs)
        
        # Create lookup for command specs by UUID
        uuid_to_spec = {cmd["command_uuid"]: cmd for cmd in command_specs}
        
        while len(completed_uuids) < total_commands:
            elapsed = (datetime.now() - start_time).total_seconds()
            if elapsed > timeout + 10:
                print(f"[Thunderbolt] Batched job {job_id} timed out")
                break
            
            for cmd_uuid, spec in uuid_to_spec.items():
                if cmd_uuid in completed_uuids:
                    continue
                
                node = spec["node"]
                node_dir = self.shared_dir / node
                result_file = node_dir / f"{cmd_uuid}.json"
                
                if result_file.exists():
                    try:
                        with open(result_file, 'r') as f:
                            result_data = json.load(f)
                        results[cmd_uuid] = result_data
                        completed_uuids.add(cmd_uuid)
                    except Exception as e:
                        print(f"[Thunderbolt] Error reading result for {cmd_uuid} from {node}: {e}")
            
            await asyncio.sleep(self.poll_interval)
        
        return results
    
    def _cleanup_job(self, job_id: str, nodes: List[str]):
        """Cleanup job and result files."""
        try:
            jobs = {}
            if self.jobs_file.exists():
                with open(self.jobs_file, 'r') as f:
                    jobs = json.load(f)
            
            if job_id in jobs:
                del jobs[job_id]
                temp_file = self.jobs_file.with_suffix('.tmp')
                with open(temp_file, 'w') as f:
                    json.dump(jobs, f, indent=2)
                temp_file.replace(self.jobs_file)
            
            for hostname in nodes:
                node_dir = self.shared_dir / hostname
                result_file = node_dir / f"{job_id}.json"
                if result_file.exists():
                    result_file.unlink()
                    
        except Exception as e:
            print(f"[Thunderbolt] Error cleaning up job {job_id}: {e}")