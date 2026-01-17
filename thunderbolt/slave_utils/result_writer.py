import json
from pathlib import Path
from datetime import datetime


class ResultWriter:
    """Writes command results to shared storage."""
    
    def __init__(self, directory_manager):
        self.dir_mgr = directory_manager
    
    async def write_command_result(
        self,
        command_id: str,
        command: str,
        result: dict,
        job_received_at: str
    ):
        """Write a single command result to file with timing information."""
        completion_time = datetime.now()
        
        output = {
            "type": "command_result",
            "command_id": command_id,
            "hostname": self.dir_mgr.hostname,
            "command": command,
            "job_received_at": job_received_at,
            "job_completed_at": completion_time.isoformat(),
            **result
        }
        
        # Calculate total latency if we have execution times
        if "execution_start" in result and "execution_end" in result:
            try:
                received = datetime.fromisoformat(job_received_at)
                exec_start = datetime.fromisoformat(result["execution_start"])
                exec_end = datetime.fromisoformat(result["execution_end"])
                completed = completion_time
                
                # Time from job received to execution start (queuing/overhead)
                queue_time = (exec_start - received).total_seconds()
                
                # Time from execution end to writing result (I/O overhead)
                write_overhead = (completed - exec_end).total_seconds()
                
                # Total end-to-end latency
                total_latency = (completed - received).total_seconds()
                
                output["timing_breakdown"] = {
                    "queue_time_seconds": round(queue_time, 3),
                    "execution_time_seconds": result.get("execution_duration_seconds"),
                    "write_overhead_seconds": round(write_overhead, 3),
                    "total_latency_seconds": round(total_latency, 3)
                }
            except:
                pass
        
        try:
            result_file = self.dir_mgr.get_result_path(command_id)
            with open(result_file, 'w') as f:
                json.dump(output, f, indent=2)
            print(f"[ResultWriter] Wrote result for {command_id}")
        except Exception as e:
            print(f"[ResultWriter] Failed to write {command_id}: {e}")
    
    async def write_batch_result(
        self,
        command_id: str,
        batch_result: dict,
        job_received_at: str
    ):
        """Write batch command results to file with timing information."""
        completion_time = datetime.now()
        
        # Add timing metadata
        batch_result["job_received_at"] = job_received_at
        batch_result["job_completed_at"] = completion_time.isoformat()
        
        # Calculate total latency
        if "batch_start" in batch_result and "batch_end" in batch_result:
            try:
                received = datetime.fromisoformat(job_received_at)
                batch_start = datetime.fromisoformat(batch_result["batch_start"])
                batch_end = datetime.fromisoformat(batch_result["batch_end"])
                completed = completion_time
                
                queue_time = (batch_start - received).total_seconds()
                write_overhead = (completed - batch_end).total_seconds()
                total_latency = (completed - received).total_seconds()
                
                batch_result["timing_breakdown"] = {
                    "queue_time_seconds": round(queue_time, 3),
                    "batch_execution_time_seconds": batch_result.get("batch_total_duration_seconds"),
                    "write_overhead_seconds": round(write_overhead, 3),
                    "total_latency_seconds": round(total_latency, 3)
                }
            except:
                pass
        
        try:
            result_file = self.dir_mgr.get_result_path(command_id)
            with open(result_file, 'w') as f:
                json.dump(batch_result, f, indent=2)
            print(f"[ResultWriter] Wrote batch result for {command_id}")
        except Exception as e:
            print(f"[ResultWriter] Failed to write batch {command_id}: {e}")