from datetime import datetime
from time import perf_counter
from .command_executor import CommandExecutor

class BatchExecutor:
    """Executes batches of commands sequentially."""
    
    def __init__(self, hostname: str, privileged: bool):
        self.hostname = hostname
        self.privileged = privileged
        self.executor = CommandExecutor(hostname, privileged)
    
    async def execute_batch(
        self,
        command_id: str,
        commands: list
    ) -> dict:
        """Execute a batch of commands sequentially with timing information."""
        batch_start = datetime.now()
        batch_start_perf = perf_counter()
        
        print(f"[BatchExecutor] [{self.hostname}] Starting batch {command_id} with {len(commands)} commands")
        
        results = {
            "type": "batched_command_result",
            "command_id": command_id,
            "hostname": self.hostname,
            "batch_start": batch_start.isoformat(),
            "commands": []
        }
        
        for idx, cmd_spec in enumerate(commands):
            command = cmd_spec.get("command")
            timeout = cmd_spec.get("timeout", 30)
            use_sudo = cmd_spec.get("use_sudo", False)
            
            print(f"[BatchExecutor] [{self.hostname}] [{idx+1}/{len(commands)}] Executing: {command[:50]}... (timeout={timeout}s)")
            
            # Execute command
            cmd_result = await self.executor.execute(
                command_id=f"{command_id}_sub_{idx}",
                command=command,
                timeout=timeout,
                use_sudo=use_sudo
            )
            
            print(f"[BatchExecutor] [{self.hostname}] [{idx+1}/{len(commands)}] Result: success={cmd_result.get('success')}, error={cmd_result.get('error', 'None')}")
            
            # Format for batch result
            result_entry = {
                "command": command,
                "result": {
                    "success": cmd_result.get("success"),
                    "exit_code": cmd_result.get("exit_code"),
                    "stdout": cmd_result.get("stdout"),
                    "stderr": cmd_result.get("stderr"),
                    "error": cmd_result.get("error"),
                    "execution_start": cmd_result.get("execution_start"),
                    "execution_end": cmd_result.get("execution_end"),
                    "execution_duration_seconds": cmd_result.get("execution_duration_seconds")
                }
            }
            
            results["commands"].append(result_entry)
            
            print(f"[BatchExecutor] [{self.hostname}] [{idx+1}/{len(commands)}] Added to batch results - success={result_entry['result']['success']}")
        
        batch_end = datetime.now()
        batch_end_perf = perf_counter()
        
        results["batch_end"] = batch_end.isoformat()
        results["batch_total_duration_seconds"] = round(batch_end_perf - batch_start_perf, 3)
        
        print(f"[BatchExecutor] [{self.hostname}] Batch {command_id} complete in {results['batch_total_duration_seconds']}s")
        print(f"[BatchExecutor] [{self.hostname}] Batch summary: {len(results['commands'])} commands, results written")
        
        return results