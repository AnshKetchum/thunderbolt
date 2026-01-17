from datetime import datetime
from time import perf_counter
from .command_executor import CommandExecutor


class BatchExecutor:
    """Executes batches of commands sequentially."""
    
    def __init__(self, hostname: str):
        self.hostname = hostname
        self.executor = CommandExecutor(hostname)
    
    async def execute_batch(
        self,
        command_id: str,
        commands: list
    ) -> dict:
        """Execute a batch of commands sequentially with timing information."""
        batch_start = datetime.now()
        batch_start_perf = perf_counter()
        
        results = {
            "type": "batched_command_result",
            "command_id": command_id,
            "hostname": self.hostname,
            "batch_start": batch_start.isoformat(),
            "commands": []
        }
        
        for cmd_spec in commands:
            command = cmd_spec.get("command")
            timeout = cmd_spec.get("timeout", 30)
            use_sudo = cmd_spec.get("use_sudo", False)
            
            # Execute command
            cmd_result = await self.executor.execute(
                command_id=f"{command_id}_sub",
                command=command,
                timeout=timeout,
                use_sudo=use_sudo
            )
            
            # Format for batch result
            results["commands"].append({
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
            })
        
        batch_end = datetime.now()
        batch_end_perf = perf_counter()
        
        results["batch_end"] = batch_end.isoformat()
        results["batch_total_duration_seconds"] = round(batch_end_perf - batch_start_perf, 3)
        
        return results