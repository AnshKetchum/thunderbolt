from datetime import datetime
from time import perf_counter
from .command_executor import CommandExecutor
from .response_models import CommandResult


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
    ) -> list[CommandResult]:
        """Execute a batch of commands sequentially and return batch result."""
        batch_start = datetime.now()
        batch_start_perf = perf_counter()
        print(f"[BatchExecutor] [{self.hostname}] Starting batch {command_id} with {len(commands)} commands")
        
        results: list[CommandResult] = []
        
        for idx, cmd_spec in enumerate(commands):
            command = cmd_spec.get("command")
            timeout = cmd_spec.get("timeout", 30)
            use_sudo = cmd_spec.get("use_sudo", False)
            command_uuid = cmd_spec.get("command_uuid")  # Get UUID from spec
            
            print(
                f"[BatchExecutor] [{self.hostname}] "
                f"[{idx+1}/{len(commands)}] Executing: {command[:50]}... (timeout={timeout}s, uuid={command_uuid})"
            )
            
            cmd_result: CommandResult = await self.executor.execute(
                command_id=command_uuid,  # Use the UUID from master
                command=command,
                timeout=timeout,
                use_sudo=use_sudo,
            )
            
            print(
                f"[BatchExecutor] [{self.hostname}] "
                f"[{idx+1}/{len(commands)}] "
                f"Result: success={cmd_result.error is None}, error={cmd_result.error or 'None'}"
            )
            
            results.append(cmd_result)
            
            print(
                f"[BatchExecutor] [{self.hostname}] "
                f"[{idx+1}/{len(commands)}] "
                f"Added to batch results - success={cmd_result.error is None}"
            )
        
        batch_end_perf = perf_counter()
        total_duration = round(batch_end_perf - batch_start_perf, 3)
        
        print(f"[BatchExecutor] [{self.hostname}] Batch {command_id} complete in {total_duration}s")
        print(f"[BatchExecutor] [{self.hostname}] Batch summary: {len(results)} commands, results written")
        print(f"[BatchExecutor] [{self.hostname}] Results: {results}")
        
        return results