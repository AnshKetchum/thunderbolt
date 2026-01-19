import asyncio
from datetime import datetime
from time import perf_counter


class CommandExecutor:
    """Executes shell commands with timeout support."""
    
    def __init__(self, hostname: str):
        self.hostname = hostname
    
    async def _kill_process(self, process, grace_timeout: float = 2.0):
        """
        Forcefully terminate a process with escalating signals.
        
        Args:
            process: The subprocess to terminate
            grace_timeout: How long to wait for graceful termination before force killing
        """
        if not process or process.returncode is not None:
            print(f"[CommandExecutor] [{self.hostname}] Process already terminated (returncode={process.returncode if process else None})")
            return  # Already terminated
        
        print(f"[CommandExecutor] [{self.hostname}] Attempting to kill process (PID: {process.pid})")
        
        try:
            # Try graceful shutdown first (SIGTERM)
            print(f"[CommandExecutor] [{self.hostname}] Sending SIGTERM to process {process.pid}")
            process.terminate()
            
            try:
                await asyncio.wait_for(process.wait(), timeout=grace_timeout)
                print(f"[CommandExecutor] [{self.hostname}] ✓ Process {process.pid} terminated gracefully")
                return  # Successfully terminated
            except asyncio.TimeoutError:
                print(f"[CommandExecutor] [{self.hostname}] Process {process.pid} did not terminate gracefully after {grace_timeout}s, escalating to SIGKILL")
                pass  # Need to escalate to SIGKILL
            
            # Force kill (SIGKILL)
            print(f"[CommandExecutor] [{self.hostname}] Sending SIGKILL to process {process.pid}")
            process.kill()
            await process.wait()
            print(f"[CommandExecutor] [{self.hostname}] ✓ Process {process.pid} force killed")
            
        except ProcessLookupError:
            # Process already died between our check and termination
            print(f"[CommandExecutor] [{self.hostname}] Process already died (ProcessLookupError)")
            pass
        except Exception as e:
            # Log but don't fail - best effort cleanup
            print(f"[CommandExecutor] [{self.hostname}] ✗ Warning: Failed to kill process: {type(e).__name__}: {e}")
    
    async def execute(
        self,
        command_id: str,
        command: str,
        timeout: int = 30,
        use_sudo: bool = False
    ) -> dict:
        """
        Execute a command and return the result with timing information.
        
        Args:
            command_id: Unique identifier for this command execution
            command: Shell command to execute
            timeout: Maximum execution time in seconds
            use_sudo: Whether to prepend 'sudo' to the command
            
        Returns:
            dict: Execution result containing success status, output, timing info
        """
        start_time = datetime.now()
        start_perf = perf_counter()
        
        # Truncate command for logging if too long
        cmd_display = command if len(command) <= 60 else f"{command[:57]}..."
        
        print(f"[CommandExecutor] [{self.hostname}] [{command_id}] Starting execution")
        print(f"[CommandExecutor] [{self.hostname}] [{command_id}] Command: {cmd_display}")
        print(f"[CommandExecutor] [{self.hostname}] [{command_id}] Timeout: {timeout}s")
        print(f"[CommandExecutor] [{self.hostname}] [{command_id}] Use sudo: {use_sudo}")
        
        result = {
            "command_id": command_id,
            "command": command,
            "execution_start": start_time.isoformat()
        }
        
        process = None
        
        try:
            # Prepare command
            full_command = f"sudo {command}" if use_sudo else command
            
            # Create subprocess
            print(f"[CommandExecutor] [{self.hostname}] [{command_id}] Creating subprocess...")
            process = await asyncio.create_subprocess_shell(
                full_command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            print(f"[CommandExecutor] [{self.hostname}] [{command_id}] Process created (PID: {process.pid})")
            
            try:
                # Execute with timeout enforcement
                print(f"[CommandExecutor] [{self.hostname}] [{command_id}] Waiting for process to complete (max {timeout}s)...")
                stdout, stderr = await asyncio.wait_for(
                    process.communicate(),
                    timeout=timeout
                )
                
                end_time = datetime.now()
                end_perf = perf_counter()
                duration = round(end_perf - start_perf, 3)
                
                print(f"[CommandExecutor] [{self.hostname}] [{command_id}] ✓ Process completed in {duration}s")
                print(f"[CommandExecutor] [{self.hostname}] [{command_id}] Exit code: {process.returncode}")
                print(f"[CommandExecutor] [{self.hostname}] [{command_id}] Stdout length: {len(stdout)} bytes")
                print(f"[CommandExecutor] [{self.hostname}] [{command_id}] Stderr length: {len(stderr)} bytes")
                
                result.update({
                    "success": True,
                    "exit_code": process.returncode,
                    "stdout": stdout.decode('utf-8', errors='replace'),
                    "stderr": stderr.decode('utf-8', errors='replace'),
                    "execution_end": end_time.isoformat(),
                    "execution_duration_seconds": duration
                })
                
            except asyncio.TimeoutError:
                # Command exceeded timeout - kill it
                end_time = datetime.now()
                end_perf = perf_counter()
                duration = round(end_perf - start_perf, 3)
                
                print(f"[CommandExecutor] [{self.hostname}] [{command_id}] ✗ TIMEOUT after {duration}s (limit: {timeout}s)")
                
                # Kill the process
                await self._kill_process(process)
                
                result.update({
                    "success": False,
                    "error": f"Command timed out after {timeout}s",
                    "execution_end": end_time.isoformat(),
                    "execution_duration_seconds": duration
                })
                
                print(f"[CommandExecutor] [{self.hostname}] [{command_id}] Timeout result prepared: success={result['success']}, error='{result['error']}'")
        
        except asyncio.CancelledError:
            # Task was cancelled - clean up and re-raise
            end_time = datetime.now()
            end_perf = perf_counter()
            duration = round(end_perf - start_perf, 3)
            
            print(f"[CommandExecutor] [{self.hostname}] [{command_id}] ✗ CANCELLED after {duration}s")
            
            if process:
                await self._kill_process(process)
            
            result.update({
                "success": False,
                "error": "Command execution was cancelled",
                "execution_end": end_time.isoformat(),
                "execution_duration_seconds": duration
            })
            
            print(f"[CommandExecutor] [{self.hostname}] [{command_id}] Re-raising CancelledError")
            raise  # Re-raise CancelledError
        
        except Exception as e:
            # Unexpected error during execution
            end_time = datetime.now()
            end_perf = perf_counter()
            duration = round(end_perf - start_perf, 3)
            
            print(f"[CommandExecutor] [{self.hostname}] [{command_id}] ✗ EXCEPTION after {duration}s: {type(e).__name__}: {e}")
            
            # Try to clean up process if it exists
            if process:
                await self._kill_process(process)
            
            result.update({
                "success": False,
                "error": f"Execution error: {str(e)}",
                "execution_end": end_time.isoformat(),
                "execution_duration_seconds": duration
            })
        
        # Final result summary
        success_marker = "✓" if result.get("success") else "✗"
        print(f"[CommandExecutor] [{self.hostname}] [{command_id}] {success_marker} Final result:")
        print(f"[CommandExecutor] [{self.hostname}] [{command_id}]   - Success: {result.get('success')}")
        print(f"[CommandExecutor] [{self.hostname}] [{command_id}]   - Duration: {result.get('execution_duration_seconds')}s")
        if not result.get("success"):
            print(f"[CommandExecutor] [{self.hostname}] [{command_id}]   - Error: {result.get('error')}")
        else:
            print(f"[CommandExecutor] [{self.hostname}] [{command_id}]   - Exit code: {result.get('exit_code')}")
        
        return result