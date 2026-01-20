import asyncio
from datetime import datetime
from time import perf_counter
from .response_models import CommandResult

class CommandExecutor:
    """Executes shell commands with timeout support."""
    
    def __init__(self, hostname: str, privileged: bool):
        self.hostname = hostname
        self.privileged = privileged
    
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
    ) -> CommandResult:
        """
        Execute a command and return the result with timing information.
        
        Args:
            command_id: Unique identifier for this command execution
            command: Shell command to execute
            timeout: Maximum execution time in seconds
            use_sudo: Whether to prepend 'sudo' to the command
            
        Returns:
            CommandResult: Execution result containing success status, output, timing info
        """
        start_time = datetime.now()
        start_perf = perf_counter()
        
        # Truncate command for logging if too long
        cmd_display = command if len(command) <= 60 else f"{command[:57]}..."
        
        print(f"[CommandExecutor] [{self.hostname}] [{command_id}] Starting execution")
        print(f"[CommandExecutor] [{self.hostname}] [{command_id}] Command: {cmd_display}")
        print(f"[CommandExecutor] [{self.hostname}] [{command_id}] Timeout: {timeout}s")
        print(f"[CommandExecutor] [{self.hostname}] [{command_id}] Use sudo: {use_sudo}")
        print(f"[CommandExecutor] [{self.hostname}] [{command_id}] Privileged: {self.privileged}")
        
        # Check privilege requirements
        if use_sudo and not self.privileged:
            end_perf = perf_counter()
            duration = round(end_perf - start_perf, 3)
            
            error_msg = "Cannot execute sudo command: executor not privileged"
            print(f"[CommandExecutor] [{self.hostname}] [{command_id}] ✗ PERMISSION DENIED: {error_msg}")
            
            result = CommandResult(
                command_uuid=command_id,
                node=self.hostname,
                command=command,
                error=error_msg,
                timed_out=False
            )
            
            print(f"[CommandExecutor] [{self.hostname}] [{command_id}] ✗ Final result:")
            print(f"[CommandExecutor] [{self.hostname}] [{command_id}]   - Duration: {duration}s")
            print(f"[CommandExecutor] [{self.hostname}] [{command_id}]   - Error: {result.error}")
            
            return result
        
        process = None
        
        try:
            # Prepare command
            full_command = f"sudo {command}" if use_sudo else command
            
            # Create subprocess
            print(f"[CommandExecutor] [{self.hostname}] [{command_id}] [{full_command}] Creating subprocess...")
            process = await asyncio.create_subprocess_shell(
                full_command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            print(f"[CommandExecutor] [{self.hostname}] [{command_id}] Process created (PID: {process.pid})")
            
            try:
                # Execute with timeout enforcement
                print(f"[CommandExecutor] [{self.hostname}] [{command_id}] [{full_command}] Waiting for process to complete (max {timeout}s)...")
                stdout, stderr = await asyncio.wait_for(
                    process.communicate(),
                    timeout=timeout
                )
                
                end_perf = perf_counter()
                duration = round(end_perf - start_perf, 3)
                
                print(f"[CommandExecutor] [{self.hostname}] [{command_id}] [{full_command}] ✓ Process completed in {duration}s")
                print(f"[CommandExecutor] [{self.hostname}] [{command_id}] Exit code: {process.returncode}")
                print(f"[CommandExecutor] [{self.hostname}] [{command_id}] Stdout length: {len(stdout)} bytes")
                print(f"[CommandExecutor] [{self.hostname}] [{command_id}] Stderr length: {len(stderr)} bytes")
                
                # Print stderr content if non-empty
                if len(stderr) > 0:
                    stderr_text = stderr.decode('utf-8', errors='replace')
                    print(f"[CommandExecutor] [{self.hostname}] [{command_id}] [{full_command}] Stderr content:")
                    print(f"[CommandExecutor] [{self.hostname}] [{command_id}] {stderr_text}")
                
                result = CommandResult(
                    command_uuid=command_id,
                    node=self.hostname,
                    command=command,
                    stdout=stdout.decode('utf-8', errors='replace'),
                    stderr=stderr.decode('utf-8', errors='replace'),
                    exit_code=process.returncode,
                    timed_out=False
                )
                
            except asyncio.TimeoutError:
                # Command exceeded timeout - kill it
                end_perf = perf_counter()
                duration = round(end_perf - start_perf, 3)
                
                print(f"[CommandExecutor] [{self.hostname}] [{command_id}] [{full_command}] ✗ TIMEOUT after {duration}s (limit: {timeout}s)")
                
                # Kill the process
                await self._kill_process(process)
                
                result = CommandResult(
                    command_uuid=command_id,
                    node=self.hostname,
                    command=command,
                    error=f"Command timed out after {timeout}s",
                    timed_out=True
                )
                
                print(f"[CommandExecutor] [{self.hostname}] [{command_id}] [{full_command}] Timeout result prepared: timed_out={result.timed_out}, error='{result.error}'")
                
        except asyncio.CancelledError:
            # Task was cancelled - clean up and re-raise
            end_perf = perf_counter()
            duration = round(end_perf - start_perf, 3)
            
            print(f"[CommandExecutor] [{self.hostname}] [{command_id}] [{full_command}] ✗ CANCELLED after {duration}s")
            
            if process:
                await self._kill_process(process)
            
            result = CommandResult(
                command_uuid=command_id,
                node=self.hostname,
                command=command,
                error="Command execution was cancelled",
                timed_out=False
            )
            
            print(f"[CommandExecutor] [{self.hostname}] [{command_id}] [{full_command}] Re-raising CancelledError")
            raise  # Re-raise CancelledError
            
        except Exception as e:
            # Unexpected error during execution
            end_perf = perf_counter()
            duration = round(end_perf - start_perf, 3)
            
            print(f"[CommandExecutor] [{self.hostname}] [{command_id}] [{full_command}] ✗ EXCEPTION after {duration}s: {type(e).__name__}: {e}")
            
            # Try to clean up process if it exists
            if process:
                await self._kill_process(process)
            
            result = CommandResult(
                command_uuid=command_id,
                node=self.hostname,
                command=command,
                error=f"Execution error: {str(e)}",
                timed_out=False
            )
        
        # Final result summary
        success_marker = "✓" if result.error is None else "✗"
        print(f"[CommandExecutor] [{self.hostname}] [{command_id}] {success_marker} Final result:")
        if result.error:
            print(f"[CommandExecutor] [{self.hostname}] [{command_id}] [{full_command}]   - Error: {result.error}")
            print(f"[CommandExecutor] [{self.hostname}] [{command_id}] [{full_command}] - Timed out: {result.timed_out}")
        else:
            print(f"[CommandExecutor] [{self.hostname}] [{command_id}] [{full_command}]   - Exit code: {result.exit_code}")
        
        print(f"[Command Executor] Returning result {result}")
        return result