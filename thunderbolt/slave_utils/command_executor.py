import asyncio
from datetime import datetime
from time import perf_counter


class CommandExecutor:
    """Executes shell commands with timeout support."""
    
    def __init__(self, hostname: str):
        self.hostname = hostname
    
    async def execute(
        self,
        command_id: str,
        command: str,
        timeout: int = 30,
        use_sudo: bool = False
    ) -> dict:
        """Execute a command and return the result with timing information."""
        start_time = datetime.now()
        start_perf = perf_counter()
        
        result = {
            "command_id": command_id,
            "command": command,
            "execution_start": start_time.isoformat()
        }
        
        try:
            # Prepare command
            full_command = f"sudo {command}" if use_sudo else command
            
            # Execute with timeout
            process = await asyncio.create_subprocess_shell(
                full_command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            try:
                stdout, stderr = await asyncio.wait_for(
                    process.communicate(),
                    timeout=timeout
                )
                
                end_time = datetime.now()
                end_perf = perf_counter()
                
                result.update({
                    "success": True,
                    "exit_code": process.returncode,
                    "stdout": stdout.decode('utf-8', errors='replace'),
                    "stderr": stderr.decode('utf-8', errors='replace'),
                    "execution_end": end_time.isoformat(),
                    "execution_duration_seconds": round(end_perf - start_perf, 3)
                })
                
            except asyncio.TimeoutError:
                # Kill the process
                try:
                    process.kill()
                    await process.wait()
                except:
                    pass
                
                end_time = datetime.now()
                end_perf = perf_counter()
                
                result.update({
                    "success": False,
                    "error": f"Command timed out after {timeout}s",
                    "execution_end": end_time.isoformat(),
                    "execution_duration_seconds": round(end_perf - start_perf, 3)
                })
        
        except Exception as e:
            end_time = datetime.now()
            end_perf = perf_counter()
            
            result.update({
                "success": False,
                "error": str(e),
                "execution_end": end_time.isoformat(),
                "execution_duration_seconds": round(end_perf - start_perf, 3)
            })
        
        return result