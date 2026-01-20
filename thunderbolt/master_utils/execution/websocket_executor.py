import asyncio
import json
import uuid
from typing import List, Dict
from .response_models import BatchedResponse, CommandResult

class WebSocketExecutor:
    """Executes commands via WebSocket connections."""
    
    def __init__(self, slaves: Dict, pending_commands: Dict, send_semaphore: asyncio.Semaphore):
        self.slaves = slaves
        self.pending_commands = pending_commands
        self._send_semaphore = send_semaphore
    
    async def execute_command(
        self,
        command: str,
        nodes: List[str],
        timeout: int,
        use_sudo: bool
    ) -> List[CommandResult]:
        """Execute command via direct WebSocket connections."""
        command_id = str(uuid.uuid4())
        print("Sending command via execute_command")

        # Prepare pending command tracking
        self.pending_commands[command_id] = {
            "responses": {},
            "total_nodes": len(nodes),
            "received": 0,
            "event": asyncio.Event()
        }

        command_msg = json.dumps({
            "type": "command",
            "command_id": command_id,
            "command": command,
            "timeout": timeout,
            "use_sudo": use_sudo
        })

        # Batch send with rate limiting
        send_tasks = []
        for hostname in nodes:
            slave_info = self.slaves.get(hostname)
            if slave_info and slave_info.get("command_ws"):
                send_tasks.append(
                    self._send_with_semaphore(
                        slave_info["command_ws"],
                        command_msg,
                        hostname
                    )
                )

        send_results = await asyncio.gather(*send_tasks, return_exceptions=True)

        failed_sends = sum(1 for r in send_results if isinstance(r, Exception))
        if failed_sends > 0:
            print(f"[Thunderbolt] {failed_sends}/{len(send_tasks)} command sends failed")

        # Wait for responses
        try:
            await asyncio.wait_for(
                self.pending_commands[command_id]["event"].wait(),
                timeout=timeout + 5
            )
        except asyncio.TimeoutError:
            pass

        raw_results = self.pending_commands[command_id]["responses"]

        # Cleanup pending command
        del self.pending_commands[command_id]

        # Build CommandResult list (preserve node order)
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

        return results

    async def execute_batched(self, command_specs: List[dict]) -> BatchedResponse:
        """Execute batched commands via websocket, maintaining input order."""
        print("Sending command via batched api")
        
        async def execute_single_command(cmd_spec: dict) -> CommandResult:
            """Execute a single command and return a CommandResult."""
            command_uuid = cmd_spec["command_uuid"]
            hostname = cmd_spec["node"]
            command = cmd_spec["command"]
            timeout = cmd_spec["timeout"]
            use_sudo = cmd_spec["use_sudo"]
            
            slave_info = self.slaves.get(hostname)
            
            if not slave_info or not slave_info.get("command_ws"):
                return CommandResult(
                    command_uuid=command_uuid,
                    node=hostname,
                    command=command,
                    error=f"Node {hostname} not connected",
                    timed_out=False
                )
            
            command_id = str(uuid.uuid4())
            
            self.pending_commands[command_id] = {
                "responses": {},
                "total_nodes": 1,
                "received": 0,
                "event": asyncio.Event()
            }
            
            command_msg = json.dumps({
                "type": "command",
                "command_id": command_id,
                "command": command,
                "timeout": timeout,
                "use_sudo": use_sudo
            })
            
            try:
                async with self._send_semaphore:
                    await slave_info["command_ws"].send(command_msg)
                
                await asyncio.wait_for(
                    self.pending_commands[command_id]["event"].wait(),
                    timeout=max(timeout + 5, 60)
                )
                
                result = self.pending_commands[command_id]["responses"].get(hostname, {})
                print(f"Command {command}: result {result}") 
                
                return CommandResult(
                    command_uuid=command_uuid,
                    node=hostname,
                    command=command,
                    stdout=result.get("stdout"),
                    stderr=result.get("stderr"),
                    exit_code=result.get("exit_code"),
                    error=result.get("error"),
                    timed_out=result.get("timed_out")
                )
                
            except asyncio.TimeoutError as e:
                import traceback 
                traceback.print_exc()
                return CommandResult(
                    command_uuid=command_uuid,
                    node=hostname,
                    command=command,
                    error="Command timeout",
                    timed_out=True
                )
            except Exception as e:
                return CommandResult(
                    command_uuid=command_uuid,
                    node=hostname,
                    command=command,
                    error=f"Send failed: {str(e)}",
                    timed_out=False
                )
            finally:
                if command_id in self.pending_commands:
                    del self.pending_commands[command_id]
        
        # Execute all commands in parallel
        tasks = [execute_single_command(cmd_spec) for cmd_spec in command_specs]
        results = await asyncio.gather(*tasks)
        
        # Get unique nodes count
        nodes_set = {cmd["node"] for cmd in command_specs}
        
        return BatchedResponse(
            total_commands=len(command_specs),
            total_nodes=len(nodes_set),
            method="websocket",
            results=results
        )
    
    async def _send_with_semaphore(self, websocket, message: str, hostname: str):
        """Send message with semaphore-based rate limiting."""
        async with self._send_semaphore:
            try:
                await websocket.send(message)
            except Exception as e:
                print(f"[Thunderbolt] Failed to send to {hostname}: {e}")
                raise