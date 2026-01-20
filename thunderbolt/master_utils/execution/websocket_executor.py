import asyncio
import json
import uuid
from typing import List, Dict
from .response_models import CommandResult

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

    async def execute_batched(self, command_specs: List[dict]) -> List[CommandResult]:
        """Execute batched commands via websocket using batched_command message type."""
        print("Sending command via batched API")
        
        # Group commands by node
        node_commands = {}
        spec_lookup = {}  # Map (node, command_index) to original spec
        
        for spec in command_specs:
            hostname = spec["node"]
            if hostname not in node_commands:
                node_commands[hostname] = []
            
            cmd_entry = {
                "command": spec["command"],
                "timeout": spec["timeout"],
                "use_sudo": spec["use_sudo"],
                "command_uuid": spec["command_uuid"]
            }
            
            node_commands[hostname].append(cmd_entry)
            spec_lookup[(hostname, len(node_commands[hostname]) - 1)] = spec
        
        # Generate a single batch command ID
        batch_id = str(uuid.uuid4())
        
        # Track all nodes involved
        all_nodes = list(node_commands.keys())
        
        self.pending_commands[batch_id] = {
            "responses": {},
            "total_nodes": len(all_nodes),
            "received": 0,
            "event": asyncio.Event()
        }
        
        # Send batched command to each node
        send_tasks = []
        for hostname, commands in node_commands.items():
            slave_info = self.slaves.get(hostname)
            if slave_info and slave_info.get("command_ws"):
                batch_msg = json.dumps({
                    "type": "batched_command",
                    "command_id": batch_id,
                    "commands": commands
                })
                
                send_tasks.append(
                    self._send_with_semaphore(
                        slave_info["command_ws"],
                        batch_msg,
                        hostname
                    )
                )
        
        send_results = await asyncio.gather(*send_tasks, return_exceptions=True)
        
        failed_sends = sum(1 for r in send_results if isinstance(r, Exception))
        if failed_sends > 0:
            print(f"[Thunderbolt] {failed_sends}/{len(send_tasks)} batched command sends failed")
        
        # Wait for all node responses
        max_timeout = max((spec["timeout"] for spec in command_specs), default=30)
        try:
            await asyncio.wait_for(
                self.pending_commands[batch_id]["event"].wait(),
                timeout=max_timeout + 10
            )
        except asyncio.TimeoutError:
            print(f"[Thunderbolt] Batch {batch_id} timed out waiting for responses")
        
        raw_responses = self.pending_commands[batch_id]["responses"]
        
        # Cleanup
        del self.pending_commands[batch_id]
        
        # Build results in original input order
        results: List[CommandResult] = []
        for spec in command_specs:
            hostname = spec["node"]
            command_uuid = spec["command_uuid"]
            command = spec["command"]
            
            # Find this command's result in the node's batch response
            node_response = raw_responses.get(hostname, {})
            command_results = node_response.get("results", [])
            
            # Match by command_uuid
            cmd_result = None
            for cr in command_results:
                if cr.get("command_uuid") == command_uuid:
                    cmd_result = cr
                    break
            
            if cmd_result:
                results.append(CommandResult(
                    command_uuid=command_uuid,
                    node=hostname,
                    command=command,
                    stdout=cmd_result.get("stdout"),
                    stderr=cmd_result.get("stderr"),
                    exit_code=cmd_result.get("exit_code"),
                    error=cmd_result.get("error"),
                    timed_out=cmd_result.get("timed_out", False)
                ))
            else:
                results.append(CommandResult(
                    command_uuid=command_uuid,
                    node=hostname,
                    command=command,
                    error="No response received for this command",
                    timed_out=True
                ))
        
        return results
    
    async def _send_with_semaphore(self, websocket, message: str, hostname: str):
        """Send message with semaphore-based rate limiting."""
        async with self._send_semaphore:
            try:
                await websocket.send(message)
            except Exception as e:
                print(f"[Thunderbolt] Failed to send to {hostname}: {e}")
                raise