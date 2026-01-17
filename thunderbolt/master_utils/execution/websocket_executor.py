import asyncio
import json
import uuid
from typing import List, Dict


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
    ) -> Dict:
        """Execute command via direct WebSocket connections."""
        command_id = str(uuid.uuid4())
        
        # Prepare pending command tracking
        self.pending_commands[command_id] = {
            "responses": {},
            "total_nodes": len(nodes),
            "received": 0,
            "event": asyncio.Event()
        }
        
        # Send command to all specified nodes in parallel
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
        
        # Send all commands in parallel with rate limiting
        send_results = await asyncio.gather(*send_tasks, return_exceptions=True)
        
        # Count failed sends
        failed_sends = sum(1 for r in send_results if isinstance(r, Exception))
        if failed_sends > 0:
            print(f"[Thunderbolt] {failed_sends}/{len(send_tasks)} command sends failed")
        
        # Wait for all responses (with timeout)
        try:
            await asyncio.wait_for(
                self.pending_commands[command_id]["event"].wait(),
                timeout=timeout + 5
            )
        except asyncio.TimeoutError:
            pass
        
        # Collect results
        results = self.pending_commands[command_id]["responses"]
        received = self.pending_commands[command_id]["received"]
        
        # Cleanup
        del self.pending_commands[command_id]
        
        return {
            "command": command,
            "total_nodes": len(nodes),
            "responses_received": received,
            "failed_sends": failed_sends,
            "method": "websocket",
            "results": results
        }
    
    async def execute_batched(self, node_queues: Dict[str, List[dict]]) -> Dict:
        """Execute batched commands via websocket."""
        async def execute_node_queue(hostname: str, commands: List[dict]):
            """Execute a queue of commands for a single node sequentially."""
            node_results = []
            slave_info = self.slaves.get(hostname)
            
            if not slave_info or not slave_info.get("command_ws"):
                return [(cmd["command"], {
                    "success": False,
                    "error": f"Node {hostname} not connected"
                }) for cmd in commands]
            
            for cmd_spec in commands:
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
                    "command": cmd_spec["command"],
                    "timeout": cmd_spec["timeout"],
                    "use_sudo": cmd_spec["use_sudo"]
                })
                
                try:
                    async with self._send_semaphore:
                        await slave_info["command_ws"].send(command_msg)
                    
                    await asyncio.wait_for(
                        self.pending_commands[command_id]["event"].wait(),
                        timeout=cmd_spec["timeout"] + 5
                    )
                    
                    result = self.pending_commands[command_id]["responses"].get(
                        hostname,
                        {"success": False, "error": "No response received"}
                    )
                    
                except asyncio.TimeoutError:
                    result = {"success": False, "error": "Command timeout"}
                except Exception as e:
                    result = {"success": False, "error": f"Send failed: {str(e)}"}
                finally:
                    if command_id in self.pending_commands:
                        del self.pending_commands[command_id]
                
                node_results.append((cmd_spec["command"], result))
            
            return node_results
        
        tasks = [
            execute_node_queue(hostname, commands)
            for hostname, commands in node_queues.items()
        ]
        
        results_by_node = await asyncio.gather(*tasks, return_exceptions=True)
        
        formatted_results = {}
        for hostname, results in zip(node_queues.keys(), results_by_node):
            if isinstance(results, Exception):
                formatted_results[hostname] = {
                    "error": f"Node execution failed: {str(results)}",
                    "commands": []
                }
            else:
                formatted_results[hostname] = {
                    "commands": [
                        {"command": cmd, "result": result}
                        for cmd, result in results
                    ]
                }
        
        return {
            "total_commands": sum(len(cmds) for cmds in node_queues.values()),
            "total_nodes": len(node_queues),
            "method": "websocket",
            "results": formatted_results
        }
    
    async def _send_with_semaphore(self, websocket, message: str, hostname: str):
        """Send message with semaphore-based rate limiting."""
        async with self._send_semaphore:
            try:
                await websocket.send(message)
            except Exception as e:
                print(f"[Thunderbolt] Failed to send to {hostname}: {e}")
                raise