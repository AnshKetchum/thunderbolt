import asyncio
import json
import websockets
from .base import BaseChannel
from .response_models import CommandResult

class CommandChannel(BaseChannel):
    """Handles command execution channel."""
    
    def __init__(self, *args, command_executor=None, batch_executor=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.command_executor = command_executor
        self.batch_executor = batch_executor
    
    @property
    def channel_name(self) -> str:
        return "Command"
    
    async def _handle_messages(self, websocket):
        """Handle incoming command messages."""
        try:
            async for message in websocket:
                data = json.loads(message)
                msg_type = data.get("type")
                
                if msg_type == "command":
                    # Execute single command asynchronously
                    asyncio.create_task(
                        self._execute_and_respond(
                            websocket,
                            data.get("command_id"),
                            data.get("command"),
                            data.get("timeout", 30),
                            data.get("use_sudo", False)
                        )
                    )
                elif msg_type == "batched_command":
                    # Execute batch of commands asynchronously
                    asyncio.create_task(
                        self._execute_batch_and_respond(
                            websocket,
                            data.get("command_id"),
                            data.get("commands", [])
                        )
                    )
        except websockets.exceptions.ConnectionClosed:
            pass
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[Command] Error handling messages: {e}")
    
    async def _execute_and_respond(
        self,
        websocket,
        command_id: str,
        command: str,
        timeout: int,
        use_sudo: bool
    ):
        """Execute command and send result back to master."""
        if not self.command_executor:
            print(f"[Command] No executor available for command {command_id}")
            return
        
        # Execute command
        result = await self.command_executor.execute(
            command_id=command_id,
            command=command,
            timeout=timeout,
            use_sudo=use_sudo
        )
        print(f"[Command Channel]: Got result {result}")
        
        # Add metadata
        payload = result.model_dump()
        payload.update({
            "type": "command_result",
            "hostname": self.hostname,
            "command_id": command_id
        })
        
        # Send result back to master
        try:
            await websocket.send(json.dumps(payload))
        except Exception as e:
            print(f"[Command] Failed to send result: {e}")
    
    async def _execute_batch_and_respond(
        self,
        websocket,
        command_id: str,
        commands: list
    ):
        """Execute batch of commands and send results back to master."""
        if not self.batch_executor:
            print(f"[Command] No batch executor available for batch {command_id}")
            return
        
        print(f"[Command Channel]: Executing batch {command_id} with {len(commands)} commands")
        
        # Execute batch
        batch_result = await self.batch_executor.execute_batch(
            command_id=command_id,
            commands=commands
        )
        
        print(f"[Command Channel]: Batch {command_id} complete with {len(batch_result)} results")
        
        # Build response payload
        payload = {
            "type": "batch_result",
            "hostname": self.hostname,
            "command_id": command_id,
            "results": [result.model_dump() for result in batch_result]
        }
        
        # Send results back to master
        try:
            await websocket.send(json.dumps(payload))
        except Exception as e:
            print(f"[Command] Failed to send batch result: {e}")