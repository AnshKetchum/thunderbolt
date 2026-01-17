import asyncio
import websockets
import json
from typing import Dict
from datetime import datetime


class WebSocketHandlers:
    """Handles WebSocket connections for command and health channels."""
    
    def __init__(self, slaves: Dict, pending_commands: Dict, slaves_lock: asyncio.Lock):
        self.slaves = slaves
        self.pending_commands = pending_commands
        self._slaves_lock = slaves_lock  # Keep for dict modifications only
    
    async def handle_command_connection(self, websocket):
        """Handle incoming slave command connections."""
        hostname = None
        try:
            registration_msg = await websocket.recv()
            registration = json.loads(registration_msg)
            
            if registration.get("type") != "register":
                await websocket.send(json.dumps({
                    "status": "error",
                    "message": "First message must be registration"
                }))
                return
            
            hostname = registration["hostname"]
            api_key = registration["api_key"]
            
            # LOCK ONLY FOR DICT MODIFICATION
            async with self._slaves_lock:
                if hostname not in self.slaves:
                    self.slaves[hostname] = {
                        "command_ws": websocket,
                        "health_ws": None,
                        "api_key": api_key,
                        "last_seen": datetime.now(),
                        "failed_healthchecks": 0
                    }
                else:
                    self.slaves[hostname]["command_ws"] = websocket
                    self.slaves[hostname]["api_key"] = api_key
            
            await websocket.send(json.dumps({
                "status": "registered",
                "hostname": hostname,
                "connection_type": "command"
            }))
            
            print(f"[Thunderbolt] Slave command channel registered: {hostname}")
            
            # NO LOCK NEEDED - just reading messages
            async for message in websocket:
                data = json.loads(message)
                
                if data.get("type") == "command_result":
                    command_id = data.get("command_id")
                    # NO LOCK - pending_commands dict writes are single-threaded per command_id
                    if command_id in self.pending_commands:
                        pending = self.pending_commands[command_id]
                        pending["responses"][hostname] = data
                        pending["received"] += 1
                        
                        if pending["received"] >= pending["total_nodes"]:
                            pending["event"].set()
        
        except websockets.exceptions.ConnectionClosed:
            print(f"[Thunderbolt] Slave command channel disconnected: {hostname}")
        except asyncio.CancelledError:
            print(f"[Thunderbolt] Command connection cancelled for: {hostname}")
            raise
        except Exception as e:
            print(f"[Thunderbolt] Error handling slave command {hostname}: {e}")
        finally:
            if hostname:
                # LOCK ONLY FOR DICT MODIFICATION
                async with self._slaves_lock:
                    if hostname in self.slaves:
                        self.slaves[hostname]["command_ws"] = None
                        if not self.slaves[hostname].get("health_ws"):
                            del self.slaves[hostname]
                            print(f"[Thunderbolt] Removed slave: {hostname}")
    
    async def handle_health_connection(self, websocket):
        """Handle incoming slave health check connections."""
        hostname = None
        try:
            registration_msg = await websocket.recv()
            registration = json.loads(registration_msg)
            
            if registration.get("type") != "register":
                await websocket.send(json.dumps({
                    "status": "error",
                    "message": "First message must be registration"
                }))
                return
            
            hostname = registration["hostname"]
            
            # LOCK ONLY FOR DICT MODIFICATION
            async with self._slaves_lock:
                if hostname not in self.slaves:
                    self.slaves[hostname] = {
                        "command_ws": None,
                        "health_ws": websocket,
                        "api_key": registration.get("api_key"),
                        "last_seen": datetime.now(),
                        "failed_healthchecks": 0
                    }
                else:
                    self.slaves[hostname]["health_ws"] = websocket
            
            await websocket.send(json.dumps({
                "status": "registered",
                "hostname": hostname,
                "connection_type": "health"
            }))
            
            print(f"[Thunderbolt] Slave health channel registered: {hostname}")
            
            # NO LOCK NEEDED - just reading messages
            async for message in websocket:
                data = json.loads(message)
                
                if data.get("type") == "healthcheck_response":
                    # LOCK ONLY FOR DICT MODIFICATION
                    async with self._slaves_lock:
                        if hostname in self.slaves:
                            self.slaves[hostname]["last_seen"] = datetime.now()
                            self.slaves[hostname]["failed_healthchecks"] = 0
        
        except websockets.exceptions.ConnectionClosed:
            print(f"[Thunderbolt] Slave health channel disconnected: {hostname}")
        except asyncio.CancelledError:
            print(f"[Thunderbolt] Health connection cancelled for: {hostname}")
            raise
        except Exception as e:
            print(f"[Thunderbolt] Error handling slave health {hostname}: {e}")
        finally:
            if hostname:
                # LOCK ONLY FOR DICT MODIFICATION
                async with self._slaves_lock:
                    if hostname in self.slaves:
                        self.slaves[hostname]["health_ws"] = None
                        if not self.slaves[hostname].get("command_ws"):
                            del self.slaves[hostname]
                            print(f"[Thunderbolt] Removed slave: {hostname}")
    
    async def perform_health_checks(self, max_failed_healthchecks: int):
        """Perform health checks on all connected slaves."""
        health_check_msg = json.dumps({"type": "healthcheck"})
        
        # NO LOCK - snapshot is atomic in Python
        slaves_snapshot = list(self.slaves.items())
        
        # NO LOCK - just sending messages
        health_tasks = []
        for hostname, slave_info in slaves_snapshot:
            health_tasks.append(
                self._send_health_check(hostname, slave_info, health_check_msg, max_failed_healthchecks)
            )
        
        results = await asyncio.gather(*health_tasks, return_exceptions=True)
        
        # Collect failures first
        disconnected_slaves = []
        for (hostname, _), result in zip(slaves_snapshot, results):
            if hostname not in self.slaves:
                continue
                
            if isinstance(result, Exception):
                print(f"[Thunderbolt] Health check failed for {hostname}: {result}")
                disconnected_slaves.append(hostname)
            elif result is False:
                print(f"[Thunderbolt] Slave {hostname} failed {max_failed_healthchecks} health checks. Disconnecting.")
                disconnected_slaves.append(hostname)
        
        # Get slave_info references BEFORE locking
        slaves_to_close = []
        for hostname in disconnected_slaves:
            if hostname in self.slaves:
                slaves_to_close.append((hostname, self.slaves[hostname]))
        
        # Close connections WITHOUT holding lock
        for hostname, slave_info in slaves_to_close:
            close_tasks = []
            if slave_info.get("command_ws"):
                close_tasks.append(slave_info["command_ws"].close())
            if slave_info.get("health_ws"):
                close_tasks.append(slave_info["health_ws"].close())
            
            if close_tasks:
                await asyncio.gather(*close_tasks, return_exceptions=True)
        
        # SINGLE LOCK for all deletions
        if disconnected_slaves:
            async with self._slaves_lock:
                for hostname in disconnected_slaves:
                    if hostname in self.slaves:
                        del self.slaves[hostname]
    
    async def _send_health_check(self, hostname: str, slave_info: dict, msg: str, max_failed_healthchecks: int) -> bool:
        """Send health check to a single slave. Returns False if max failures reached."""
        try:
            health_ws = slave_info.get("health_ws")
            if not health_ws:
                return False
            
            # NO LOCK - just sending
            await health_ws.send(msg)
            
            # LOCK ONLY FOR INCREMENT
            async with self._slaves_lock:
                if hostname in self.slaves:
                    self.slaves[hostname]["failed_healthchecks"] += 1
                    
                    if self.slaves[hostname]["failed_healthchecks"] >= max_failed_healthchecks:
                        return False
            
            return True
            
        except Exception as e:
            raise e