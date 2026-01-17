import asyncio
import websockets
import json
from typing import Optional


class BaseChannel:
    """Base class for WebSocket channel connections."""
    
    def __init__(
        self,
        master_host: str,
        port: int,
        hostname: str,
        api_key: str,
        reconnect_interval: int = 5,
        max_reconnect_attempts: int = -1
    ):
        self.master_host = master_host
        self.port = port
        self.hostname = hostname
        self.api_key = api_key
        self.reconnect_interval = reconnect_interval
        self.max_reconnect_attempts = max_reconnect_attempts
        
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self._connected = asyncio.Event()
        self._shutdown_event = asyncio.Event()
    
    @property
    def is_connected(self) -> bool:
        return self._connected.is_set()
    
    async def connect(self):
        """Connect to master with reconnection logic."""
        attempt = 0
        
        while not self._shutdown_event.is_set():
            if self.max_reconnect_attempts > 0 and attempt >= self.max_reconnect_attempts:
                print(f"[{self.channel_name}] Max reconnection attempts reached")
                break
            
            try:
                attempt += 1
                uri = f"ws://{self.master_host}:{self.port}"
                print(f"[{self.channel_name}] Connecting to {uri} (attempt {attempt})")
                
                async with websockets.connect(uri, ping_interval=30, ping_timeout=10) as websocket:
                    self.ws = websocket
                    
                    # Register with master
                    await self._register(websocket)
                    
                    # Wait for acknowledgment
                    ack = await self._wait_for_ack(websocket)
                    
                    if ack.get("status") == "registered":
                        print(f"[{self.channel_name}] Registered successfully")
                        self._connected.set()
                        attempt = 0
                        
                        # Handle messages
                        await self._handle_messages(websocket)
                    else:
                        print(f"[{self.channel_name}] Registration failed: {ack.get('message')}")
            
            except websockets.exceptions.ConnectionClosed:
                print(f"[{self.channel_name}] Connection closed")
                self._connected.clear()
            except asyncio.CancelledError:
                print(f"[{self.channel_name}] Connection cancelled")
                self._connected.clear()
                raise
            except Exception as e:
                print(f"[{self.channel_name}] Error: {e}")
                self._connected.clear()
            finally:
                self.ws = None
            
            # Wait before reconnecting
            if not self._shutdown_event.is_set():
                print(f"[{self.channel_name}] Reconnecting in {self.reconnect_interval}s...")
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(),
                        timeout=self.reconnect_interval
                    )
                except asyncio.TimeoutError:
                    pass
    
    async def _register(self, websocket):
        """Send registration message to master."""
        registration = {
            "type": "register",
            "hostname": self.hostname,
            "api_key": self.api_key
        }
        await websocket.send(json.dumps(registration))
    
    async def _wait_for_ack(self, websocket):
        """Wait for acknowledgment from master."""
        ack_msg = await websocket.recv()
        return json.loads(ack_msg)
    
    async def _handle_messages(self, websocket):
        """Handle incoming messages. Override in subclass."""
        raise NotImplementedError
    
    async def shutdown(self):
        """Shutdown the channel."""
        self._shutdown_event.set()
        if self.ws:
            await self.ws.close()
    
    @property
    def channel_name(self) -> str:
        """Channel name for logging. Override in subclass."""
        return "Base"