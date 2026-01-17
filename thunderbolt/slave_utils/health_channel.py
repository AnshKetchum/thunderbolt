import asyncio
import json
import websockets
from .base import BaseChannel


class HealthChannel(BaseChannel):
    """Handles health check channel."""
    
    @property
    def channel_name(self) -> str:
        return "Health"
    
    async def connect(self):
        """Override to use different ping settings for health checks."""
        attempt = 0
        
        while not self._shutdown_event.is_set():
            if self.max_reconnect_attempts > 0 and attempt >= self.max_reconnect_attempts:
                print(f"[{self.channel_name}] Max reconnection attempts reached")
                break
            
            try:
                attempt += 1
                uri = f"ws://{self.master_host}:{self.port}"
                print(f"[{self.channel_name}] Connecting to {uri} (attempt {attempt})")
                
                # Health checks use shorter ping intervals
                async with websockets.connect(uri, ping_interval=20, ping_timeout=10) as websocket:
                    self.ws = websocket
                    
                    await self._register(websocket)
                    ack = await self._wait_for_ack(websocket)
                    
                    if ack.get("status") == "registered":
                        print(f"[{self.channel_name}] Registered successfully")
                        self._connected.set()
                        attempt = 0
                        
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
            
            if not self._shutdown_event.is_set():
                print(f"[{self.channel_name}] Reconnecting in {self.reconnect_interval}s...")
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(),
                        timeout=self.reconnect_interval
                    )
                except asyncio.TimeoutError:
                    pass
    
    async def _handle_messages(self, websocket):
        """Handle incoming health check messages."""
        try:
            async for message in websocket:
                data = json.loads(message)
                
                if data.get("type") == "healthcheck":
                    # Respond immediately
                    response = {
                        "type": "healthcheck_response",
                        "hostname": self.hostname,
                        "status": "healthy"
                    }
                    await websocket.send(json.dumps(response))
        except websockets.exceptions.ConnectionClosed:
            pass
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[Health] Error handling messages: {e}")