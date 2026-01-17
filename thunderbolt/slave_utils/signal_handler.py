import signal
import asyncio


class SignalHandler:
    """Handles system signals for graceful shutdown."""
    
    def __init__(self, shutdown_callback):
        self.shutdown_callback = shutdown_callback
        self.loop = None
    
    def setup(self, loop):
        """Setup signal handlers for the given event loop."""
        self.loop = loop
        
        def handler(sig, frame):
            print(f"\n[Signal] Received signal {sig}")
            self.loop.create_task(self.shutdown_callback())
        
        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)