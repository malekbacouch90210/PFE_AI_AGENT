import asyncio
import threading
from typing import Coroutine, Any

class ProactorThread:
    """Manages a background thread running a ProactorEventLoop for Playwright."""
    def __init__(self):
        self.loop = asyncio.WindowsProactorEventLoopPolicy().new_event_loop()
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()

    def _run_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def run_coro(self, coro: Coroutine) -> Any:
        """Submits a coroutine to the proactor loop and waits for the result."""
        future = asyncio.run_coroutine_threadsafe(coro, self.loop)
        return future.result()

# Global instance to reuse across your pipeline
proactor_worker = ProactorThread()