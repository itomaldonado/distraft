import asyncio
import logging

logger = logging.getLogger(__name__)


class EventTimer:
    """Schedules events on a timer
    args:
        - callback: function to call when timer runs out
        - interval: (function or number) should return or *be* a number to sleep for before callback
    """
    def __init__(self, interval, callback):
        self.interval = interval
        self.callback = callback
        self.loop = asyncio.get_event_loop()

        self.is_active = False

    def start(self):
        self.is_active = True
        self.handler = self.loop.call_later(self.get_interval(), self._run)

    def _run(self):
        if self.is_active:
            self.callback()
            self.handler = self.loop.call_later(self.get_interval(), self._run)

    def stop(self):
        self.is_active = False
        self.handler.cancel()

    def reset(self):
        self.stop()
        self.start()

    def get_interval(self):
        return self.interval() if callable(self.interval) else self.interval
