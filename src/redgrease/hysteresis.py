import logging
from threading import Timer
from typing import Callable, Dict, Hashable, Optional

log = logging.getLogger(__name__)


def nop(*args, **kwargs):
    """No Operation: Do nothing, regardless what is passed as arguments"""
    pass


class ResettableTimer:
    """Simple reresttable timer that will call a function when the timer elapses.
    Both the timer, as well as the function and its arguments can be reset
    when/if the timer is reset.
    """

    def __init__(self):
        """Create timer"""
        self.handler = nop
        self.handler_args = []
        self.handler_kwargs = {}
        self.timer = None

    def __del__(self):
        """Destructor. Cancels timer"""
        self.cancel()

    def cancel(self):
        """Cancel the timer (if set) so that it wont elapse"""
        if self.timer:
            try:
                self.timer.cancel()
            except Exception:
                pass
        self.timer = None

    def set(
        self,
        timeout: Optional[float],
        handler: Callable[..., None],
        *handler_args,
        **handler_kwargs
    ):
        """Reset the timer and callback

        Args:
            timeout (float): New timeout, in seconds.

            handler (Callable[..., None]): Handler function.

            Note that any additional positional and/or keyword arguments will
            be forwarded as arguments to the handler, when the timer elapses.
        """
        self.cancel()
        self.handler = handler
        self.handler_args = handler_args
        self.handler_kwargs = handler_kwargs
        self.timer = Timer(timeout if timeout else 1.0, self.trigger)
        self.timer.start()

    def trigger(self):
        """Trigger the handler immediately. Will cancel the timer."""
        self.cancel()
        if self.handler:
            self.handler(*self.handler_args, **self.handler_kwargs)


class HysteresisHandlerIndex:
    """
    Index of signal handlers, triggering each handler only when its'
    corresponding signal has not been registered for a set amount of time.
    This is useful for holding back some event handler as long as new events
    are still occuring, triggering only when the events stop.
    For example loading a file when there hasnt been any modifications to it
    for a period of time
    """

    def __init__(self, hysteresis_duration: Optional[float] = 1.0):
        """Initialize the Index.

        Args:
            hysteresis_duration (float, optional): The required number of
            seconds without any signals for a given handler for it to trigger.
            Defaults to 1.0 second.
        """
        self.hysteresis_duration = hysteresis_duration
        self.handlers: Dict[Hashable, ResettableTimer] = {}

    def signal(
        self,
        handler_id: Hashable,
        handler_function: Callable[..., None],
        *handler_args,
        **handler_kwargs
    ):
        """Register a new signal for a specific handler.

        Args:
            handler_id ([Hashable]): Id of the handler / signal

            handler_function (Callable[..., None]): Signal handler function.

            Note that any additional positional and/or keyword arguments will
            be forwarded as arguments to the handler, when triggered.
        """
        if handler_id not in self.handlers:
            self.handlers[handler_id] = ResettableTimer()

        self.handlers[handler_id].set(
            self.hysteresis_duration, handler_function, *handler_args, **handler_kwargs
        )
