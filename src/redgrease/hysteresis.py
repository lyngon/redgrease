# -*- coding: utf-8 -*-
"""
Handling of events that come in bursts, without unecessary duplicate handling.

In RedGrease this is used in the cli / loader, when watching a directory and
'hot reloading' of scripts when a change is detedcted.
To avoid scripts being loaded unesssearily often while being modified.

This module is however general enough to be used for other situatins where events occur
in bursts.
"""
__author__ = "Anders Åström"
__contact__ = "anders@lyngon.com"
__copyright__ = "2021, Lyngon Pte. Ltd."
__licence__ = """The MIT License
Copyright © 2021 Lyngon Pte. Ltd.

Permission is hereby granted, free of charge, to any person obtaining a copy of this
 software and associated documentation files (the “Software”), to deal in the Software
 without restriction, including without limitation the rights to use, copy, modify,
 merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 permit persons to whom the Software is furnished to do so, subject to the following
 conditions:

The above copyright notice and this permission notice shall be included in all copies
 or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
 CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE
 OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""


from threading import Timer
from typing import Callable, Dict, Hashable, Optional


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
            timeout (float):
                New timeout, in seconds.

            handler (Callable[..., None]):
                Handler function that will be called after `timeout` duration from
                **last** received event.
                Any new events received before will 'reset' the interval.

            *handler_args (Any):
                Any additional arguments for the handler, when triggered.

            **handler_kwargs (Any):
                Any additional keword arguments for the handler, when triggered.
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
    Index of multiple signal handlers for different signals, triggering each handler
    only when its' corresponding signal has not been registered for a set amount
    of time.

    This is useful for holding back some event handler as long as new events
    are still occuring, triggering only when the events stop.
    For example loading a file when there hasnt been any modifications to it
    for a period of time
    """

    def __init__(self, hysteresis_duration: Optional[float] = 1.0):
        """Initialize the Index.

        Args:
            hysteresis_duration (float, optional):
                The required number of seconds without any signals for a given handler
                for it to trigger.
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
            handler_id ([Hashable]):
                Id / name of the handler / signal / event.

            handler_function (Callable[..., None]):
                Signal handler function.

            *handler_args (Any):
                Any additional arguments for the handler, when triggered.

            **handler_kwargs (Any):
                Any additional keword arguments for the handler, when triggered.
        """
        if handler_id not in self.handlers:
            self.handlers[handler_id] = ResettableTimer()

        self.handlers[handler_id].set(
            self.hysteresis_duration, handler_function, *handler_args, **handler_kwargs
        )
