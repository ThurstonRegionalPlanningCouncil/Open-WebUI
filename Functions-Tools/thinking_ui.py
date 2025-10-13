"""
title: Thinking Indicator (simple, no emoji)
description: Shows a single 'Thinking...' line while processing, then replaces it with total elapsed time.
author: me
version: 0.1.0
license: MIT
requirements: asyncio, pydantic
"""

import time
from typing import Any, Awaitable, Callable
from pydantic import BaseModel, Field


class Filter:
    class Valves(BaseModel):
        priority: int = Field(
            default=15, description="Priority for executing the filter"
        )

    def __init__(self):
        self.start_time = None

    async def inlet(
        self,
        body: dict,
        __event_emitter__: Callable[[Any], Awaitable[None]] = None,
    ) -> dict:
        """Invoked at the start of processing — show one 'Thinking...' status."""
        self.start_time = time.time()
        await __event_emitter__(
            {
                "type": "status",
                "data": {
                    "description": "Thinking...",
                    "done": False,
                },
            }
        )
        return body

    async def outlet(
        self,
        body: dict,
        __event_emitter__: Callable[[Any], Awaitable[None]] = None,
    ) -> dict:
        """Invoked after processing — replace line with total elapsed time."""
        elapsed_time = int(time.time() - (self.start_time or time.time()))
        await __event_emitter__(
            {
                "type": "status",
                "data": {
                    "description": f"Thought for {elapsed_time} seconds",
                    "done": True,
                },
            }
        )
        return body
