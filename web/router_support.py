"""Shared helpers used by multiple web route modules."""
from __future__ import annotations

import asyncio
from functools import partial


def _run_sync(fn, *args):
    """在线程池中执行同步函数，避免阻塞事件循环。"""
    loop = asyncio.get_event_loop()
    return loop.run_in_executor(None, partial(fn, *args))
