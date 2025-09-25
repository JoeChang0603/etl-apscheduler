"""Discord webhook integrations used for logging and alerting."""

from __future__ import annotations

import asyncio
import hashlib
import time
from dataclasses import dataclass
from typing import List, Optional, Sequence

import httpx

from utils.logger.config import LogLevel, LoggerConfig, LogEvent
from utils.logger.handlers.base import BaseLogHandler

# ---------- Text utilities (共用) ----------
def fence_code(text: str, lang: str = "") -> str:
    """Wrap ``text`` in a fenced code block while escaping existing fences.

    :param text: Message body to wrap.
    :param lang: Optional language hint for syntax highlighting.
    :return: Code-fenced string safe for Discord rendering.
    """

    safe = text.replace("```", "```\u200b")
    return f"```{lang}\n{safe}\n```"

def calc_fence_overhead(lang: str = "") -> int:
    """Return the character overhead introduced by ``fence_code``.

    :param lang: Optional language hint.
    :return: Number of characters consumed by the code fence wrapper.
    """

    return 8 + len(lang)

def chunk_text(text: str, limit: int) -> List[str]:
    """Split ``text`` into chunks that do not exceed ``limit`` characters.

    :param text: Input string to split.
    :param limit: Maximum characters per chunk.
    :return: List of chunked substrings.
    """

    return [text[i : i + limit] for i in range(0, len(text), limit)] or [""]

# ---------- Transport (共用) ----------
class DiscordTransport:
    """Thin async wrapper around a Discord webhook endpoint."""

    def __init__(self, webhook_url: str, *, username: str | None = None,
                 avatar_url: str | None = None, suppress_mentions: bool = True,
                 http_timeout: float = 5.0):
        """Persist webhook settings and defer HTTP client creation.

        :param webhook_url: Discord webhook URL.
        :param username: Optional override for message author name.
        :param avatar_url: Optional override for avatar image.
        :param suppress_mentions: Whether to disable @everyone/@here mentions.
        :param http_timeout: HTTP client timeout in seconds.
        """
        self.url = webhook_url
        self.username = username
        self.avatar_url = avatar_url
        self.suppress_mentions = suppress_mentions
        self.http_timeout = http_timeout
        self._client: httpx.AsyncClient | None = None

    async def start(self):
        """Instantiate the shared HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self.http_timeout)

    async def shutdown(self):
        """Close the HTTP client if it exists."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def send(self, content: str, *, thread_id: int | None = None, wait: bool = False):
        """POST a message payload to the webhook, handling rate limits.

        :param content: Message body to deliver.
        :param thread_id: Optional thread identifier for forum channels.
        :param wait: Whether to request Discord to return the created message.
        """
        assert self._client is not None
        payload = {"content": content}
        if self.username: payload["username"] = self.username
        if self.avatar_url: payload["avatar_url"] = self.avatar_url
        if self.suppress_mentions: payload["allowed_mentions"] = {"parse": []}
        params = {}
        if thread_id is not None: params["thread_id"] = str(thread_id)
        if wait: params["wait"] = "true"

        try:
            r = await self._client.post(self.url, params=params, json=payload)
            if r.status_code == 429:
                try:
                    retry = float(r.json().get("retry_after", 1))
                except Exception:
                    retry = float(r.headers.get("Retry-After", "1"))
                await asyncio.sleep(max(0.0, retry))
            elif r.status_code >= 500:
                await asyncio.sleep(1.0)
        except httpx.RequestError:
            # 傳輸層避免再 logging，必要可落地備援
            pass

# ---------- 佇列 Worker 基底（共用） ----------
@dataclass
class _Batch:
    """Queued batch of log lines destined for a Discord post."""

    lines: List[str]
    thread_id: Optional[int] = None

class _DiscordQueueWorker:
    """Queue worker that batches and delivers messages to Discord."""
    def __init__(
        self,
        transport: DiscordTransport,
        *,
        queue_size: int = 1000,
        max_lines_per_post: int = 50,
        max_chars_per_post: int = 2000,
        format_as_code: bool = True,
        code_lang: str = "",
    ):
        """Configure batching behaviour and start with an empty queue.

        :param transport: Transport used to send messages.
        :param queue_size: Maximum queued batches before dropping.
        :param max_lines_per_post: Maximum lines combined per Discord post.
        :param max_chars_per_post: Maximum characters per post including fences.
        :param format_as_code: Whether to wrap batches in code fences.
        :param code_lang: Language hint for code fences.
        """
        self.transport = transport
        self.q: asyncio.Queue[_Batch] = asyncio.Queue(maxsize=queue_size)
        self._task: Optional[asyncio.Task] = None
        self.max_lines = max_lines_per_post
        self.max_chars = max_chars_per_post
        self.format_as_code = format_as_code
        self.code_lang = code_lang

    async def start(self):
        """Start the transport and worker task."""
        await self.transport.start()
        if self._task is None:
            self._task = asyncio.create_task(self._runner(), name=self.__class__.__name__)

    async def flush(self, timeout: float | None = None):
        """Wait until the queue is empty or timeout expires.

        :param timeout: Optional number of seconds to wait.
        """
        async def _join(): await self.q.join()
        if timeout is None:
            await _join()
        else:
            await asyncio.wait_for(_join(), timeout=timeout)

    async def shutdown(self, timeout: float | None = 5.0):
        """Flush pending messages and stop the worker and transport.

        :param timeout: Optional number of seconds to wait for queue drain.
        """
        # 先等 queue 清空，避免尚未送出的訊息被丟掉
        try:
            if timeout is None:
                await self.q.join()
            else:
                await asyncio.wait_for(self.q.join(), timeout)
        except asyncio.TimeoutError:
            pass  # 超時就直接收掉，避免卡住

        # 再停掉 worker
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

        # 關閉 HTTP client
        await self.transport.shutdown()

    async def enqueue_lines(self, lines: Sequence[str], *, thread_id: int | None = None):
        """Queue a sequence of message lines for asynchronous delivery.

        :param lines: Lines to enqueue as a single batch.
        :param thread_id: Optional thread identifier.
        """
        if not lines:
            return
        try:
            self.q.put_nowait(_Batch(lines=list(lines), thread_id=thread_id))
        except asyncio.QueueFull:
            # 熱路徑防阻塞：直接丟棄
            pass

    async def _runner(self):
        """Continuously process queued batches and send them to Discord."""
        eff_max = self.max_chars - (calc_fence_overhead(self.code_lang) if self.format_as_code else 0)
        eff_max = max(1, eff_max)

        while True:
            batch = await self.q.get()
            chunk, total = [], 0
            for ln in batch.lines:
                # 先把單行切到 eff_max
                while len(ln) > eff_max:
                    piece, ln = ln[:eff_max], ln[eff_max:]
                    # 送出目前聚合
                    if chunk:
                        await self._send("\n".join(chunk), batch.thread_id)
                        chunk, total = [], 0
                    await self._send(piece, batch.thread_id)
                # 再嘗試聚合
                if len(chunk) >= self.max_lines or (total + len(ln) + 1) > eff_max:
                    await self._send("\n".join(chunk), batch.thread_id)
                    chunk, total = [], 0
                chunk.append(ln)
                total += len(ln) + 1
            if chunk:
                await self._send("\n".join(chunk), batch.thread_id)
            self.q.task_done()

    async def _send(self, text: str, thread_id: int | None):
        """Send a prepared block of text using the transport."""
        if self.format_as_code:
            text = fence_code(text, self.code_lang)
        await self.transport.send(text, thread_id=thread_id)

# ---------- Handler：給 Logger 用 ----------
class DiscordHandler(BaseLogHandler, _DiscordQueueWorker):  # ← BaseLogHandler 放第一
    """Log handler that ships buffered log events to Discord."""
    def __init__(
        self,
        webhook_url: str,
        *,
        min_level: LogLevel = LogLevel.ERROR,
        queue_size: int = 1000,
        max_lines_per_post: int = 50,
        max_chars_per_post: int = 1900,
        username: str | None = None,
        avatar_url: str | None = None,
        suppress_mentions: bool = True,
        thread_id: int | None = None,
        format_as_code: bool = True,
        code_lang: str = "",
        http_timeout: float = 5.0,
    ):
        BaseLogHandler.__init__(self)  # ← 明確呼叫
        transport = DiscordTransport(
            webhook_url, username=username, avatar_url=avatar_url,
            suppress_mentions=suppress_mentions, http_timeout=http_timeout,
        )
        _DiscordQueueWorker.__init__(
            self, transport,
            queue_size=queue_size,
            max_lines_per_post=max_lines_per_post,
            max_chars_per_post=max_chars_per_post,
            format_as_code=format_as_code,
            code_lang=code_lang,
        )
        self.min_level = min_level
        self.default_thread_id = thread_id
        self._primary_config: LoggerConfig | None = None

    def add_primary_config(self, config: LoggerConfig):
        self._primary_config = config

    async def start(self):
        await _DiscordQueueWorker.start(self)

    async def shutdown(self):
        await _DiscordQueueWorker.shutdown(self)

    async def push(self, records: list[LogEvent]):
        lines = [ev.text for ev in records if ev.level.value >= self.min_level.value]
        await self.enqueue_lines(lines, thread_id=self.default_thread_id)

# ---------- Alerter：業務觸發用 ----------
class DiscordAlerter(_DiscordQueueWorker):
    """High-level alert helper with cooldown and dedupe support."""
    def __init__(
        self,
        webhook_url: str,
        *,
        username: str | None = None,
        avatar_url: str | None = None,
        suppress_mentions: bool = True,
        queue_size: int = 1000,
        max_lines_per_post: int = 50,
        max_chars_per_post: int = 1900,
        default_thread_id: int | None = None,
        format_as_code: bool = True,
        code_lang: str = "",
        default_cooldown_sec: int = 60,
        enable_dedupe: bool = True,
        http_timeout: float = 5.0,
    ):
        transport = DiscordTransport(
            webhook_url, username=username, avatar_url=avatar_url,
            suppress_mentions=suppress_mentions, http_timeout=http_timeout
        )
        super().__init__(
            transport,
            queue_size=queue_size, max_lines_per_post=max_lines_per_post,
            max_chars_per_post=max_chars_per_post, format_as_code=format_as_code,
            code_lang=code_lang,
        )
        self.default_thread_id = default_thread_id
        self.default_cooldown = default_cooldown_sec
        self.enable_dedupe = enable_dedupe
        self._last_sent_at: dict[str, float] = {}
        self._last_hash: dict[str, str] = {}

    @staticmethod
    def _prefix_for(level: LogLevel) -> str:
        """Return the emoji prefix associated with ``level``."""
        return {
            LogLevel.CRITICAL: "🔥",
            LogLevel.ERROR: "❌",
            LogLevel.WARNING: "⚠️",
            LogLevel.INFO: "ℹ️",
            LogLevel.DEBUG: "🐞",
            LogLevel.TRACE: "🔍",
        }.get(level, "")

    async def trigger(
        self,
        key: str,
        message: str,
        *,
        severity: LogLevel = LogLevel.WARNING,
        cooldown_sec: int | None = None,
        thread_id: int | None = None,
    ):
        now = time.time()
        cd = self.default_cooldown if cooldown_sec is None else cooldown_sec
        tid = self.default_thread_id if thread_id is None else thread_id

        if cd > 0:
            last = self._last_sent_at.get(key, 0.0)
            if now - last < cd:
                if self.enable_dedupe:
                    h = hashlib.sha256(message.encode("utf-8")).hexdigest()
                    if self._last_hash.get(key) == h:
                        return
                else:
                    return
            self._last_sent_at[key] = now
            if self.enable_dedupe:
                self._last_hash[key] = hashlib.sha256(message.encode("utf-8")).hexdigest()

        prefix = self._prefix_for(severity)
        text = f"{prefix} {message}" if prefix else message
        await self.enqueue_lines([text], thread_id=tid)
