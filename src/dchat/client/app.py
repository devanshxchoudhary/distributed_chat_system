"""Textual terminal chat client. Displays leader/term from the backend; never decides them."""

from __future__ import annotations

import time

from textual.app import App, ComposeResult
from textual.widgets import Input, RichLog, Static

from dchat.protocol.client_messages import ErrorMessage, MessageDeliver, NodeStatus, Welcome

from .ws_client import ChatConnection, Connected, Disconnected, Event


class ChatApp(App[None]):
    CSS = """
    #status { height: 3; border: round $accent; padding: 0 1; }
    #log { height: 1fr; border: round $primary; }
    """
    BINDINGS = [("ctrl+c", "quit", "Quit")]

    def __init__(self, urls: list[str], user: str) -> None:
        super().__init__()
        self.user = user
        self.conn = ChatConnection(urls, user, self._on_event)
        self._where = "connecting..."
        self._cluster = "role=? term=? leader=?"

    def compose(self) -> ComposeResult:
        yield Static("connecting...", id="status")
        yield RichLog(id="log", wrap=True, markup=False)
        yield Input(placeholder="Type a message and press Enter", id="input")

    async def on_mount(self) -> None:
        self.run_worker(self.conn.run(), name="connection", exclusive=True)
        self.query_one("#input", Input).focus()

    async def on_input_submitted(self, event: Input.Submitted) -> None:
        text = event.value.strip()
        event.input.value = ""
        if text:
            await self.conn.send_message(text)
            self._refresh_status()

    def _refresh_status(self) -> None:
        pending = len(self.conn.pending)
        self.query_one("#status", Static).update(
            f"{self._where} | {self._cluster} | last_seq={self.conn.last_seen_seq}"
            + (f" | sending: {pending}" if pending else "")
        )

    def _on_event(self, event: Event) -> None:
        log = self.query_one("#log", RichLog)
        if isinstance(event, Connected):
            self._where = f"connected to {event.url}"
        elif isinstance(event, Disconnected):
            self._where = f"DISCONNECTED ({event.reason}); retrying"
        elif isinstance(event, (Welcome, NodeStatus)):
            self._cluster = (
                f"node={event.node_id} role={event.role} term={event.term} leader={event.leader_id}"
            )
        elif isinstance(event, MessageDeliver):
            stamp = time.strftime("%H:%M:%S", time.localtime(event.ts))
            log.write(f"[{event.seq:>4}] {stamp} {event.user}: {event.text}")
        elif isinstance(event, ErrorMessage):
            log.write(f"! {event.code}: {event.detail}")
        self._refresh_status()
