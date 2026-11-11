"""Transport interface so consensus logic can be tested without sockets."""

from __future__ import annotations

from typing import Protocol

from dchat.protocol.messages import PeerMessageT


class Transport(Protocol):
    def send(self, peer_id: str, message: PeerMessageT) -> None:
        """Best-effort, non-blocking. Raft tolerates loss, duplication and reordering."""

    def set_peer_address(self, peer_id: str, addr: str) -> None:
        """Teach the transport where a peer listens (host:port)."""
