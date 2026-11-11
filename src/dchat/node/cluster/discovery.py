"""Seed-node discovery: a node that is not yet a member asks seeds to let it join.

No broadcast or multicast. The request is redirected to the leader (JOIN_RESPONSE with
``not_leader`` carries the leader's address). The leader adds us as a learner and promotes
us after we catch up.
"""

from __future__ import annotations

import asyncio
import logging

from dchat.protocol.messages import JoinRequest, JoinResponse

from ..network.transport import Transport
from ..raft.core import RaftNode

log = logging.getLogger("dchat.discovery")


class JoinDriver:
    def __init__(
        self, raft: RaftNode, transport: Transport, seeds: tuple[str, ...], interval: float = 1.0
    ) -> None:
        self.raft = raft
        self.transport = transport
        self.seeds = [s for s in seeds if s != raft.addr]
        self.interval = interval

    def on_join_response(self, msg: JoinResponse) -> None:
        self.raft.logger.info("join response from %s: %s", msg.sender, msg.status)
        if (
            not msg.accepted
            and msg.leader_id
            and msg.leader_addr
            and msg.leader_id != self.raft.node_id
        ):
            self.transport.set_peer_address(msg.leader_id, msg.leader_addr)
            self.transport.send(msg.leader_id, self._request())

    def _request(self) -> JoinRequest:
        return JoinRequest(sender=self.raft.node_id, term=self.raft.term, addr=self.raft.addr)

    async def run(self) -> None:
        """Retry until the cluster configuration (replicated in the log) includes this node."""
        while not self.raft.is_member:
            for seed in self.seeds:
                peer_id = f"seed:{seed}"
                self.transport.set_peer_address(peer_id, seed)
                self.transport.send(peer_id, self._request())
            await asyncio.sleep(self.interval)
        self.raft.logger.info("joined cluster as %s", "voter" if self.raft.is_voter else "learner")
