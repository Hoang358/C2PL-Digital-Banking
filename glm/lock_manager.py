"""
Global Lock Manager (GLM) - Centralized 2-Phase Locking
=========================================================
Implements a single centralized lock manager that all branch nodes
must contact before accessing any data item.

Lock Compatibility Matrix:
         Held: RL   WL
Request: RL  [YES, NO ]
         WL  [NO,  NO ]
"""
import threading
import time
import uuid
import json
import logging
from enum import Enum
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Set
from collections import defaultdict, deque

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [GLM] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("GLM")


class LockType(str, Enum):
    READ  = "READ"
    WRITE = "WRITE"


class TxStatus(str, Enum):
    ACTIVE    = "ACTIVE"
    COMMITTED = "COMMITTED"
    ABORTED   = "ABORTED"
    WAITING   = "WAITING"


@dataclass
class LockRequest:
    tx_id:      str
    item_id:    str
    lock_type:  LockType
    branch_id:  str
    requested_at: float = field(default_factory=time.time)
    granted_at:   Optional[float] = None
    wait_time_ms: float = 0.0
    granted:      bool  = False


@dataclass
class Transaction:
    tx_id:      str
    branch_id:  str
    start_time: float = field(default_factory=time.time)
    end_time:   Optional[float] = None
    status:     TxStatus = TxStatus.ACTIVE
    held_locks: List[str] = field(default_factory=list)   # item_ids
    wait_queue: List[str] = field(default_factory=list)   # item_ids waiting
    total_wait_ms: float  = 0.0
    operations:   int     = 0

    @property
    def duration_ms(self):
        end = self.end_time or time.time()
        return (end - self.start_time) * 1000


class GlobalLockManager:
    """
    Centralized 2-Phase Locking Manager.

    Data structures:
      lock_table  : item_id → { readers: set[tx_id], writer: tx_id|None }
      wait_queue  : item_id → deque of LockRequest (FIFO)
      tx_table    : tx_id   → Transaction
    """

    def __init__(self):
        self._mu         = threading.Lock()
        self.lock_table  : Dict[str, Dict]           = defaultdict(lambda: {"readers": set(), "writer": None})
        self.wait_queue  : Dict[str, deque]          = defaultdict(deque)
        self.tx_table    : Dict[str, Transaction]    = {}
        self.request_log : List[LockRequest]         = []
        self.metrics     : Dict                      = {
            "total_requests": 0,
            "granted_immediately": 0,
            "queued": 0,
            "aborts": 0,
            "commits": 0,
            "total_wait_ms": 0.0,
        }
        self._start_time = time.time()
        logger.info("Global Lock Manager initialized.")

    # ── Public API ────────────────────────────────────────────────────────────

    def begin_transaction(self, branch_id: str) -> str:
        tx_id = f"TX-{branch_id}-{uuid.uuid4().hex[:6].upper()}"
        with self._mu:
            self.tx_table[tx_id] = Transaction(tx_id=tx_id, branch_id=branch_id)
        logger.debug(f"BEGIN {tx_id} from {branch_id}")
        return tx_id

    def request_lock(self, tx_id: str, item_id: str, lock_type: LockType,
                     timeout_s: float = 5.0) -> bool:
        """
        Request a lock. Blocks until granted or timeout.
        Returns True if granted, False if timed out (caller should abort).
        """
        req = LockRequest(
            tx_id=tx_id, item_id=item_id, lock_type=lock_type,
            branch_id=self.tx_table[tx_id].branch_id if tx_id in self.tx_table else "UNKNOWN",
        )
        granted_event = threading.Event()

        with self._mu:
            self.metrics["total_requests"] += 1
            tx = self.tx_table.get(tx_id)
            if tx is None or tx.status != TxStatus.ACTIVE:
                return False

            if self._can_grant(tx_id, item_id, lock_type):
                self._grant(tx_id, item_id, lock_type)
                req.granted = True
                req.granted_at = time.time()
                req.wait_time_ms = 0.0
                self.request_log.append(req)
                self.metrics["granted_immediately"] += 1
                logger.debug(f"  GRANT {lock_type} on {item_id} → {tx_id} (immediate)")
                return True
            else:
                # Must wait
                req.granted_event = granted_event
                self.wait_queue[item_id].append(req)
                self.request_log.append(req)
                self.metrics["queued"] += 1
                if tx_id in self.tx_table:
                    self.tx_table[tx_id].status = TxStatus.WAITING
                    self.tx_table[tx_id].wait_queue.append(item_id)
                logger.debug(f"  QUEUE {lock_type} on {item_id} ← {tx_id} (waiting)")

        # Wait outside the lock
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            if granted_event.wait(timeout=0.01):
                wait_ms = (time.time() - req.requested_at) * 1000
                req.wait_time_ms = wait_ms
                self.metrics["total_wait_ms"] += wait_ms
                with self._mu:
                    tx = self.tx_table.get(tx_id)
                    if tx:
                        tx.total_wait_ms += wait_ms
                        tx.status = TxStatus.ACTIVE
                logger.debug(f"  GRANT {lock_type} on {item_id} → {tx_id} (waited {wait_ms:.1f}ms)")
                return True

        # Timeout → abort
        logger.warning(f"  TIMEOUT {lock_type} on {item_id} for {tx_id}")
        self._remove_from_queue(tx_id, item_id)
        return False

    def release_all_locks(self, tx_id: str, commit: bool = True):
        """Release all locks held by a transaction (2PL shrinking phase)."""
        with self._mu:
            tx = self.tx_table.get(tx_id)
            if tx is None:
                return
            tx.status = TxStatus.COMMITTED if commit else TxStatus.ABORTED
            tx.end_time = time.time()
            if commit:
                self.metrics["commits"] += 1
            else:
                self.metrics["aborts"] += 1

            released_items = list(tx.held_locks)
            tx.held_locks.clear()

            for item_id in released_items:
                entry = self.lock_table[item_id]
                entry["readers"].discard(tx_id)
                if entry["writer"] == tx_id:
                    entry["writer"] = None
                self._try_grant_waiting(item_id)

        action = "COMMIT" if commit else "ABORT"
        logger.debug(f"  {action} {tx_id} | released {len(released_items)} locks")

    # ── Internals ─────────────────────────────────────────────────────────────

    def _can_grant(self, tx_id: str, item_id: str, lock_type: LockType) -> bool:
        entry = self.lock_table[item_id]
        if lock_type == LockType.READ:
            # Can grant if no writer (or writer is self)
            return entry["writer"] is None or entry["writer"] == tx_id
        else:  # WRITE
            # Can grant if no readers (except self) and no writer (except self)
            readers_excl_self = entry["readers"] - {tx_id}
            writer_excl_self  = entry["writer"] not in (None, tx_id)
            return len(readers_excl_self) == 0 and not writer_excl_self

    def _grant(self, tx_id: str, item_id: str, lock_type: LockType):
        entry = self.lock_table[item_id]
        if lock_type == LockType.READ:
            entry["readers"].add(tx_id)
        else:
            entry["writer"] = tx_id
        tx = self.tx_table.get(tx_id)
        if tx and item_id not in tx.held_locks:
            tx.held_locks.append(item_id)
        tx.operations += 1

    def _try_grant_waiting(self, item_id: str):
        """After a release, try to satisfy queued requests for item_id."""
        q = self.wait_queue[item_id]
        granted_any = True
        while q and granted_any:
            granted_any = False
            req = q[0]
            if self._can_grant(req.tx_id, item_id, req.lock_type):
                q.popleft()
                self._grant(req.tx_id, item_id, req.lock_type)
                req.granted = True
                req.granted_at = time.time()
                if hasattr(req, "granted_event"):
                    req.granted_event.set()
                granted_any = True

    def _remove_from_queue(self, tx_id: str, item_id: str):
        with self._mu:
            q = self.wait_queue[item_id]
            new_q = deque(r for r in q if r.tx_id != tx_id)
            self.wait_queue[item_id] = new_q

    # ── Snapshot / Reporting ──────────────────────────────────────────────────

    def snapshot(self) -> dict:
        with self._mu:
            active_txs    = [t for t in self.tx_table.values() if t.status == TxStatus.ACTIVE]
            waiting_txs   = [t for t in self.tx_table.values() if t.status == TxStatus.WAITING]
            completed_txs = [t for t in self.tx_table.values()
                             if t.status in (TxStatus.COMMITTED, TxStatus.ABORTED)]
            total_q = sum(len(q) for q in self.wait_queue.values())
            avg_wait = (self.metrics["total_wait_ms"] / max(1, self.metrics["queued"]))
            uptime   = time.time() - self._start_time

            return {
                "uptime_s":         round(uptime, 2),
                "metrics":          dict(self.metrics),
                "active_txs":       len(active_txs),
                "waiting_txs":      len(waiting_txs),
                "completed_txs":    len(completed_txs),
                "queue_depth":      total_q,
                "avg_wait_ms":      round(avg_wait, 2),
                "lock_table_size":  len(self.lock_table),
                "recent_txs": [
                    {
                        "tx_id":        t.tx_id,
                        "branch":       t.branch_id,
                        "status":       t.status.value,
                        "duration_ms":  round(t.duration_ms, 2),
                        "wait_ms":      round(t.total_wait_ms, 2),
                        "ops":          t.operations,
                    }
                    for t in sorted(completed_txs, key=lambda x: x.end_time or 0, reverse=True)[:20]
                ],
                "queue_snapshot": {
                    item: [{"tx": r.tx_id, "type": r.lock_type} for r in list(q)[:5]]
                    for item, q in self.wait_queue.items() if q
                },
            }

    def get_lock_queue_log(self) -> List[dict]:
        with self._mu:
            return [
                {
                    "tx_id":       r.tx_id,
                    "item_id":     r.item_id,
                    "lock_type":   r.lock_type,
                    "branch_id":   r.branch_id,
                    "requested_at": r.requested_at,
                    "wait_time_ms": round(r.wait_time_ms, 3),
                    "granted":     r.granted,
                }
                for r in self.request_log[-200:]  # last 200 requests
            ]
