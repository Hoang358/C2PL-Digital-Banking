"""
Branch Node Simulation
======================
Each branch runs transactions and must coordinate ALL lock requests
through the single Global Lock Manager (C2PL).

Transaction types:
  - TRANSFER  : debit src, credit dst  → 2 WRITE locks
  - INQUIRY   : read balance           → 1 READ lock
  - DEPOSIT   : credit account         → 1 WRITE lock
  - WITHDRAWAL: debit account          → 1 WRITE lock
"""
import csv
import random
import time
import threading
import logging
from typing import List, Dict, Optional
from dataclasses import dataclass

from glm.lock_manager import GlobalLockManager, LockType, TxStatus

logger = logging.getLogger("Branch")


@dataclass
class TxResult:
    tx_id:       str
    branch_id:   str
    tx_type:     str
    success:     bool
    duration_ms: float
    wait_ms:     float
    operations:  int


class BranchNode:
    """
    Simulates a bank branch that originates transactions.
    All lock requests go to the shared Global Lock Manager.
    """

    def __init__(self, branch_id: str, glm: GlobalLockManager, accounts: List[dict]):
        self.branch_id = branch_id
        self.glm       = glm
        self.accounts  = [a for a in accounts if a["BranchID"] == branch_id]
        self.all_accounts = accounts
        if not self.accounts:
            self.accounts = random.sample(accounts, min(50, len(accounts)))
        logger.info(f"Branch {branch_id} initialized with {len(self.accounts)} local accounts.")

    def _pick_account(self) -> dict:
        return random.choice(self.accounts)

    def _pick_remote_account(self) -> dict:
        """Picks any account (simulates inter-branch transfers)."""
        return random.choice(self.all_accounts)

    # ── Transaction implementations ───────────────────────────────────────────

    def do_transfer(self, amount: float) -> TxResult:
        src = self._pick_account()
        dst = self._pick_remote_account()
        if src["AccountID"] == dst["AccountID"]:
            dst = self._pick_remote_account()

        tx_id = self.glm.begin_transaction(self.branch_id)
        start = time.time()

        # Acquire locks in deterministic order (prevent deadlock by ordering)
        items = sorted([src["AccountID"], dst["AccountID"]])
        granted = True
        for item in items:
            if not self.glm.request_lock(tx_id, item, LockType.WRITE):
                granted = False
                break

        if granted:
            # Simulate work
            time.sleep(random.uniform(0.001, 0.005))
            # Check balance
            if float(src["Balance"]) >= amount:
                src["Balance"] = float(src["Balance"]) - amount
                dst["Balance"] = float(dst["Balance"]) + amount
                self.glm.release_all_locks(tx_id, commit=True)
                success = True
            else:
                self.glm.release_all_locks(tx_id, commit=False)
                success = False
        else:
            self.glm.release_all_locks(tx_id, commit=False)
            success = False

        tx = self.glm.tx_table.get(tx_id)
        return TxResult(
            tx_id=tx_id, branch_id=self.branch_id, tx_type="TRANSFER",
            success=success,
            duration_ms=(time.time() - start) * 1000,
            wait_ms=tx.total_wait_ms if tx else 0,
            operations=tx.operations if tx else 0,
        )

    def do_inquiry(self) -> TxResult:
        acc   = self._pick_account()
        tx_id = self.glm.begin_transaction(self.branch_id)
        start = time.time()

        granted = self.glm.request_lock(tx_id, acc["AccountID"], LockType.READ)
        if granted:
            time.sleep(random.uniform(0.0005, 0.002))
            _ = acc["Balance"]  # read
            self.glm.release_all_locks(tx_id, commit=True)

        tx = self.glm.tx_table.get(tx_id)
        return TxResult(
            tx_id=tx_id, branch_id=self.branch_id, tx_type="INQUIRY",
            success=granted,
            duration_ms=(time.time() - start) * 1000,
            wait_ms=tx.total_wait_ms if tx else 0,
            operations=tx.operations if tx else 0,
        )

    def do_deposit(self, amount: float) -> TxResult:
        acc   = self._pick_account()
        tx_id = self.glm.begin_transaction(self.branch_id)
        start = time.time()

        granted = self.glm.request_lock(tx_id, acc["AccountID"], LockType.WRITE)
        if granted:
            time.sleep(random.uniform(0.001, 0.003))
            acc["Balance"] = float(acc["Balance"]) + amount
            self.glm.release_all_locks(tx_id, commit=True)

        tx = self.glm.tx_table.get(tx_id)
        return TxResult(
            tx_id=tx_id, branch_id=self.branch_id, tx_type="DEPOSIT",
            success=granted,
            duration_ms=(time.time() - start) * 1000,
            wait_ms=tx.total_wait_ms if tx else 0,
            operations=tx.operations if tx else 0,
        )

    def run_random_transaction(self) -> TxResult:
        r = random.random()
        amount = random.uniform(100_000, 5_000_000)
        if r < 0.5:
            return self.do_transfer(amount)
        elif r < 0.75:
            return self.do_inquiry()
        else:
            return self.do_deposit(amount)


def load_accounts(csv_path: str) -> List[dict]:
    with open(csv_path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def run_concurrent_workload(
    glm: GlobalLockManager,
    accounts: List[dict],
    n_concurrent: int,
    n_transactions: int,
    branches: List[str],
) -> Dict:
    """
    Spin up n_concurrent threads, each running transactions until
    n_transactions total are completed.
    """
    results   = []
    results_lock = threading.Lock()
    counter   = {"done": 0}
    c_lock    = threading.Lock()

    branch_nodes = {b: BranchNode(b, glm, accounts) for b in branches}

    def worker():
        branch = random.choice(branches)
        node   = branch_nodes[branch]
        while True:
            with c_lock:
                if counter["done"] >= n_transactions:
                    return
                counter["done"] += 1
            result = node.run_random_transaction()
            with results_lock:
                results.append(result)

    start_time = time.time()
    threads = [threading.Thread(target=worker, daemon=True) for _ in range(n_concurrent)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
    elapsed = time.time() - start_time

    success_count = sum(1 for r in results if r.success)
    avg_wait  = sum(r.wait_ms for r in results) / max(1, len(results))
    avg_dur   = sum(r.duration_ms for r in results) / max(1, len(results))
    throughput = len(results) / max(0.001, elapsed)

    return {
        "n_concurrent":     n_concurrent,
        "n_transactions":   len(results),
        "elapsed_s":        round(elapsed, 3),
        "throughput_tps":   round(throughput, 2),
        "success_rate":     round(success_count / max(1, len(results)) * 100, 2),
        "avg_wait_ms":      round(avg_wait, 3),
        "avg_duration_ms":  round(avg_dur, 3),
        "aborts":           len(results) - success_count,
    }
