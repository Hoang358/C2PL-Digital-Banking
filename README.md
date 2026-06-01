# C2PL Digital Banking — Project #21

**Centralized 2-Phase Locking · Category 3: Distributed Concurrency Control**

> Implements a single Global Lock Manager (GLM) that all branch nodes must contact
> before reading or writing any account record. Measures the bottleneck effect as
> concurrent transactions scale from 10 → 100.

---

## Project Structure

```
c2pl-banking/
├── branches/
│   └── branch_node.py        ← BranchNode simulation (TRANSFER, INQUIRY, DEPOSIT)
├── dashboard/
│   └── index.html            ← Interactive benchmark dashboard 
├── data/
│   ├── accounts.csv          ← 1,000 synthetic bank accounts
│   └── generate_data.py      ← re-generate dataset
├── glm/
│   └── lock_manager.py       ← GlobalLockManager (core C2PL engine)
├── logs/
│   ├── benchmark_results.json
│   ├── failure_simulation.json
│   └── failure_simulation.txt
├── server.py                 ← Flask REST API — GLM chạy tại http://localhost:5000
├── benchmark.py              ← Bottleneck analysis: 10 → 100 concurrent tx
├── failure_sim.py            ← Failure scenarios (GLM crash, branch failure, deadlock)
└── requirements.txt          ← Python dependencies
```

---

## Cài đặt

### 1. Tạo và kích hoạt virtual environment

```bash
python -m venv .venv
```

| OS | Lệnh kích hoạt |
|----|----------------|
| macOS / Linux | `source .venv/bin/activate` |
| Windows (PowerShell) | `.venv\Scripts\Activate.ps1` |
| Windows (CMD) | `.venv\Scripts\activate` |

### 2. Cài dependencies

```bash
pip install -r requirements.txt
```

---

## Chạy dự án


### Bước 1 — Khởi động GLM Server

```bash
python server.py
```

Server chạy tại **http://localhost:5000**

```
  GLM Server running on http://localhost:5000
  Dataset: 1000 accounts loaded
```

### Bước 2 — Chạy bottleneck benchmark (terminal riêng, venv đã kích hoạt)

```bash
python benchmark.py
```

Output mẫu:
```
========================================================================
  C2PL BOTTLENECK BENCHMARK  —  HIGH CONTENTION MODE
  Hot accounts: 10 accts | Hold time: 15ms | Hot ratio: 80%
========================================================================
 Concurrency      TPS   Avg Wait(ms)   Avg Dur(ms)   Success%   Aborts
------------------------------------------------------------------------
          10     66.2           15.0          15.2      100.0%        0
          20    128.1           39.9          30.1       99.0%        3
          30    133.3          108.4          44.9       98.0%        6
          50    134.2          257.0          74.3       95.3%       14
          75    133.5          456.2         112.4       91.2%       26
         100    132.8          680.1         150.0       86.7%       40
========================================================================
```

Kết quả lưu tại `logs/benchmark_results.json` .

### Bước 3 — Chạy failure simulation

```bash
python failure_sim.py
```

Mô phỏng 3 kịch bản:
- **Scenario 1** — GLM crash giữa chừng → tất cả transaction đang chạy bị abort
- **Scenario 2** — Branch BR003-DN bị kill → GLM dọn orphaned locks, branches khác tiếp tục
- **Scenario 3** — Kiểm tra deadlock prevention (lock theo thứ tự alphabetical)
