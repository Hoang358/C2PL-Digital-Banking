"""
Generate synthetic banking dataset: 1,000 accounts across 5 branches.
"""
import csv
import random
import os

random.seed(42)

BRANCHES = ["BR001-HCM", "BR002-HN", "BR003-DN", "BR004-CT", "BR005-HP"]
ACCOUNT_TYPES = ["Savings", "Checking", "Business"]

accounts = []
for i in range(1, 1001):
    account = {
        "AccountID": f"ACC{i:04d}",
        "OwnerName": f"Customer_{i:04d}",
        "BranchID": random.choice(BRANCHES),
        "AccountType": random.choice(ACCOUNT_TYPES),
        "Balance": round(random.uniform(500_000, 50_000_000), 2),  # VND
        "CreditLimit": round(random.uniform(0, 100_000_000), 2),
        "CreatedAt": f"202{random.randint(0,4)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
        "IsActive": random.choice(["True", "True", "True", "False"]),
    }
    accounts.append(account)

out_path = os.path.join(os.path.dirname(__file__), "accounts.csv")
with open(out_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=accounts[0].keys())
    writer.writeheader()
    writer.writerows(accounts)

print(f"Generated {len(accounts)} accounts → {out_path}")
