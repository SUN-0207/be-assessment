import csv
import random
import sys
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

CURRENCIES = ["USD", "EUR", "GBP", "JPY", "SGD"]
BASE_RATES = {
    "USD": Decimal("1.0"),
    "EUR": Decimal("1.0832"),
    "GBP": Decimal("1.2710"),
    "JPY": Decimal("0.00642"),
    "SGD": Decimal("0.7395"),
}
START_DATE = date(2026, 6, 15)


def _rate_for(currency: str, day_index: int) -> Decimal:
    if currency == "USD":
        return Decimal("1.0")
    # Deterministic per-day variation so per-(currency,date) conversion matters.
    factor = Decimal("1") + Decimal(day_index) / Decimal("1000")
    return (BASE_RATES[currency] * factor).quantize(Decimal("0.000001"))


def generate(
    seed: int,
    n_transactions: int,
    out_dir: str,
    n_ids: int = 900,
    n_dates: int = 10,
) -> tuple[str, str]:
    rng = random.Random(seed)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    dates = [START_DATE + timedelta(days=i) for i in range(n_dates)]
    ids = list(range(100, 100 + n_ids))  # 100..999 for n_ids=900

    tx_path = out / "transactions.csv"
    with open(tx_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id", "name", "plus", "minus", "currency", "date"])
        for _ in range(n_transactions):
            acc = rng.choice(ids)
            cur = rng.choice(CURRENCIES)
            d = rng.choice(dates)
            plus = Decimal(rng.randint(0, 200000)) / 100
            minus = Decimal(rng.randint(0, 200000)) / 100
            w.writerow([acc, f"acct{acc}", f"{plus:.2f}", f"{minus:.2f}", cur, d.isoformat()])

    rates_path = out / "rates.csv"
    with open(rates_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date", "currency", "rate"])
        for day_index, d in enumerate(dates):
            for cur in CURRENCIES:
                w.writerow([d.isoformat(), cur, str(_rate_for(cur, day_index))])

    return str(tx_path), str(rates_path)


def main() -> None:
    seed = int(sys.argv[1]) if len(sys.argv) > 1 else 42
    n = int(sys.argv[2]) if len(sys.argv) > 2 else 50000
    out_dir = sys.argv[3] if len(sys.argv) > 3 else "."
    tx, rates = generate(seed=seed, n_transactions=n, out_dir=out_dir)
    print(f"wrote {tx} and {rates}")


if __name__ == "__main__":
    main()
