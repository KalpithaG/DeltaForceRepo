"""Optional: generate a larger synthetic data set with the same kinds of seeded issues.

Writes landing files plus a matching config folder (with its own answer key) so the
detection scorecard works on the generated data too.

    dataops_datagen --out /tmp/dfdq_demo --days 12 --sales-per-day 2000

Scenario calendar (day number within the run):
    day 3   bad data in Sales and Customer (nulls, duplicates, invalid IDs, bad values)
    day 5   Customer file delayed by 90 minutes
    day 6   Product file missing
    day 8   Sales volume drops by about 70%
    day 10  Revenue spike (quantities about 30 times normal)
"""

from __future__ import annotations

import argparse
import csv
import os
import random
from datetime import date, datetime, timedelta
from importlib import resources

REGIONS = ["North", "South", "East", "West"]
CATEGORIES = ["Lighting", "Hydration", "Wearables", "Gadgets", "Food", "Technology", "Electronics"]


def _write(path, header, rows):
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)


def generate(out: str, days: int = 12, sales_per_day: int = 2000, customers: int = 200, products: int = 40,
             start: date = date(2026, 11, 1), seed: int = 7) -> dict:
    rng = random.Random(seed)
    landing, config = os.path.join(out, "landing"), os.path.join(out, "config")
    os.makedirs(landing, exist_ok=True)
    os.makedirs(config, exist_ok=True)

    cust = [
        [f"C{i:04d}", f"Customer {i:04d}", rng.choice(REGIONS), f"contact{i:04d}@example.com",
         (date(2026, 1, 1) + timedelta(days=rng.randint(0, 280))).isoformat()]
        for i in range(1, customers + 1)
    ]
    prod = [
        [f"P{i:03d}", f"Product {i:03d}", rng.choice(CATEGORIES), rng.choice([120, 250, 350, 400, 500, 1500, 2000]),
         (date(2025, 1, 1) + timedelta(days=rng.randint(0, 300))).isoformat()]
        for i in range(1, products + 1)
    ]
    price = {p[0]: p[3] for p in prod}
    pairs = [(c[0], p[0]) for c in cust for p in prod]
    issues, sale_no = [], 0

    def issue(day, dataset, rule, key, text):
        issues.append([f"G{len(issues) + 1:04d}", day.isoformat(), dataset, rule, key or "", "Y", text])

    for n in range(1, days + 1):
        day = start + timedelta(days=n - 1)
        stamp = day.strftime("%d%m%Y")

        # ---- Customer --------------------------------------------------------
        c_rows = [list(r) for r in cust]
        if n == 3:
            picks = rng.sample(range(len(c_rows)), 15)
            for i in picks[:5]:
                c_rows[i][1] = ""
                issue(day, "Customer", "customer_name_missing", c_rows[i][0], "Seeded missing name")
            for i in picks[5:10]:
                c_rows[i][3] = "invalid_email"
                issue(day, "Customer", "customer_email_invalid", c_rows[i][0], "Seeded invalid email")
            for i in picks[10:12]:
                c_rows[i][4] = "2027-01-01"
                issue(day, "Customer", "customer_created_date_future", c_rows[i][0], "Seeded future date")
            for i in picks[12:15]:
                c_rows.append(list(c_rows[i]))
                issue(day, "Customer", "customer_id_duplicate", c_rows[i][0], "Seeded duplicate customer")
        c_time = "093000" if n == 5 else "080000"
        if n == 5:
            issue(day, "Customer", "file_late", None, "Customer file delayed by 90 minutes")
        _write(os.path.join(landing, f"Customer_{stamp}_{c_time}.csv"),
               ["Customer_ID", "Customer_Name", "Region", "Email", "Created_Date"], c_rows)

        # ---- Product ---------------------------------------------------------
        if n == 6:
            issue(day, "Product", "file_missing", None, "Product file not delivered")
        else:
            _write(os.path.join(landing, f"Product_{stamp}_080000.csv"),
                   ["Product_ID", "Product_Name", "Category", "Price", "Launch_Date"], prod)

        # ---- Sales -----------------------------------------------------------
        volume = int(sales_per_day * rng.uniform(0.95, 1.05))
        if n == 8:
            volume = int(volume * 0.3)
            issue(day, "Sales", "sales_volume_drop", None, "Sales volume drops by about 70%")
            issue(day, "Sales", "revenue_drop", None, "Sales revenue drops by about 70%")
        factor = 30 if n == 10 else 1
        if n == 10:
            issue(day, "Sales", "revenue_spike", None, "Revenue spike of about 30 times normal")

        s_rows = []
        for c_id, p_id in rng.sample(pairs, volume):
            sale_no += 1
            qty = rng.randint(1, 20) * factor
            s_rows.append([f"S{sale_no:07d}", day.isoformat(), c_id, p_id, qty, qty * price[p_id]])

        if n == 3:
            picks = rng.sample(range(len(s_rows)), 130)

            def seed_rows(idx, rule, text, change):
                for i in idx:
                    change(s_rows[i])
                    issue(day, "Sales", rule, s_rows[i][0], text)

            seed_rows(picks[0:20], "sale_date_missing", "Seeded missing sale date", lambda r: r.__setitem__(1, ""))
            seed_rows(picks[20:40], "invalid_customer_ref", "Seeded unknown customer", lambda r: r.__setitem__(2, "C9999"))
            seed_rows(picks[40:60], "invalid_product_ref", "Seeded unknown product", lambda r: r.__setitem__(3, "P999"))

            def negative(r):
                r[4], r[5] = -r[4], -r[5]

            seed_rows(picks[60:75], "quantity_not_positive", "Seeded negative quantity", negative)
            for i in picks[60:75]:
                issue(day, "Sales", "revenue_negative", s_rows[i][0], "Seeded negative revenue")
            seed_rows(picks[75:90], "revenue_price_mismatch", "Seeded revenue mismatch", lambda r: r.__setitem__(5, r[5] + 77))
            seed_rows(picks[90:100], "sale_date_future", "Seeded future sale date",
                      lambda r: r.__setitem__(1, (day + timedelta(days=25)).isoformat()))

            def outlier(r):
                r[4] = r[4] * 300
                r[5] = r[4] * price[r[3]]

            seed_rows(picks[100:110], "quantity_outlier", "Seeded quantity outlier", outlier)
            for i in picks[110:130]:
                sale_no += 1
                copy = list(s_rows[i])
                copy[0] = f"S{sale_no:07d}"
                s_rows.append(copy)
                issue(day, "Sales", "duplicate_sale", copy[0], "Seeded duplicate sale")

        _write(os.path.join(landing, f"Sales_{stamp}_080000.csv"),
               ["Sales_ID", "Sale_Date", "Customer_ID", "Product_ID", "Quantity", "Revenue"], s_rows)

    # ---- config folder: packaged defaults + this run's answer key -------------
    defaults = resources.files("deltaforce_dq").joinpath("default_config")
    for item in defaults.iterdir():
        if item.name.endswith(".csv") and item.name != "expected_issues.csv":
            text = item.read_text(encoding="utf-8")
            if item.name == "monitored_datasets.csv":
                # Generated data gets its own source tables, so it can never replace the real ones.
                text = text.replace("{catalog}.default.", "{catalog}.{schema}.generated_")
            with open(os.path.join(config, item.name), "w", encoding="utf-8", newline="") as fh:
                fh.write(text)
    _write(os.path.join(config, "expected_issues.csv"),
           ["issue_id", "file_date", "dataset", "rule_name", "record_key", "official_scenario", "description"], issues)
    return {"landing_path": landing, "config_path": config, "seeded_issues": len(issues), "sales_rows": sale_no}


def main(argv=None) -> None:
    p = argparse.ArgumentParser(prog="dataops_datagen", description="Generate extended synthetic data")
    p.add_argument("--out", required=True, help="Output folder (landing/ and config/ are created inside)")
    p.add_argument("--days", type=int, default=12)
    p.add_argument("--sales-per-day", type=int, default=2000)
    p.add_argument("--start", default="2026-11-01", help="First file date, YYYY-MM-DD")
    args = p.parse_args(argv)
    info = generate(args.out, args.days, args.sales_per_day, start=datetime.strptime(args.start, "%Y-%m-%d").date())
    print(info)


if __name__ == "__main__":
    main()
