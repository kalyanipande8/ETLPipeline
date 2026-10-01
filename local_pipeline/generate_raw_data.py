"""
generate_raw_data.py

Generates a synthetic raw "events" dataset standing in for the kind of
transactional/event data a 100+ GB/day production ETL pipeline would ingest
(e.g. clickstream, order, or sensor events). Scaled down to a size that
actually, safely fits and runs on this machine (disk was critically low at
build time) while keeping the same column shapes, dtypes, and data-quality
problems (nulls, out-of-range values, duplicates) that a real pipeline has
to handle.

A fixed random seed makes row counts and injected defect counts
deterministic across runs.
"""
import os
import random
import string
import numpy as np
import pandas as pd

SEED = 42
N_ROWS = 400_000  # ~400K rows -> tens of MB, disk-safe; see README for scale note

REGIONS = ["us-east", "us-west", "eu-west", "eu-central", "ap-south"]
EVENT_TYPES = ["page_view", "add_to_cart", "purchase", "signup", "refund"]
DEVICE_TYPES = ["mobile", "desktop", "tablet"]

OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")


def _rand_user_id(rng: random.Random) -> str:
    return "u_" + "".join(rng.choices(string.ascii_lowercase + string.digits, k=10))


def generate(n_rows: int = N_ROWS, seed: int = SEED) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    pyrng = random.Random(seed)

    user_pool = [_rand_user_id(pyrng) for _ in range(20_000)]

    event_ids = np.arange(1, n_rows + 1)
    user_ids = rng.choice(user_pool, size=n_rows)
    regions = rng.choice(REGIONS, size=n_rows, p=[0.30, 0.25, 0.20, 0.15, 0.10])
    event_types = rng.choice(
        EVENT_TYPES, size=n_rows, p=[0.55, 0.20, 0.12, 0.08, 0.05]
    )
    device_types = rng.choice(DEVICE_TYPES, size=n_rows, p=[0.6, 0.3, 0.1])

    # order_value is only meaningful for purchase/refund events, NaN otherwise
    order_value = np.where(
        np.isin(event_types, ["purchase", "refund"]),
        rng.gamma(shape=2.0, scale=35.0, size=n_rows).round(2),
        np.nan,
    )

    # session_duration_sec: realistic log-normal distribution
    session_duration_sec = rng.lognormal(mean=4.0, sigma=1.0, size=n_rows).round(1)

    # timestamps across a 30-day window
    base = pd.Timestamp("2026-08-01")
    offsets_sec = rng.integers(0, 30 * 24 * 3600, size=n_rows)
    timestamps = base + pd.to_timedelta(offsets_sec, unit="s")

    df = pd.DataFrame(
        {
            "event_id": event_ids,
            "user_id": user_ids,
            "event_ts": timestamps,
            "region": regions,
            "event_type": event_types,
            "device_type": device_types,
            "session_duration_sec": session_duration_sec,
            "order_value": order_value,
        }
    )

    # --- Inject realistic data-quality defects (so QC has real work to do) ---

    # 1. Completeness: null out ~1.2% of user_id (simulating dropped client IDs)
    null_idx = rng.choice(n_rows, size=int(n_rows * 0.012), replace=False)
    df.loc[null_idx, "user_id"] = None

    # 2. Range violation: ~0.3% of session_duration_sec get a negative/bogus value
    bad_dur_idx = rng.choice(n_rows, size=int(n_rows * 0.003), replace=False)
    df.loc[bad_dur_idx, "session_duration_sec"] = -1.0

    # 3. Range violation: ~0.2% of order_value get an absurd outlier (sensor/entry error)
    purchase_like = df.index[df["event_type"].isin(["purchase", "refund"])]
    bad_val_idx = rng.choice(
        purchase_like, size=max(1, int(len(purchase_like) * 0.002)), replace=False
    )
    df.loc[bad_val_idx, "order_value"] = 999_999.99

    # 4. Duplicate rows: append an exact-duplicate 0.5% sample of rows
    dup_sample = df.sample(frac=0.005, random_state=seed)
    df = pd.concat([df, dup_sample], ignore_index=True)

    # 5. Schema violation: a handful of rows get an invalid event_type value
    schema_bad_idx = rng.choice(len(df), size=50, replace=False)
    df.loc[schema_bad_idx, "event_type"] = "UNKNOWN_EVENT"

    return df


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    df = generate()
    out_path = os.path.join(OUT_DIR, "raw_events.csv")
    df.to_csv(out_path, index=False)
    size_mb = os.path.getsize(out_path) / (1024 * 1024)
    print(f"Generated {len(df):,} raw rows -> {out_path} ({size_mb:.1f} MB)")
    return out_path


if __name__ == "__main__":
    main()
