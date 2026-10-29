import argparse

import numpy as np
import pandas as pd
from rich.console import Console
from rich.table import Table

KEY = ["chosen_day", "days_to_fetch"]

# end of the dataset (second of the epoch pair duplicated across the repo), naive UTC like chosen_day
DATASET_END = pd.Timestamp(1780351201, unit="s")
DATASET_START = pd.Timestamp(1725608400, unit="s").normalize()
MAX_SLICES_LISTED = 14

STAT_LABELS = [
    ("mean", "Mean"),
    ("median", "Median"),
    ("std", "Std dev"),
    ("min", "Min"),
    ("max", "Max"),
    ("p95", "95th pct"),
]

console = Console()


def load(path, from_date, keep_truncated):
    df = pd.read_csv(path)

    # cancelled-while-pending runs have no chosen_day and can't be matched
    n_unkeyed = int(df["chosen_day"].isna().sum())
    df = df[df["chosen_day"].notna()].copy()
    df["chosen_day"] = pd.to_datetime(df["chosen_day"])

    # nothing to schedule: not a failure, and it has no savings to compare
    n_empty = int((df["num_jobs"] == 0).sum())
    df = df[df["num_jobs"] != 0].copy()

    if from_date is not None:
        df = df[df["chosen_day"] >= pd.Timestamp(from_date)]

    if not keep_truncated:
        span = pd.to_timedelta(df["days_to_fetch"].fillna(1) + df.get("cooldown_days", pd.Series(0, index=df.index)).fillna(0), unit="D")
        df = df[df["chosen_day"] + span <= DATASET_END]

    df = df.copy()
    df["success"] = df["carbon"].notna() & df["carbon_2"].notna() & df["num_jobs"].notna()
    df["savings_pct"] = 100 * (df["carbon"] - df["carbon_2"]) / df["carbon"]
    df["max_ram_mib"] = df["max_ram_kib"] / 1024

    n_repeats = int(df.duplicated(KEY).sum())
    if n_repeats:
        print(f"WARNING {path}: {n_repeats} repeated (chosen_day, days_to_fetch) rows; the merge will multiply them")

    print(f"{path}: {len(df)} runs after filters ({n_unkeyed} without a chosen_day and {n_empty} empty days set aside)")
    return df


def stats(series):
    return {
        "mean": series.mean(),
        "median": series.median(),
        "std": series.std(),
        "min": series.min(),
        "max": series.max(),
        "p95": series.quantile(0.95),
    }


def pct_change(baseline, candidate):
    """Relative change from baseline to candidate, baseline as denominator."""
    return round(100 * (candidate - baseline) / baseline, 2)


def colour_for(change):
    return "green" if change > 0 else ("yellow" if change == 0 else "red")


def weighted_savings(df):
    """Savings of the summed carbon, so each run counts in proportion to its carbon."""
    return 100 * (df["carbon"].sum() - df["carbon_2"].sum()) / df["carbon"].sum()


def slice_mask(df, anchor, k=0):
    """Rows whose start day is k days into a span-day cycle from anchor, so kept windows share no days."""
    span = df["days_to_fetch"].fillna(1).astype(int)
    return ((df["chosen_day"] - anchor).dt.days % span) == k


def weighted_gain(m):
    """Carbon-weighted savings of b minus a on a merged frame."""
    return (100 * (m["carbon_b"].sum() - m["carbon_2_b"].sum()) / m["carbon_b"].sum()
            - 100 * (m["carbon_a"].sum() - m["carbon_2_a"].sum()) / m["carbon_a"].sum())


def slice_gains_line(base, cand, anchor):
    """Weighted gain at every slice of a single-span table, so the arbitrary choice of slice is visible."""
    full = base[base["success"]].merge(cand[cand["success"]], on=KEY, suffixes=("_a", "_b"))
    if full.empty or full["days_to_fetch"].nunique() != 1 or int(full["days_to_fetch"].iloc[0]) < 2:
        return None
    span = int(full["days_to_fetch"].iloc[0])
    gains = [weighted_gain(full[slice_mask(full, anchor, k)]) for k in range(span)]
    if span <= MAX_SLICES_LISTED:
        by_slice = f"weighted gain by slice: {' '.join(f'{g:+.2f}' for g in gains)}"
    else:
        by_slice = (f"weighted gain over {span} slices: min {min(gains):+.2f}, "
                    f"median {float(np.median(gains)):+.2f}, max {max(gains):+.2f}")
    return f"{by_slice} (shown: {gains[0]:+.2f}; all overlapping runs {weighted_gain(full):+.2f})"


def render(title, base_f, cand_f, base, cand, anchor=None):
    """One table. Success rate uses every run on each side; everything else only runs both sides completed."""
    full_base, full_cand = base, cand
    if anchor is not None:
        base, cand = base[slice_mask(base, anchor)], cand[slice_mask(cand, anchor)]

    merged = base[base["success"]].merge(cand[cand["success"]], on=KEY, suffixes=("_a", "_b"))

    a = merged[[c for c in merged.columns if c.endswith("_a")]].rename(columns=lambda c: c[:-2])
    b = merged[[c for c in merged.columns if c.endswith("_b")]].rename(columns=lambda c: c[:-2])

    rows = []
    roundto = 3

    ok_a, ok_b = int(base["success"].sum()), int(cand["success"].sum())
    rate_a = 100 * ok_a / len(base) if len(base) else float("nan")
    rate_b = 100 * ok_b / len(cand) if len(cand) else float("nan")
    rows.append([
        "Success rate",
        f"{round(rate_a, roundto)}% ({ok_a}/{len(base)})",
        f"{round(rate_b, roundto)}% ({ok_b}/{len(cand)})",
        f"[{colour_for(ok_b - ok_a)}]{round(rate_b - rate_a, roundto)}% ({ok_b - ok_a:+})[/]",
    ])
    rows.append(["Runs compared (both succeeded)", len(merged), len(merged), ""])

    if merged.empty:
        render_table(title, base_f, cand_f, rows)
        return

    for column, name in [("elapsed_sec", "runtime (s)"), ("max_ram_mib", "peak RAM (MiB)")]:
        s_a, s_b = stats(a[column]), stats(b[column])
        for key, label in STAT_LABELS:
            change = -pct_change(s_a[key], s_b[key])  # lower is better: decrease -> positive
            rows.append([f"{label} {name}", s_a[key], s_b[key], f"[{colour_for(change)}]{change}%[/]"])

    s_a, s_b = stats(a["savings_pct"]), stats(b["savings_pct"])
    for key, label in STAT_LABELS:
        change = round(s_b[key] - s_a[key], roundto)
        rows.append([f"{label} per-day carbon savings %", s_a[key], s_b[key], f"[{colour_for(change)}]{change}%[/]"])

    w_a, w_b = weighted_savings(a), weighted_savings(b)
    change = round(w_b - w_a, roundto)
    rows.append(["Carbon-weighted savings %", w_a, w_b, f"[{colour_for(change)}]{change}%[/]"])

    delta = b["savings_pct"].to_numpy() - a["savings_pct"].to_numpy()
    tied = np.isclose(delta, 0, atol=1e-9, rtol=0)
    rows.append([
        "Days improved / worsened / tied",
        "",
        f"{int(((delta > 0) & ~tied).sum())}/{int(((delta < 0) & ~tied).sum())}/{int(tied.sum())}",
        "",
    ])

    flipped = merged[(merged["savings_pct_a"] > 0) != (merged["savings_pct_b"] > 0)]
    to_pos = int(((flipped["savings_pct_a"] <= 0) & (flipped["savings_pct_b"] > 0)).sum())
    to_neg = len(flipped) - to_pos
    n_neg_a, n_neg_b = int((a["savings_pct"] <= 0).sum()), int((b["savings_pct"] <= 0).sum())
    rows.append([
        "Days with no saving (<= 0%)",
        n_neg_a,
        n_neg_b,
        f"[{colour_for(n_neg_a - n_neg_b)}]{n_neg_b - n_neg_a:+}[/]",
    ])
    rows.append(["Days flipped (no saving->saving / saving->none)", "", f"{to_pos}/{to_neg}", ""])

    render_table(title, base_f, cand_f, rows)

    if anchor is not None:
        line = slice_gains_line(full_base, full_cand, anchor)
        print(f"Non-overlapping: every span-th start date from {anchor.date()}."
              + (f" {line}" if line else ""))

    if len(flipped):
        print("\nRuns that flipped sign (got better->worse or worse->better):")
        for _, r in flipped.sort_values(KEY).iterrows():
            print(f'  {r["chosen_day"].date()} ({int(r["days_to_fetch"])}d): {r["savings_pct_a"]:+.2f}% -> {r["savings_pct_b"]:+.2f}%')


def render_table(title, base_f, cand_f, rows):
    table = Table(title=title)
    for header in ["Metric", base_f, cand_f, "Change"]:
        table.add_column(header)
    for row in rows:
        newrow = []
        for item in row:
            if isinstance(item, (float, np.floating)):
                item = str(round(item, 3))
            newrow.append(str(item))
        end_section = "95th pct" in newrow[0] or "Success rate" in newrow[0] or "Runs compared" in newrow[0]
        table.add_row(*newrow, end_section=end_section)
    console.print(table)


def main():
    parser = argparse.ArgumentParser(description="Compare two processed benchmark CSVs, one table per days_to_fetch span.")
    parser.add_argument("base")
    parser.add_argument("cand")
    parser.add_argument("--from-date", help="keep only runs whose chosen_day is on or after YYYY-MM-DD")
    parser.add_argument("--keep-truncated", action="store_true", help="keep windows that run past the dataset end")
    parser.add_argument("--pooled", action="store_true", help="also print one table over all spans (mixes window lengths)")
    parser.add_argument("--non-overlapping", action="store_true",
                        help="keep every span-th start date from --from-date (else the dataset start), so windows share no days")
    args = parser.parse_args()

    base = load(args.base, args.from_date, args.keep_truncated)
    cand = load(args.cand, args.from_date, args.keep_truncated)
    anchor = None
    if args.non_overlapping:
        anchor = pd.Timestamp(args.from_date) if args.from_date else DATASET_START

    spans = sorted(set(base["days_to_fetch"].dropna()) | set(cand["days_to_fetch"].dropna()))
    for span in spans:
        render(f"{int(span)}-day windows", args.base, args.cand,
               base[base["days_to_fetch"] == span], cand[cand["days_to_fetch"] == span], anchor)

    if args.pooled and len(spans) > 1:
        render("All spans pooled", args.base, args.cand, base, cand, anchor=anchor)


if __name__ == "__main__":
    main()
