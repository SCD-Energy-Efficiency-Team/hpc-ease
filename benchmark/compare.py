import sys

import numpy as np
import pandas as pd
from rich.console import Console
from rich.table import Table


def load(path):
    df = pd.read_csv(path, parse_dates=["chosen_day"])
    df["success"] = df["exit_code"] == 0
    df["savings_pct"] = 100 * (df["carbon"] - df["carbon_2"]) / df["carbon"]
    df["max_ram_mib"] = df["max_ram_kib"] / 1024
    return df

def main():
    base_f, cand_f = sys.argv[1], sys.argv[2]
    base, cand = load(base_f), load(cand_f)
    merged = base.merge(cand, on=["chosen_day", "days_to_fetch"], suffixes=("_a", "_b"))

    def pct_change(a, b):
        return round(100 * (b - a) / a, 2)

    success_a = (base["success"]).mean() * 100
    success_b = (cand["success"]).mean() * 100
    success_a_count = base["success"].sum()
    success_b_count = cand["success"].sum()
    total_rows_a = base["success"].count()
    total_rows_b = cand["success"].count()

    def stats(series):
        return {
            "mean": series.mean(),
            "median": series.median(),
            "std": series.std(),
            "min": series.min(),
            "max": series.max(),
            "p95": series.quantile(0.95),
        }

    rt_a, rt_b = stats(base["elapsed_sec"]), stats(cand["elapsed_sec"])
    ram_a, ram_b = stats(base["max_ram_mib"]), stats(cand["max_ram_mib"])
    sav_a, sav_b = stats(base["savings_pct"]), stats(cand["savings_pct"])

    m = merged.dropna(subset=["savings_pct_a", "savings_pct_b"])
    delta = m["savings_pct_b"] - m["savings_pct_a"]
    improved = (delta > 0).sum()
    worsened = (delta < 0).sum()
    flipped = m[(m["savings_pct_a"] > 0) != (m["savings_pct_b"] > 0)]

    ###

    rows = []
    headers = ["Metric", base_f, cand_f, "Change"]
    roundto = 3

    change = success_b_count - success_a_count
    colour = "green" if change > 0 else ("yellow" if change == 0 else "red")
    rows.append([
        "Success rate",
        f"{round(success_a, roundto)}% ({success_a_count}/{total_rows_a})",
        f"{round(success_b, roundto)}% ({success_b_count}/{total_rows_b})",
        f"[{colour}]{round(success_b-success_a, roundto)}% ({success_b_count-success_a_count:+})[/]"
    ])

    stat_labels = [
        ("mean", "Mean"),
        ("median", "Median"),
        ("std", "Std dev"),
        ("min", "Min"),
        ("max", "Max"),
        ("p95", "95th pct"),
    ]

    for key, label in stat_labels:
        change = pct_change(rt_b[key], rt_a[key])
        colour = "green" if change > 0 else ("yellow" if change == 0 else "red")
        rows.append([
            f"{label} runtime (s)",
            rt_a[key],
            rt_b[key],
            f"[{colour}]{change}%[/]"
        ])

    for key, label in stat_labels:
        change = pct_change(ram_a[key], ram_b[key])
        colour = "green" if change < 0 else ("yellow" if change == 0 else "red")
        rows.append([
            f"{label} peak RAM (MiB)",
            ram_a[key],
            ram_b[key],
            f"[{colour}]{change}%[/]"
        ])

    for key, label in stat_labels:
        change = round(sav_b[key] - sav_a[key], roundto)
        colour = "green" if change > 0 else ("yellow" if change == 0 else "red")
        rows.append([
            f"{label} carbon savings %",
            sav_a[key],
            sav_b[key],
            f"[{colour}]{change}%[/]"
        ])

    rows.append([
        "Days improved / worsened",
        "",
        f"{improved}/{worsened}",
        ""
    ])

    table = Table()
    for header in headers:
        table.add_column(header)
    for row in rows:
        newrow = []
        for item in row:
            if isinstance(item, np.floating):
                item = str(round(item, 3))
            newrow.append(item)
        end_section = "95th pct" in newrow[0] or "Success rate" in newrow[0]
        table.add_row(*newrow, end_section=end_section)
    console = Console()
    console.print(table)

    if len(flipped):
        print("\nDays that flipped sign (got better->worse or worse->better):")
        for _, r in flipped.iterrows():
            print(f'  {r["chosen_day"].date()}: {r["savings_pct_a"]:+}% -> {r["savings_pct_b"]:+}%')

if __name__ == "__main__":
    main()