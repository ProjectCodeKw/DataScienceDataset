#!/usr/bin/env python3
"""Combine multiple final CSV files into one, grouping rows by game title.

Usage:
  python combine_datasets.py --input "final_datasets/*.csv" --output combined_reviews_by_game.csv

If --input is a directory, all .csv files inside will be used.
"""

import argparse
import glob
import os
import pandas as pd


def load_and_standardize(path, all_columns):
    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        df = pd.read_csv(path, encoding="utf-8", errors="replace")

    # Ensure all expected columns exist
    for col in all_columns:
        if col not in df.columns:
            df[col] = "N/A"

    # Keep only the expected columns (in order)
    df = df[all_columns]
    return df


def combine_csvs(pattern_or_dir, output_file):
    # Standard column list matching other scrapers
    # Use the column set used by the ARABIC datasets
    # These Arabic datasets have the following canonical columns:
    all_columns = [
        "game_name",
        "app_id",
        "review_text",
        "user_score",
        "voted_up",
        "source",
    ]

    # Resolve input pattern
    if os.path.isdir(pattern_or_dir):
        pattern = os.path.join(pattern_or_dir, "*.csv")
    else:
        pattern = pattern_or_dir

    files = sorted(glob.glob(pattern))
    if not files:
        print(f"No CSV files found for pattern: {pattern}")
        return None

    parts = []
    for p in files:
        print(f"Loading {p}...")
        df = load_and_standardize(p, all_columns)
        parts.append(df)

    combined = pd.concat(parts, ignore_index=True)
    combined = combined.fillna("N/A")

    # Sort by game_name (case-insensitive) so identical titles are contiguous
    # Normalize game_name whitespace and sort by it (case-insensitive)
    if "game_name" in combined.columns:
        combined["game_name"] = combined["game_name"].astype(str).str.strip()
        combined = combined.sort_values(by=["game_name"], key=lambda s: s.str.lower())

    # Save
    combined.to_csv(output_file, index=False, encoding="utf-8-sig")

    print(
        f"\nCombined {len(combined):,} rows from {len(files)} files into {output_file}"
    )
    return combined


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Combine CSV files grouped by game title"
    )
    parser.add_argument(
        "--input",
        "-i",
        default="final_datasets/*.csv",
        help="Input glob pattern or directory containing CSVs",
    )
    parser.add_argument(
        "--output", "-o", default="combined_reviews_by_game.csv", help="Output CSV path"
    )
    args = parser.parse_args()

    combined = combine_csvs(args.input, args.output)
    if combined is not None:
        # print a small summary
        print("\nTop games (first 10 titles):")
        print(combined["game_name"].dropna().astype(str).head(10).to_list())
