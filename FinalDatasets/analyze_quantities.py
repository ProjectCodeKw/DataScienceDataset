#!/usr/bin/env python3
"""Analyze dataset quantities for final English and Arabic datasets.

Prints:
- number of unique games
- number of unique genres
- number of unique reviewers
- percentage of each source (for English and Arabic datasets)
- negative vs positive reviews (percentage)

This script expects the two files to be present in the same folder:
- `final_english_dataset.csv`
- `combined_arabic_cleaned1_with_prices.csv`

No CLI args — edit the file paths below if your filenames differ.
"""

from __future__ import annotations

import os
import json
import re
from collections import Counter
from typing import Set, List

import pandas as pd


# Config: filenames (next to this script)
HERE = os.path.dirname(os.path.abspath(__file__))
EN_PATH = os.path.join(HERE, "final_english_dataset.csv")
AR_PATH = os.path.join(HERE, "combined_arabic_cleaned1_with_prices.csv")

# thresholds for mapping numeric user_score to sentiment
POS_THRESHOLD = 6.0
NEG_THRESHOLD = 4.0


def parse_user_score(val) -> float | None:
    if pd.isna(val):
        return None
    s = str(val).strip()
    # percent like '80%'
    m = re.match(r"^(\d{1,3})\s*%$", s)
    if m:
        pct = float(m.group(1))
        return max(0.0, min(100.0, pct)) / 10.0

    m = re.match(r"^(\d+(?:\.\d+)?)/(\d+(?:\.\d+)?)$", s)
    if m:
        num = float(m.group(1))
        den = float(m.group(2))
        if den == 0:
            return None
        return 10.0 * (num / den)

    m = re.match(r"^(\d+(?:\.\d+)?)$", s)
    if m:
        num = float(m.group(1))
        if num > 10:
            return min(100.0, num) / 10.0
        return num
    return None


def map_sentiment(row) -> str:
    # prefer voted_up if present
    if "voted_up" in row.index:
        v = row["voted_up"]
        if pd.isna(v):
            pass
        else:
            s = str(v).strip().lower()
            if s in ("true", "1", "yes", "y", "positive", "pos", "up"):
                return "positive"
            if s in ("false", "0", "no", "n", "negative", "neg", "down"):
                return "negative"
            if s in ("neutral", "n/a", "na"):
                return "neutral"
    # fallback to user_score
    if "user_score" in row.index:
        sc = parse_user_score(row["user_score"])
        if sc is not None:
            if sc >= POS_THRESHOLD:
                return "positive"
            if sc <= NEG_THRESHOLD:
                return "negative"
            return "neutral"
    return "unknown"


def extract_genres(series: pd.Series) -> Set[str]:
    out: Set[str] = set()
    for v in series.dropna().astype(str):
        # remove wrapping quotes
        s = v.strip().strip('"').strip()
        if not s:
            continue
        # split by comma
        parts = [p.strip() for p in s.split(",") if p.strip()]
        for p in parts:
            out.add(p)
    return out


def analyze_file(path: str) -> dict:
    print(f"Loading: {path}")
    df = pd.read_csv(path, encoding="utf-8-sig")
    cols = list(df.columns)
    print(f"Columns found: {cols}\n")

    total = len(df)
    games = df["game_name"].dropna().astype(str).str.strip().unique()
    unique_games = len(games)

    # genres
    genres = set()
    if "genres" in df.columns:
        genres = extract_genres(df["genres"])
    unique_genres = len(genres)

    # reviewers
    unique_reviewers = None
    if "user_id" in df.columns:
        unique_reviewers = int(df["user_id"].dropna().astype(str).nunique())
    else:
        # try to guess a reviewer column
        for cand in ("author", "reviewer", "user", "username"):
            if cand in df.columns:
                unique_reviewers = int(df[cand].dropna().astype(str).nunique())
                break
    if unique_reviewers is None:
        unique_reviewers = "(no reviewer id column)"

    # source distribution
    source_col = None
    for cand in ("source", "site", "source_name"):
        if cand in df.columns:
            source_col = cand
            break
    if source_col is None:
        src_counts = {"Unknown": total}
    else:
        src_counts = (
            df[source_col].fillna("Unknown").astype(str).value_counts().to_dict()
        )

    # sentiment
    sentiments = df.apply(map_sentiment, axis=1)
    sent_counts = sentiments.value_counts().to_dict()
    pos = sent_counts.get("positive", 0)
    neg = sent_counts.get("negative", 0)
    neu = sent_counts.get("neutral", 0)
    unk = sent_counts.get("unknown", 0)

    summary = {
        "path": path,
        "total_rows": int(total),
        "unique_games": int(unique_games),
        "unique_genres": int(unique_genres),
        "unique_reviewers": unique_reviewers,
        "source_counts": src_counts,
        "sentiment_counts": {
            "positive": int(pos),
            "negative": int(neg),
            "neutral": int(neu),
            "unknown": int(unk),
        },
    }
    return summary


def print_report(eng: dict, ar: dict):
    print("\n=== English dataset ===")
    print(f"Rows: {eng['total_rows']}")
    print(f"Unique games: {eng['unique_games']}")
    print(f"Unique genres: {eng['unique_genres']}")
    print(f"Unique reviewers: {eng['unique_reviewers']}")
    print("Source distribution:")
    for s, c in sorted(eng["source_counts"].items(), key=lambda x: -x[1]):
        pct = round(c / eng["total_rows"] * 100, 2) if eng["total_rows"] else 0
        print(f" - {s}: {c} rows ({pct}%)")
    sc = eng["sentiment_counts"]
    total = eng["total_rows"]
    print("Sentiment (percent):")
    for k in ("positive", "negative", "neutral", "unknown"):
        v = sc.get(k, 0)
        print(f" - {k}: {v} ({round(v/total*100,2) if total else 0}%)")

    print("\n=== Arabic dataset ===")
    print(f"Rows: {ar['total_rows']}")
    print(f"Unique games: {ar['unique_games']}")
    print(f"Unique genres: {ar['unique_genres']}")
    print(f"Unique reviewers: {ar['unique_reviewers']}")
    print("Source distribution:")
    for s, c in sorted(ar["source_counts"].items(), key=lambda x: -x[1]):
        pct = round(c / ar["total_rows"] * 100, 2) if ar["total_rows"] else 0
        print(f" - {s}: {c} rows ({pct}%)")
    sc = ar["sentiment_counts"]
    total = ar["total_rows"]
    print("Sentiment (percent):")
    for k in ("positive", "negative", "neutral", "unknown"):
        v = sc.get(k, 0)
        print(f" - {k}: {v} ({round(v/total*100,2) if total else 0}%)")


def main():
    eng = analyze_file(EN_PATH)
    ar = analyze_file(AR_PATH)
    print_report(eng, ar)

    out = os.path.join(HERE, "final_datasets_summary.json")
    try:
        with open(out, "w", encoding="utf-8") as f:
            json.dump({"english": eng, "arabic": ar}, f, ensure_ascii=False, indent=2)
        print(f"\nSaved summary to: {out}")
    except Exception as e:
        print(f"Failed to save summary: {e}")


if __name__ == "__main__":
    main()
