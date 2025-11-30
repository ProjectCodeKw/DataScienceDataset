#!/usr/bin/env python3
"""Enrich combined ARABIC reviews with genres from RAWG.io"""

import argparse
import json
import os
import time
import requests
import pandas as pd
from collections import Counter

RAWG_SEARCH_URL = "https://api.rawg.io/api/games"


def lookup_genres_rawg(title, api_key=None, session=None, max_retries=3):
    """Query RAWG search API for title and return comma-separated genres or None."""
    params = {"search": title, "page_size": 1}
    if api_key:
        params["key"] = api_key

    if session is None:
        session = requests

    for attempt in range(max_retries):
        try:
            print(f"    Querying RAWG API (attempt {attempt + 1}/{max_retries})...")
            r = session.get(RAWG_SEARCH_URL, params=params, timeout=10)
            
            if r.status_code == 429:
                print(f"    ⚠ Rate limited. Waiting 5 seconds...")
                time.sleep(5)
                continue
            
            if r.status_code != 200:
                print(f"    ✗ API returned status {r.status_code}")
                return None
                
            data = r.json()
            results = data.get("results") or []
            
            if not results:
                print(f"    ✗ No results found for '{title}'")
                return None
                
            first = results[0]
            matched_name = first.get("name", "Unknown")
            print(f"    ✓ Matched to: {matched_name}")
            
            genres = [g.get("name") for g in first.get("genres", []) if g.get("name")]
            if genres:
                genre_str = ", ".join(genres)
                print(f"    ✓ Found genres: {genre_str}")
                return genre_str
            
            # Fallback to tags
            tags = [t.get("name") for t in first.get("tags", [])[:3] if t.get("name")]
            if tags:
                tag_str = ", ".join(tags)
                print(f"    ⚠ No genres, using tags: {tag_str}")
                return tag_str
                
            print(f"    ✗ No genres or tags found")
            return None
            
        except requests.Timeout:
            print(f"    ⚠ Request timeout (attempt {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(2)
        except Exception as e:
            print(f"    ✗ Error: {e}")
            return None
    
    print(f"    ✗ Failed after {max_retries} attempts")
    return None


def main(input_csv, output_csv, cache_path=None, api_key=None, delay=1.0):
    print("="*70)
    print("RAWG GENRE ENRICHMENT SCRIPT")
    print("="*70)
    
    # Validate input file
    if not os.path.exists(input_csv):
        raise SystemExit(f"Error: Input file not found: {input_csv}")
    
    # Load combined csv
    print(f"\n[1/5] Loading data from {input_csv}...")
    df = pd.read_csv(input_csv, encoding="utf-8-sig")
    print(f"  ✓ Loaded {len(df):,} reviews")
    
    if "game_name" not in df.columns:
        raise SystemExit("Error: Input CSV must contain a 'game_name' column")
    
    # Show current columns
    print(f"  Columns: {', '.join(df.columns.tolist())}")
    
    # Load or create cache
    print(f"\n[2/5] Loading cache...")
    cache = {}
    if cache_path and os.path.exists(cache_path):
        try:
            with open(cache_path, "r", encoding="utf-8") as cf:
                cache = json.load(cf)
            print(f"  ✓ Loaded {len(cache)} cached titles from {cache_path}")
        except Exception as e:
            print(f"  ⚠ Failed to read cache: {e}")
            print(f"  Starting with empty cache")
            cache = {}
    else:
        print(f"  No existing cache found, starting fresh")
    
    # Build list of unique titles
    print(f"\n[3/5] Identifying unique game titles...")
    unique_titles = df["game_name"].dropna().astype(str).str.strip().unique()
    print(f"  ✓ Found {len(unique_titles)} unique game titles")
    
    # Count how many need lookup
    need_lookup = [t for t in unique_titles if t not in cache]
    print(f"  Already cached: {len(unique_titles) - len(need_lookup)}")
    print(f"  Need to lookup: {len(need_lookup)}")
    
    if not need_lookup:
        print(f"\n  All titles already cached! Skipping API calls.")
    else:
        print(f"\n[4/5] Looking up genres from RAWG...")
        if api_key:
            print(f"  Using API key: {api_key[:8]}...")
        else:
            print(f"  ⚠ No API key provided (may hit rate limits)")
        
        session = requests.Session()
        headers = {"User-Agent": "Mozilla/5.0 (GenreLookup/1.0)"}
        session.headers.update(headers)
        
        looked_up = 0
        found_genres = 0
        
        for idx, title in enumerate(need_lookup, 1):
            print(f"\n  [{idx}/{len(need_lookup)}] Processing: {title}")
            
            if title in cache:
                print(f"    ✓ Using cached result")
                continue
            
            genres = lookup_genres_rawg(title, api_key=api_key, session=session)
            cache[title] = genres if genres is not None else "N/A"
            
            if genres and genres != "N/A":
                found_genres += 1
            
            looked_up += 1
            
            # Progress indicator
            progress = (idx / len(need_lookup)) * 100
            print(f"  Progress: {progress:.1f}% ({found_genres} genres found)")
            
            # Be polite
            if idx < len(need_lookup):
                time.sleep(delay)
        
        print(f"\n  ✓ Lookup complete!")
        print(f"    Total looked up: {looked_up}")
        print(f"    Genres found: {found_genres}")
        print(f"    Not found (N/A): {looked_up - found_genres}")
    
    # Map genres back onto dataframe
    print(f"\n[5/5] Mapping genres to reviews...")
    df["genres"] = (
        df["game_name"].astype(str).str.strip().map(lambda t: cache.get(t, "N/A"))
    )
    
    # Statistics
    total_with_genres = (df["genres"] != "N/A").sum()
    total_without = (df["genres"] == "N/A").sum()
    
    print(f"  ✓ Genres mapped to {len(df):,} reviews")
    print(f"    With genres: {total_with_genres:,} ({total_with_genres/len(df)*100:.1f}%)")
    print(f"    Without (N/A): {total_without:,} ({total_without/len(df)*100:.1f}%)")
    
    # Show most common genres
    if total_with_genres > 0:
        all_genres = []
        for g in df[df["genres"] != "N/A"]["genres"]:
            all_genres.extend([x.strip() for x in str(g).split(",")])
        genre_counts = Counter(all_genres)
        
        print(f"\n  Top 10 genres:")
        for genre, count in genre_counts.most_common(10):
            print(f"    - {genre}: {count:,} reviews")
    
    # Save output
    print(f"\nSaving enriched CSV to {output_csv}...")
    df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"  ✓ Saved {len(df):,} reviews")
    
    # Save cache
    if cache_path:
        try:
            print(f"\nSaving cache to {cache_path}...")
            with open(cache_path, "w", encoding="utf-8") as cf:
                json.dump(cache, cf, ensure_ascii=False, indent=2)
            print(f"  ✓ Cache updated ({len(cache)} titles)")
        except Exception as e:
            print(f"  ⚠ Failed to save cache: {e}")
    
    print("\n" + "="*70)
    print("COMPLETE!")
    print("="*70)
    print(f"Input:  {input_csv}")
    print(f"Output: {output_csv}")
    print(f"Reviews: {len(df):,}")
    print(f"Genres coverage: {total_with_genres/len(df)*100:.1f}%")
    print("="*70)


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_input = os.path.join(script_dir, "normalized_reviews.csv")
    default_output = os.path.join(script_dir, "final_english_dataset.csv")
    default_cache = os.path.join(script_dir, "rawg_genres_cache.json")

    parser = argparse.ArgumentParser(
        description="Enrich combined reviews with RAWG genres"
    )
    parser.add_argument(
        "--input", "-i", default=default_input,
        help=f"Path to combined reviews CSV (default: {default_input})"
    )
    parser.add_argument(
        "--output", "-o", default=default_output,
        help=f"Path to output enriched CSV (default: {default_output})"
    )
    parser.add_argument(
        "--cache", "-c", default=default_cache,
        help=f"Path to JSON cache file (default: {default_cache})"
    )
    parser.add_argument(
        "--api-key", "-k", default="ca27bfe2d754419fba3eba074bbb5f3f",
        help="RAWG API key (optional)"
    )
    parser.add_argument(
        "--delay", "-d", type=float, default=1.0,
        help="Delay between API requests (seconds)"
    )
    args = parser.parse_args()

    main(
        args.input, args.output,
        cache_path=args.cache,
        api_key=args.api_key,
        delay=args.delay
    )