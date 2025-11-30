import pandas as pd
import requests
import re
import time
import json
import os
import sys


# The scraper now strictly relies on external review URLs from
# `saudi_gamer_games.json`. We do not attempt to guess slugs anymore.


def scrape_game(app_id, game_name, url=None):
    # Require explicit external review URL from the JSON mapping.
    if not url:
        print(f"[{game_name}]")
        print("  ✗ Skipping — no external review URL provided in JSON")
        return []

    print(f"[{game_name}]")
    print(f"  URL: {url}")

    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        response = requests.get(url, headers=headers, timeout=10)

        if response.status_code != 200:
            print(f"  ✗ Not found (status {response.status_code})")
            return [
                {
                    "game_name": game_name,
                    "app_id": app_id,
                    "review_text": "N/A",
                    "user_score": "N/A",
                    "voted_up": "N/A",
                    "source": "SaudiGamer",
                }
            ]

        html = response.text

        # Extract positives
        pos_match = re.search(
            r"review_like.*?الايجابيات.*?</h3>(.*?)</div></div>", html, re.DOTALL
        )
        positives = (
            re.findall(r"<li>(.*?)</li>", pos_match.group(1), re.DOTALL)
            if pos_match
            else []
        )

        # Extract negatives
        neg_match = re.search(
            r"review_dislike.*?السلبيات.*?</h3>(.*?)</div></div>", html, re.DOTALL
        )
        negatives = (
            re.findall(r"<li>(.*?)</li>", neg_match.group(1), re.DOTALL)
            if neg_match
            else []
        )

        # Extract score
        score_match = re.search(r'<li\s+class="active"\s+>.*?rate-(\d)', html)
        rating_out_of_5 = int(score_match.group(1)) if score_match else 3
        score = rating_out_of_5 * 2

        reviews = []

        if positives:
            reviews.append(
                {
                    "game_name": game_name,
                    "app_id": app_id,
                    "review_text": " ".join([p.strip() for p in positives]),
                    "user_score": score,
                    "voted_up": score >= 6,
                    "source": "SaudiGamer",
                }
            )

        if negatives:
            reviews.append(
                {
                    "game_name": game_name,
                    "app_id": app_id,
                    "review_text": " ".join([n.strip() for n in negatives]),
                    "user_score": 4,
                    "voted_up": False,
                    "source": "SaudiGamer",
                }
            )

        if not reviews:
            print(f"  ✗ No review content found")
            return [
                {
                    "game_name": game_name,
                    "app_id": app_id,
                    "review_text": "N/A",
                    "user_score": "N/A",
                    "voted_up": "N/A",
                    "source": "SaudiGamer",
                }
            ]

        print(f"  ✓ Found {len(reviews)} reviews (score: {score}/10)")
        return reviews

    except Exception as e:
        print(f"  ✗ Error: {e}")
        return [
            {
                "game_name": game_name,
                "app_id": app_id,
                "review_text": "N/A",
                "user_score": "N/A",
                "voted_up": "N/A",
                "source": "SaudiGamer",
            }
        ]


all_reviews = []

# Try to load mapping produced by game_names.py (saudi_gamer_games.json)
mapping_path = os.path.join(os.path.dirname(__file__), "saudi_gamer_games.json")
mapping = {}
if os.path.exists(mapping_path):
    try:
        with open(mapping_path, "r", encoding="utf-8") as mf:
            mapping = json.load(mf)
    except Exception as e:
        print(f"Warning: failed to load mapping file: {e}")
if mapping:
    for game_key, v in mapping.items():
        # game_key is the OpenCritic game id (JSON keys are strings)
        try:
            app_id = int(game_key)
        except Exception:
            app_id = game_key

        if isinstance(v, dict):
            game_name = v.get("name") or str(app_id)
            external_url = v.get("external_review")
        else:
            game_name = str(v)
            external_url = None

        if not external_url:
            print(f"Skipping {game_name} ({game_key}) — no external URL in JSON")
            continue

        reviews = scrape_game(app_id, game_name, url=external_url)
        all_reviews.extend(reviews)
        time.sleep(1)
else:
    print(
        "Error: no `saudi_gamer_games.json` mapping found. Run `game_names.py` first."
    )
    sys.exit(1)

df = pd.DataFrame(all_reviews)
df.to_csv("saudigamer_reviews.csv", index=False, encoding="utf-8-sig")

print(f"\n{'='*60}")
print(f"Total reviews: {len(df)}")
print(f"  Valid: {(df['review_text'] != 'N/A').sum()}")
print(f"  N/A: {(df['review_text'] == 'N/A').sum()}")
print(f"Saved to saudigamer_reviews.csv")
