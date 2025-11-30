import pandas as pd
import requests
import re
import time
import json
import os


def scrape_arabhardware(app_id, game_name, url=None):
    # Require explicit external review URL from the JSON mapping.
    if not url:
        print(f"[{game_name}]")
        print("  ✗ Skipping — no external review URL provided in JSON")
        return []

    print(f"[{game_name}]")
    print(f"  URL: {url}")

    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        response = requests.get(url, headers=headers, timeout=40)

        if response.status_code != 200:
            print(f"  ✗ Not found (status {response.status_code})")
            return [
                {
                    "game_name": game_name,
                    "app_id": app_id,
                    "review_text": "N/A",
                    "user_score": "N/A",
                    "voted_up": "N/A",
                    "source": "ArabHardware",
                }
            ]

        html = response.text

        # Extract positives (المميزات)
        pos_match = re.search(r"المميزات</h4>(.*?)</ul>", html, re.DOTALL)
        positives = (
            re.findall(
                r'<li class="list-type list-type-green">.*?</svg>\s*</span>\s*(.*?)\s*</li>',
                pos_match.group(1),
                re.DOTALL,
            )
            if pos_match
            else []
        )

        # Extract negatives (العيوب)
        neg_match = re.search(r"العيوب</h4>(.*?)</ul>", html, re.DOTALL)
        negatives = (
            re.findall(
                r'<li class="list-type list-type-red">.*?</svg>\s*</span>\s*(.*?)\s*</li>',
                neg_match.group(1),
                re.DOTALL,
            )
            if neg_match
            else []
        )

        # Extract score
        score_match = re.search(r'<text.*?class="percentage">(\d+)</text>', html)
        score = int(score_match.group(1)) if score_match else 5

        reviews = []

        if positives:
            reviews.append(
                {
                    "game_name": game_name,
                    "app_id": app_id,
                    "review_text": " ".join([p.strip() for p in positives]),
                    "user_score": score,
                    "voted_up": score >= 6,
                    "source": "ArabHardware",
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
                    "source": "ArabHardware",
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
                    "source": "ArabHardware",
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
                "source": "ArabHardware",
            }
        ]


all_reviews = []

# Load mapping produced by the game-names script
mapping_path = os.path.join(os.path.dirname(__file__), "arabhardware_names.json")
mapping = {}
if os.path.exists(mapping_path):
    try:
        with open(mapping_path, "r", encoding="utf-8") as mf:
            mapping = json.load(mf)
    except Exception as e:
        print(f"Warning: failed to load mapping file: {e}")

if mapping:
    for game_key, v in mapping.items():
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

        reviews = scrape_arabhardware(app_id, game_name, url=external_url)
        all_reviews.extend(reviews)
        time.sleep(1)
else:
    print(
        "Error: no `arabhardware_games.json` mapping found. Run the game-names script first."
    )
    raise SystemExit(1)

df = pd.DataFrame(all_reviews)
df.to_csv("arabhardware_reviews.csv", index=False, encoding="utf-8-sig")

print(f"\n{'='*60}")
print(f"Total reviews: {len(df)}")
print(f"  Valid: {(df['review_text'] != 'N/A').sum()}")
print(f"  N/A: {(df['review_text'] == 'N/A').sum()}")
print(f"Saved to arabhardware_reviews.csv")
