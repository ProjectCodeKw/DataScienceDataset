import requests
import re
import time
import json
import os
import pandas as pd


def extract_summary_and_score(html):
    """Return (summary_text, score) where summary_text is the <p> under <h3>الخلاصة</h3>
    and score is an integer 0-10 found in the hexagon/score block. Returns (None, None)
    if not found."""
    summary = None
    score = None

    try:
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "html.parser")

        # Find h3 with text containing الخلاصة and then the following <p>
        h3 = None
        for tag in soup.find_all("h3"):
            if tag.get_text(strip=True).find("الخلاصة") != -1:
                h3 = tag
                break
        if h3:
            p = h3.find_next_sibling("p")
            if p:
                summary = p.get_text(separator=" ", strip=True)

        # Find numeric score inside hexagon/side-wrapper area
        # Look for numeric text nodes where an ancestor has a class with 'hexagon' or the 'review' wrapper
        if not score:
            texts = soup.find_all(string=re.compile(r"^\s*\d{1,2}\s*$"))
            for t in texts:
                try:
                    parent = t.parent
                    anc = parent
                    found = False
                    # walk up to 6 levels and check class names
                    for _ in range(6):
                        if not anc:
                            break
                        cls = " ".join(anc.get("class") or [])
                        if "hexagon" in cls or "side-wrapper" in cls or "review" in cls:
                            found = True
                            break
                        anc = anc.parent
                    if found:
                        val = int(t.strip())
                        if 0 <= val <= 10:
                            score = val
                            break
                except Exception:
                    continue

        # Fallback: regex search for pattern with hexagon-content
        if score is None:
            m = re.search(r"hexagon-content[\s\S]*?<div>\s*(\d{1,2})\s*</div>", html)
            if m:
                val = int(m.group(1))
                if 0 <= val <= 10:
                    score = val

    except Exception:
        # if bs4 unavailable or parsing error, fall back to regex-only extraction
        pass

    if (summary is None) or (score is None):
        # regex fallback for summary
        if summary is None:
            m = re.search(
                r"<h3[^>]*>\s*الخلاصة\s*</h3>\s*<p[^>]*>(.*?)</p>", html, re.DOTALL
            )
            if m:
                # strip tags
                txt = re.sub(r"<[^>]+>", "", m.group(1))
                summary = txt.strip()
        # regex fallback for score (search nearest hexagon number)
        if score is None:
            m2 = re.search(
                r'<div[^>]*class="[^"]*(?:hexagon|hexagon-content)[^"]*"[\s\S]*?<div>\s*(\d{1,2})\s*</div>',
                html,
                re.DOTALL,
            )
            if m2:
                val = int(m2.group(1))
                if 0 <= val <= 10:
                    score = val

    return summary, score


def scrape_ign(app_id, game_name, url):
    print(f"[{game_name}]")
    print(f"  URL: {url}")

    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code != 200:
            print(f"  ✗ Not found (status {resp.status_code})")
            return [
                {
                    "game_name": game_name,
                    "app_id": app_id,
                    "review_text": "N/A",
                    "user_score": "N/A",
                    "voted_up": "Neutral",
                    "source": "IGN",
                }
            ]

        html = resp.text
        summary, score = extract_summary_and_score(html)

        if not summary and score is None:
            print("  ✗ No review summary or score found")
            return [
                {
                    "game_name": game_name,
                    "app_id": app_id,
                    "review_text": "N/A",
                    "user_score": "N/A",
                    "voted_up": "Neutral",
                    "source": "IGN",
                }
            ]

        # Build single review per URL
        review_text = summary or ""
        user_score = score if score is not None else "N/A"

        print(f"  ✓ Found review (score: {user_score})")
        return [
            {
                "game_name": game_name,
                "app_id": app_id,
                "review_text": review_text,
                "user_score": user_score,
                "voted_up": "Neutral",
                "source": "IGN",
            }
        ]

    except Exception as e:
        print(f"  ✗ Error: {e}")
        return [
            {
                "game_name": game_name,
                "app_id": app_id,
                "review_text": "N/A",
                "user_score": "N/A",
                "voted_up": "Neutral",
                "source": "IGN",
            }
        ]


if __name__ == "__main__":
    all_reviews = []

    mapping_path = os.path.join(os.path.dirname(__file__), "ign_games.json")
    if not os.path.exists(mapping_path):
        print("Error: ign_games.json not found - run game_names.py to create it")
        raise SystemExit(1)

    try:
        with open(mapping_path, "r", encoding="utf-8") as mf:
            mapping = json.load(mf)
    except Exception as e:
        print(f"Error loading mapping: {e}")
        raise SystemExit(1)

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

        reviews = scrape_ign(app_id, game_name, external_url)
        all_reviews.extend(reviews)
        time.sleep(1)

    df = pd.DataFrame(all_reviews)
    df.to_csv("ign_reviews.csv", index=False, encoding="utf-8-sig")

    print(f"\n{'='*60}")
    print(f"Total reviews: {len(df)}")
    print(f"  Valid: {(df['review_text'] != 'N/A').sum()}")
    print(f"  N/A: {(df['review_text'] == 'N/A').sum()}")
    print(f"Saved to ign_reviews.csv")
