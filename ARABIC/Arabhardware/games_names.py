import re
import json
import glob

games_dict = {}

# Find all downloaded HTML files
html_files = sorted(glob.glob("downloads/arabhardware_page_*.html"))

# Try BeautifulSoup for robust parsing, otherwise fall back to regex
try:
    from bs4 import BeautifulSoup

    _HAS_BS4 = True
except Exception:
    _HAS_BS4 = False

for filename in html_files:
    print(f"Processing {filename}...")

    with open(filename, "r", encoding="utf-8") as f:
        html = f.read()

    if _HAS_BS4:
        soup = BeautifulSoup(html, "html.parser")
        # Each review row contains both the OpenCritic game link and the external review link
        for review in soup.select(".review-row"):
            # Find local game link
            a_game = review.find("a", href=re.compile(r"^/game/\d+"))
            if not a_game:
                continue
            href = a_game.get("href", "")
            m = re.search(r"/game/(\d+)", href)
            if not m:
                continue
            game_id = int(m.group(1))
            game_name = a_game.get_text(strip=True)

            # Find external 'Read full review' link (arabhardware review URL)
            a_external = review.find(
                "a", href=re.compile(r"^https?://[^\s]*arabhardware.net/reviews/")
            )
            external_url = a_external.get("href") if a_external else None

            if game_id not in games_dict:
                games_dict[game_id] = {
                    "name": game_name,
                    "external_review": external_url,
                }
            else:
                # fill missing external url if we didn't have it before
                if not games_dict[game_id].get("external_review") and external_url:
                    games_dict[game_id]["external_review"] = external_url
    else:
        # Fallback: split into review-like blocks and extract with regex
        # Non-greedy match for a block that contains 'review-row' in class
        block_pattern = re.compile(
            r'<div[^>]+class="[^"]*review-row[^"]*"[^>]*>(.*?)</div>', re.DOTALL
        )
        for block_match in block_pattern.finditer(html):
            block = block_match.group(1)
            game_m = re.search(
                r'href="(/game/(\d+)/[^"]*)"[^>]*>(.*?)</a>', block, re.DOTALL
            )
            if not game_m:
                continue
            game_id = int(game_m.group(2))
            # remove any tags inside the captured inner HTML
            game_name = re.sub(r"<[^>]+>", "", game_m.group(3)).strip()

            ext_m = re.search(
                r'href="(https?://[^"]*arabhardware.net/reviews/[^"]*)"', block
            )
            external_url = ext_m.group(1) if ext_m else None

            if game_id not in games_dict:
                games_dict[game_id] = {
                    "name": game_name,
                    "external_review": external_url,
                }
            else:
                if not games_dict[game_id].get("external_review") and external_url:
                    games_dict[game_id]["external_review"] = external_url

print(f"\nFound {len(games_dict)} unique games with external links (where available):")
for game_id, info in sorted(games_dict.items()):
    print(f"  {game_id}: {info.get('name')} -> {info.get('external_review')}")

# Save to JSON file
with open("arabhardware_names.json", "w", encoding="utf-8") as f:
    json.dump(games_dict, f, indent=2, ensure_ascii=False)

print(f"\nSaved to arabhardware_names.json")
