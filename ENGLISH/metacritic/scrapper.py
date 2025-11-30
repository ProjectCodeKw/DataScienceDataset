#!/usr/bin/env python3
"""
Metacritic Scraper - Duplicate-Safe Version
Gets 2,500 TOTAL reviews (loads existing 1,250 + scrapes 1,250 more)
Checks for duplicates BEFORE adding reviews
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import re
import os

GAME_IDS = {
    # Shooting (15 games)
    412020: "Insurgency Sandstorm",
    1966720: "Lethal Company",
    553850: "HELLDIVERS 2",
    632360: "Risk of Rain 2",
    394690: "Tower Unite",
    418370: "Rising Storm 2 Vietnam",
    736220: "Post Void",
    1250410: "Turbo Overkill",
    1100600: "Dusk",
    2198510: "Anger Foot",
    1229490: "Ultrakill",
    1200570: "Prodeus",
    1315690: "Nightmare Reaper",
    322500: "SUPERHOT",
    1284410: "Roboquest",
    
    # RPG (16 games)
    1903340: "Clair Obscur Expedition 33",
    1086940: "Baldur's Gate 3",
    1145360: "Hades",
    251570: "7 Days to Die",
    632470: "Disco Elysium",
    1449850: "Sea of Stars",
    1113560: "Ni no Kuni",
    1158310: "Crusader Kings III",
    230230: "Divinity Original Sin",
    435150: "Divinity Original Sin 2",
    1151640: "Horizon Zero Dawn",
    1817070: "Hogwarts Legacy",
    1057090: "Octopath Traveler",
    1096530: "Octopath Traveler II",
    774361: "TROUBLESHOOTER",
    1121910: "Yakuza Like a Dragon",
    
    # Story-based (10 games)
    524220: "Nier Automata",
    1113560: "Spiritfarer",
    1332010: "Stray",
    620: "Portal 2",
    219740: "Dont Starve",
    367520: "Hollow Knight",
    1150690: "Omori",
    813780: "Before Your Eyes",
    1677740: "Neon White",
    1091500: "Cyberpunk 2077",
    
    # Choice-based (10 games)
    319630: "Life is Strange",
    1222690: "Life is Strange True Colors",
    532210: "Life is Strange 2",
    1328670: "Mass Effect Legendary",
    207610: "The Walking Dead",
    261030: "The Wolf Among Us",
    282140: "Oxenfree",
    1274570: "Oxenfree II",
    1044720: "A Short Hike",
    1051510: "Little Misfortune",
    
    # Card games (10 games)
    2379780: "Balatro",
    1092790: "Inscryption",
    646570: "Slay the Spire",
    1182480: "Yu-Gi-Oh Master Duel",
    286160: "Tabletop Simulator",
    1942280: "Griftlands",
    1284410: "Legends of Runeterra",
    1296610: "Gordian Quest",
    1102190: "Vault of the Void",
    1494830: "Stacklands",
    
    # 2D Platformers (10 games)
    504230: "Celeste",
    268910: "Cuphead",
    774361: "Gris",
    420530: "OneShot",
    1070560: "Pizza Tower",
    1276390: "The Messenger",
    1061090: "Demon Turf",
    1089350: "Shovel Knight Dig",
    774781: "Just Shapes and Beats",
    1116740: "Unrailed",
    
    # Roguelike (15 games)
    588650: "Dead Cells",
    976730: "Hades",
    250900: "Binding of Isaac Rebirth",
    263340: "FTL Faster Than Light",
    242680: "Nuclear Throne",
    311690: "Enter the Gungeon",
    1145350: "Rogue Legacy 2",
    1548850: "Monster Train",
    1794680: "Vampire Survivors",
    1966900: "Brotato",
    1490720: "Noita",
    1240440: "Hades II",
    462770: "Crypt of the NecroDancer",
    1205140: "Cult of the Lamb",
    1637730: "Peglin",
    
    # Souls-like (10 games)
    1245620: "Elden Ring",
    374320: "Dark Souls III",
    1888160: "Lies of P",
    814380: "Sekiro",
    1203620: "Mortal Shell",
    937010: "Salt and Sanctuary",
    2053170: "Lords of the Fallen 2023",
    1245430: "Thymesia",
    1283400: "Dolmen",
    1151340: "Steelrising",
    
    # Free-to-play multiplayer (15 games)
    945360: "Among Us",
    1172620: "Sea of Thieves",
    252490: "Rust",
    438100: "VRChat",
    555160: "Phasmophobia",
    394510: "Tower Defense Simulator",
    2357570: "Marvel Rivals",
    1599340: "Lost Ark",
    570: "Dota 2",
    230410: "Warframe",
    1172470: "Apex Legends",
    238960: "Path of Exile",
    444090: "Paladins",
    1091500: "Smite 2",
    1085660: "Destiny 2"
}

# ============================================================
# DUPLICATE TRACKING
# ============================================================
existing_reviews = set()  # Stores (author, game_name) tuples
existing_df = None

def load_existing_reviews():
    """Load existing metacritic_reviews.csv to avoid duplicates"""
    global existing_reviews, existing_df
    
    if os.path.exists('metacritic_reviews.csv'):
        try:
            existing_df = pd.read_csv('metacritic_reviews.csv')
            print(f"✓ Loaded {len(existing_df):,} existing reviews")
            
            # Build duplicate detection set
            for _, row in existing_df.iterrows():
                author = str(row.get('author', 'Anonymous'))
                game = str(row.get('game_name', ''))
                existing_reviews.add((author, game))
            
            print(f"✓ Tracking {len(existing_reviews):,} (author, game) pairs")
            return len(existing_df)
        except Exception as e:
            print(f"⚠️  Error loading existing file: {e}")
            return 0
    else:
        print("ℹ️  No existing file found, starting fresh")
        return 0

def is_duplicate(author, game_name):
    """Check if (author, game) already exists"""
    return (author, game_name) in existing_reviews

def search_metacritic(game_name):
    """Search for game on Metacritic and return PC game URL"""
    search_name = game_name.lower().replace(' ', '-').replace("'", "")
    search_name = re.sub(r'[^a-z0-9-]', '', search_name)
    
    url = f"https://www.metacritic.com/game/{search_name}/"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            return url
    except:
        pass
    
    return None

def scrape_reviews_by_sentiment(game_url, game_name, sentiment, num_reviews):
    """Scrape reviews, skipping duplicates"""
    reviews = []
    duplicates_skipped = 0
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    if '?' not in game_url:
        reviews_url = game_url + 'user-reviews/?platform=pc'
    else:
        reviews_url = game_url + '&platform=pc'
    
    try:
        response = requests.get(reviews_url, headers=headers, timeout=10)
        if response.status_code != 200:
            return reviews, duplicates_skipped
            
        soup = BeautifulSoup(response.content, 'html.parser')
        review_divs = soup.find_all('div', class_='c-siteReview')
        
        for review in review_divs:
            try:
                # Extract score
                score_div = review.find('div', class_='c-siteReviewScore')
                if not score_div:
                    continue
                
                score_span = score_div.find('span')
                if not score_span:
                    continue
                
                score_text = score_span.get_text(strip=True)
                try:
                    score = int(score_text)
                except:
                    continue
                
                # Filter by sentiment
                if sentiment == "positive" and score < 7:
                    continue
                if sentiment == "negative" and score > 4:
                    continue
                
                # Extract review text
                quote_div = review.find('div', class_='c-siteReview_quote')
                if not quote_div:
                    continue
                
                text_span = quote_div.find('span')
                if not text_span:
                    continue
                
                review_text = text_span.get_text(strip=True)
                
                if len(review_text) <= 10:
                    continue
                
                # Extract author
                author_link = review.find('a', class_='c-siteReviewHeader_username')
                author = author_link.get_text(strip=True) if author_link else 'Anonymous'
                
                # ⭐ CHECK FOR DUPLICATE ⭐
                if is_duplicate(author, game_name):
                    duplicates_skipped += 1
                    continue  # Skip this review
                
                # Extract date
                date_div = review.find('div', class_='c-siteReview_reviewDate')
                date = date_div.get_text(strip=True) if date_div else 'N/A'
                
                reviews.append({
                    'review_text': review_text,
                    'user_score': score,
                    'voted_up': score >= 7,
                    'author': author,
                    'date': date
                })
                
                # Add to tracking set
                existing_reviews.add((author, game_name))
                
                if len(reviews) >= num_reviews:
                    break
                    
            except Exception:
                continue
                
    except Exception as e:
        print(f"  Error: {e}")
    
    return reviews, duplicates_skipped

def main():
    print("="*70)
    print("METACRITIC SCRAPER - DUPLICATE-SAFE")
    print("="*70)
    
    # Load existing reviews
    existing_count = load_existing_reviews()
    
    TARGET_TOTAL = 2500
    TARGET_NEW = TARGET_TOTAL - existing_count
    
    print(f"\nTarget:")
    print(f"  Existing:  {existing_count:,}")
    print(f"  Goal:      {TARGET_TOTAL:,}")
    print(f"  Need:      {TARGET_NEW:,} NEW reviews")
    print("="*70)
    print()
    
    if TARGET_NEW <= 0:
        print("✓ Already have enough reviews!")
        return
    
    num_games = len(GAME_IDS)
    reviews_per_game = TARGET_NEW // num_games
    positive_per_game = reviews_per_game // 2
    negative_per_game = reviews_per_game - positive_per_game
    
    print(f"Strategy: ~{reviews_per_game} per game ({positive_per_game} pos + {negative_per_game} neg)\n")
    
    all_new_reviews = []
    total_duplicates = 0
    
    for i, (app_id, game_name) in enumerate(GAME_IDS.items(), 1):
        print(f"[{i}/{num_games}] {game_name}")
        
        game_url = search_metacritic(game_name)
        
        if not game_url:
            print(f"  ✗ Not found")
            continue
        
        # Scrape positive
        pos_reviews, pos_dups = scrape_reviews_by_sentiment(
            game_url, game_name, "positive", positive_per_game
        )
        print(f"  + {len(pos_reviews)} positive (skipped {pos_dups} dups)")
        
        # Scrape negative
        neg_reviews, neg_dups = scrape_reviews_by_sentiment(
            game_url, game_name, "negative", negative_per_game
        )
        print(f"  - {len(neg_reviews)} negative (skipped {neg_dups} dups)")
        
        total_duplicates += (pos_dups + neg_dups)
        
        # Add metadata
        for review in pos_reviews + neg_reviews:
            review["game_name"] = game_name
            review["app_id"] = app_id
        
        all_new_reviews.extend(pos_reviews + neg_reviews)
        print(f"  Total NEW: {len(all_new_reviews):,} | Dups skipped: {total_duplicates:,}\n")
        
        time.sleep(2)
        
        if len(all_new_reviews) >= TARGET_NEW:
            print(f"✓ Reached target!")
            break
    
    # Combine with existing
    print("="*70)
    print("FINALIZING")
    print("="*70)
    
    if len(all_new_reviews) > 0:
        df_new = pd.DataFrame(all_new_reviews)
        
        columns = [
            "game_name", "app_id", "author", "review_text", "user_score", 
            "voted_up", "date"
        ]
        df_new = df_new[columns]
        
        # Combine
        if existing_df is not None:
            df_final = pd.concat([existing_df, df_new], ignore_index=True)
        else:
            df_final = df_new
        
        print(f"\nNew reviews scraped: {len(df_new):,}")
        print(f"Combined total:      {len(df_final):,}")
        print(f"Duplicates skipped:  {total_duplicates:,}")
        
        # Stats
        pos_count = df_final["voted_up"].sum()
        neg_count = len(df_final) - pos_count
        
        print(f"\nFinal balance:")
        print(f"  Positive: {pos_count:,} ({pos_count/len(df_final)*100:.1f}%)")
        print(f"  Negative: {neg_count:,} ({neg_count/len(df_final)*100:.1f}%)")
        
        # Save
        output_file = "metacritic_reviews_2500.csv"
        df_final.to_csv(output_file, index=False, encoding="utf-8")
        print(f"\n✓ Saved to {output_file}")
        
    else:
        print("\n✗ No new reviews collected (all were duplicates)")
    
    print("="*70)

if __name__ == "__main__":
    main()