import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import re

GAME_IDS = {
    # Shooting (15 games - more indie focused)
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
    
    # RPG (16 games - more indie)
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

def search_metacritic(game_name):
    """Search for game on Metacritic and return PC game URL"""
    search_name = game_name.lower().replace(' ', '-').replace("'", "")
    search_name = re.sub(r'[^a-z0-9-]', '', search_name)
    
    # New Metacritic URL format
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

def scrape_reviews_by_sentiment(game_url, sentiment, num_reviews):
    """Scrape positive or negative reviews from Metacritic"""
    reviews = []
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    # Use the correct URL format with platform parameter
    if '?' not in game_url:
        reviews_url = game_url + 'user-reviews/?platform=pc'
    else:
        reviews_url = game_url + '&platform=pc'
    
    try:
        response = requests.get(reviews_url, headers=headers, timeout=10)
        if response.status_code != 200:
            print(f"  Status code: {response.status_code}")
            return reviews
            
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find all review divs - using exact class name
        review_divs = soup.find_all('div', class_='c-siteReview')
        
        for review in review_divs:
            try:
                # Extract score - it's in a span inside c-siteReviewScore
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
                
                # Filter by sentiment: positive (7-10) or negative (0-4)
                if sentiment == "positive" and score < 7:
                    continue
                if sentiment == "negative" and score > 4:
                    continue
                
                # Extract review text - c-siteReview_quote > span
                quote_div = review.find('div', class_='c-siteReview_quote')
                if not quote_div:
                    continue
                
                text_span = quote_div.find('span')
                if not text_span:
                    continue
                
                review_text = text_span.get_text(strip=True)
                
                # Only reviews longer than 10 characters
                if len(review_text) <= 10:
                    continue
                
                # Extract author - c-siteReviewHeader_username
                author_link = review.find('a', class_='c-siteReviewHeader_username')
                author = author_link.get_text(strip=True) if author_link else 'Anonymous'
                
                # Extract date - c-siteReview_reviewDate
                date_div = review.find('div', class_='c-siteReview_reviewDate')
                date = date_div.get_text(strip=True) if date_div else 'N/A'
                
                reviews.append({
                    'review_text': review_text,
                    'user_score': score,
                    'voted_up': score >= 7,
                    'author': author,
                    'date': date
                })
                
                if len(reviews) >= num_reviews:
                    break
                    
            except Exception as e:
                continue
                
    except Exception as e:
        print(f"  Error scraping: {e}")
    
    return reviews

def main():
    TOTAL_REVIEWS = 2500
    num_games = len(GAME_IDS)
    reviews_per_game = TOTAL_REVIEWS // num_games
    positive_per_game = reviews_per_game // 2
    negative_per_game = reviews_per_game - positive_per_game
    
    print(f"Target: {TOTAL_REVIEWS} total reviews from {num_games} games")
    print(f"Per game: {positive_per_game} positive + {negative_per_game} negative\n")
    
    all_reviews = []
    
    for app_id, game_name in GAME_IDS.items():
        print(f"[{game_name}]")
        
        # Find game on Metacritic
        game_url = search_metacritic(game_name)
        
        if not game_url:
            print(f"  Not found on Metacritic")
            continue
        
        # Get positive reviews (score 7-10)
        pos_reviews = scrape_reviews_by_sentiment(game_url, "positive", positive_per_game)
        print(f"  + {len(pos_reviews)} positive")
        
        # Get negative reviews (score 0-4)
        neg_reviews = scrape_reviews_by_sentiment(game_url, "negative", negative_per_game)
        print(f"  - {len(neg_reviews)} negative")
        
        # Add game metadata
        for review in pos_reviews + neg_reviews:
            review["game_name"] = game_name
            review["app_id"] = app_id
        
        all_reviews.extend(pos_reviews + neg_reviews)
        print(f"  Total so far: {len(all_reviews)}\n")
        
        time.sleep(2)
    
    df = pd.DataFrame(all_reviews)
    
    columns = [
        "game_name", "app_id", "author", "review_text", "user_score", 
        "voted_up", "date"
    ]
    df = df[columns]
    
    pos_count = df["voted_up"].sum()
    neg_count = len(df) - pos_count
    print(f"\nFinal: {len(df)} reviews ({pos_count} positive, {neg_count} negative)")
    
    df.to_csv("metacritic_reviews.csv", index=False, encoding="utf-8")
    print(f"Saved to metacritic_reviews.csv")

if __name__ == "__main__":
    main()