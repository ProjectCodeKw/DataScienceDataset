import requests
import pandas as pd
import time
from typing import List, Dict, Optional

STEAM_REVIEW_API = "https://store.steampowered.com/appreviews/{app_id}"
STEAM_STORE_API = "https://store.steampowered.com/api/appdetails"

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

def fetch_game_details(app_id: int) -> Dict:
    """Fetch price, age rating, and game mode from Steam Store API"""
    try:
        response = requests.get(
            STEAM_STORE_API,
            params={"appids": app_id, "cc": "us", "l": "en"},
            timeout=10
        )
        
        if response.status_code != 200:
            return {}
        
        data = response.json()
        game_data = data.get(str(app_id), {}).get("data", {})
        
        if not game_data:
            return {}
        
        # Price
        price_data = game_data.get("price_overview", {})
        if game_data.get("is_free"):
            price_usd = 0.0
        else:
            price_usd = price_data.get("final", 0) / 100
        
        # Age rating
        age_rating = game_data.get("required_age", 0)
        
        # Game mode from categories
        categories = game_data.get("categories", [])
        category_names = [c.get("description", "").lower() for c in categories]
        
        game_modes = []
        if any("single" in c for c in category_names):
            game_modes.append("solo")
        if any("multi" in c for c in category_names):
            game_modes.append("multiplayer")
        if any("co-op" in c or "coop" in c for c in category_names):
            game_modes.append("co-op")
        
        game_mode = "/".join(game_modes) if game_modes else "solo"
        
        # Genres
        genres = game_data.get("genres", [])
        genre_names = ", ".join([g.get("description", "") for g in genres])
        
        return {
            "price_usd": price_usd,
            "age_rating": age_rating,
            "game_mode": game_mode,
            "genres": genre_names
        }
        
    except Exception as e:
        print(f"Error fetching details for app {app_id}: {e}")
        return {}

def fetch_reviews_by_type(app_id: int, review_type: str, num_reviews: int) -> List[Dict]:
    """Fetch positive or negative English reviews longer than 10 characters"""
    reviews = []
    cursor = "*"
    
    params = {
        "json": 1,
        "language": "english",
        "filter": "recent",
        "review_type": review_type,  # "positive" or "negative"
        "num_per_page": 100,
        "purchase_type": "all"
    }
    
    while len(reviews) < num_reviews:
        params["cursor"] = cursor
        
        try:
            response = requests.get(
                STEAM_REVIEW_API.format(app_id=app_id),
                params=params,
                timeout=10
            )
            
            if response.status_code != 200:
                break
                
            data = response.json()
            
            if not data.get("success"):
                break
            
            batch_reviews = data.get("reviews", [])
            
            if not batch_reviews:
                break
            
            for review in batch_reviews:
                review_text = review.get("review", "")
                
                if len(review_text) > 10:
                    reviews.append({
                        "app_id": app_id,
                        "user_id": review["author"]["steamid"],
                        "review_text": review_text,
                        "voted_up": review["voted_up"],
                        "votes_helpful": review["votes_up"],
                        "votes_funny": review["votes_funny"],
                        "created": review["timestamp_created"]
                    })
                    
                    if len(reviews) >= num_reviews:
                        break
            
            cursor = data.get("cursor")
            if not cursor:
                break
                
            time.sleep(0.5)
            
        except Exception as e:
            print(f"Error: {e}")
            break
    
    return reviews

def main():
    TOTAL_REVIEWS = 2500
    num_games = len(GAME_IDS)
    reviews_per_game = TOTAL_REVIEWS // num_games  # ~25 per game
    positive_per_game = reviews_per_game // 2
    negative_per_game = reviews_per_game - positive_per_game
    
    print(f"Target: {TOTAL_REVIEWS} total reviews from {num_games} games")
    print(f"Per game: {positive_per_game} positive + {negative_per_game} negative\n")
    
    all_reviews = []
    
    for app_id, game_name in GAME_IDS.items():
        print(f"[{game_name}]")
        
        # Get game metadata
        details = fetch_game_details(app_id)
        time.sleep(0.3)
        
        # Get positive reviews
        pos_reviews = fetch_reviews_by_type(app_id, "positive", positive_per_game)
        print(f"  + {len(pos_reviews)} positive")
        
        # Get negative reviews
        neg_reviews = fetch_reviews_by_type(app_id, "negative", negative_per_game)
        print(f"  - {len(neg_reviews)} negative")
        
        # Combine and add metadata
        for review in pos_reviews + neg_reviews:
            review["game_name"] = game_name
            review["price_usd"] = details.get("price_usd", None)
            review["age_rating"] = details.get("age_rating", None)
            review["game_mode"] = details.get("game_mode", None)
            review["genres"] = details.get("genres", None)
        
        all_reviews.extend(pos_reviews + neg_reviews)
        print(f"  Total so far: {len(all_reviews)}")
        
        time.sleep(1)
    
    df = pd.DataFrame(all_reviews)
    
    columns = [
        "game_name", "app_id", "price_usd", "age_rating", "game_mode", "genres",
        "user_id", "review_text", "voted_up", "votes_helpful", "votes_funny", "created"
    ]
    df = df[columns]
    
    pos_count = df["voted_up"].sum()
    neg_count = len(df) - pos_count
    print(f"\nFinal: {len(df)} reviews ({pos_count} positive, {neg_count} negative)")
    
    df.to_csv("steam_reviews.csv", index=False, encoding="utf-8")
    print(f"Saved to steam_reviews.csv")

if __name__ == "__main__":
    main()