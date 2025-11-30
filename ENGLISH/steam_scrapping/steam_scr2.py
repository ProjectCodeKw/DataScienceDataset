import requests
import pandas as pd
import time
from typing import List, Dict, Optional

STEAM_REVIEW_API = "https://store.steampowered.com/appreviews/{app_id}"
STEAM_STORE_API = "https://store.steampowered.com/api/appdetails"

# Focus on games likely to have LOTS of negative reviews
GAME_IDS = {
    # Controversial/Mixed reception games (more negative reviews)
    1091500: "Cyberpunk 2077",
    1203620: "Mortal Shell",
    1283400: "Dolmen",
    1151340: "Steelrising",
    2053170: "Lords of the Fallen 2023",
    1245430: "Thymesia",
    1817070: "Hogwarts Legacy",
    251570: "7 Days to Die",
    1085660: "Destiny 2",
    1599340: "Lost Ark",
    252490: "Rust",
    
    # Popular games (high volume = more negative reviews available)
    1245620: "Elden Ring",
    1086940: "Baldur's Gate 3",
    1332010: "Stray",
    1145360: "Hades",
    632360: "Risk of Rain 2",
    588650: "Dead Cells",
    
    # Add more from your original list
    412020: "Insurgency Sandstorm",
    1966720: "Lethal Company",
    553850: "HELLDIVERS 2",
    394690: "Tower Unite",
    418370: "Rising Storm 2 Vietnam",
    1903340: "Clair Obscur Expedition 33",
    632470: "Disco Elysium",
    1449850: "Sea of Stars",
    1113560: "Ni no Kuni",
    1158310: "Crusader Kings III",
    230230: "Divinity Original Sin",
    435150: "Divinity Original Sin 2",
    1151640: "Horizon Zero Dawn",
    1057090: "Octopath Traveler",
    1096530: "Octopath Traveler II",
    774361: "TROUBLESHOOTER",
    1121910: "Yakuza Like a Dragon",
    524220: "Nier Automata",
    1113560: "Spiritfarer",
    620: "Portal 2",
    219740: "Dont Starve",
    367520: "Hollow Knight",
    1150690: "Omori",
    813780: "Before Your Eyes",
    1677740: "Neon White",
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

def fetch_reviews_by_type(app_id: int, review_type: str, num_reviews: int, max_pages: int = 50) -> List[Dict]:
    """
    Fetch positive or negative English reviews longer than 10 characters
    
    Args:
        app_id: Steam app ID
        review_type: "positive" or "negative"
        num_reviews: Target number of reviews to fetch
        max_pages: Maximum number of pages to fetch (safety limit)
    """
    reviews = []
    cursor = "*"
    pages_fetched = 0
    
    params = {
        "json": 1,
        "language": "english",
        "filter": "recent",
        "review_type": review_type,
        "num_per_page": 100,  # Max allowed by Steam
        "purchase_type": "all"
    }
    
    print(f"    Fetching {review_type} reviews...", end="", flush=True)
    
    while len(reviews) < num_reviews and pages_fetched < max_pages:
        params["cursor"] = cursor
        
        try:
            response = requests.get(
                STEAM_REVIEW_API.format(app_id=app_id),
                params=params,
                timeout=10
            )
            
            if response.status_code != 200:
                print(f" [HTTP {response.status_code}]", end="")
                break
                
            data = response.json()
            
            if not data.get("success"):
                print(" [API failed]", end="")
                break
            
            batch_reviews = data.get("reviews", [])
            
            if not batch_reviews:
                print(" [no more reviews]", end="")
                break
            
            # Filter and add reviews
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
            
            pages_fetched += 1
            print(".", end="", flush=True)
            
            # Get next cursor
            cursor = data.get("cursor")
            if not cursor or cursor == "*":
                print(" [end]", end="")
                break
                
            time.sleep(0.5)  # Rate limiting
            
        except Exception as e:
            print(f" [Error: {e}]", end="")
            break
    
    print(f" Got {len(reviews)}")
    return reviews

def main():
    # NEW STRATEGY: Get way more negative reviews
    # Target: 1000 additional negative reviews
    TARGET_NEGATIVE = 1000
    TARGET_POSITIVE = 500  # Keep some balance
    
    num_games = len(GAME_IDS)
    negative_per_game = TARGET_NEGATIVE // num_games
    positive_per_game = TARGET_POSITIVE // num_games
    
    print(f"=" * 60)
    print(f"STEAM REVIEW SCRAPER - Enhanced Edition")
    print(f"=" * 60)
    print(f"Target: {TARGET_NEGATIVE} negative + {TARGET_POSITIVE} positive reviews")
    print(f"Games: {num_games}")
    print(f"Per game: ~{negative_per_game} negative + ~{positive_per_game} positive")
    print(f"=" * 60)
    print()
    
    all_reviews = []
    
    for i, (app_id, game_name) in enumerate(GAME_IDS.items(), 1):
        print(f"[{i}/{num_games}] {game_name} (App ID: {app_id})")
        
        # Get game metadata
        details = fetch_game_details(app_id)
        time.sleep(0.3)
        
        # PRIORITIZE NEGATIVE REVIEWS
        neg_reviews = fetch_reviews_by_type(
            app_id, 
            "negative", 
            negative_per_game,
            max_pages=20  # Allow up to 20 pages (2000 reviews) per game
        )
        
        # Get some positive reviews for balance
        pos_reviews = fetch_reviews_by_type(
            app_id, 
            "positive", 
            positive_per_game,
            max_pages=10
        )
        
        print(f"    ✓ Total: {len(neg_reviews)} negative + {len(pos_reviews)} positive")
        
        # Add metadata to all reviews
        for review in pos_reviews + neg_reviews:
            review["game_name"] = game_name
            review["price_usd"] = details.get("price_usd", None)
            review["age_rating"] = details.get("age_rating", None)
            review["game_mode"] = details.get("game_mode", None)
            review["genres"] = details.get("genres", None)
        
        all_reviews.extend(pos_reviews + neg_reviews)
        
        # Progress update
        pos_total = sum(1 for r in all_reviews if r["voted_up"])
        neg_total = len(all_reviews) - pos_total
        print(f"    Running total: {len(all_reviews)} reviews ({pos_total} positive, {neg_total} negative)")
        print()
        
        time.sleep(1)  # Be nice to Steam
    
    # Create DataFrame
    df = pd.DataFrame(all_reviews)
    
    columns = [
        "game_name", "app_id", "price_usd", "age_rating", "game_mode", "genres",
        "user_id", "review_text", "voted_up", "votes_helpful", "votes_funny", "created"
    ]
    df = df[columns]
    
    # Final stats
    pos_count = df["voted_up"].sum()
    neg_count = len(df) - pos_count
    
    print("=" * 60)
    print(f"FINAL RESULTS")
    print("=" * 60)
    print(f"Total reviews: {len(df)}")
    print(f"  Positive: {pos_count} ({pos_count/len(df)*100:.1f}%)")
    print(f"  Negative: {neg_count} ({neg_count/len(df)*100:.1f}%)")
    print("=" * 60)
    
    # Save to CSV
    output_file = "steam_reviews_enhanced.csv"
    df.to_csv(output_file, index=False, encoding="utf-8")
    print(f"✓ Saved to {output_file}")

if __name__ == "__main__":
    main()


    """
    Combined Dataset Analysis
Before (original):

Total: 4,241 reviews
Positive: 2,524 (59.5%)
Negative: 1,717 (40.5%)
Needed: ~800 more negative reviews

New scrape:

Total: 1,372 reviews
Positive: 442 (32.2%)
Negative: 930 (67.8%)
Got: 930 negative reviews ✅

Combined Total:

Total: 5,613 reviews
Positive: 2,966 (52.8%)
Negative: 2,647 (47.2%)
    
    """