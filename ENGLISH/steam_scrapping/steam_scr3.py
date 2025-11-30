#!/usr/bin/env python3
"""
Smart Steam Scraper with Duplicate Detection
- Loads existing cleaned_reviews.csv
- Checks for duplicates BEFORE adding reviews
- Targets equal reviews per game
- Maintains 50/50 positive/negative balance
"""

import pandas as pd
import requests
import time
from typing import Set, Tuple, Dict

STEAM_REVIEW_API = "https://store.steampowered.com/appreviews/{app_id}"
STEAM_STORE_API = "https://store.steampowered.com/api/appdetails"

# Your game list - ADD MORE GAMES HERE
GAME_IDS = {
    251570: "7 Days to Die",
    1044720: "A Short Hike",
    945360: "Among Us",
    2198510: "Anger Foot",
    1172470: "Apex Legends",
    2379780: "Balatro",
    1086940: "Baldur's Gate 3",
    813780: "Before Your Eyes",
    250900: "Binding of Isaac Rebirth",
    1966900: "Brotato",
    504230: "Celeste",
    1903340: "Clair Obscur Expedition 33",
    1158310: "Crusader Kings III",
    462770: "Crypt of the NecroDancer",
    1205140: "Cult of the Lamb",
    268910: "Cuphead",
    1091500: "Cyberpunk 2077",
    374320: "Dark Souls III",
    588650: "Dead Cells",
    1061090: "Demon Turf",
    1085660: "Destiny 2",
    632470: "Disco Elysium",
    230230: "Divinity Original Sin",
    435150: "Divinity Original Sin 2",
    1283400: "Dolmen",
    219740: "Dont Starve",
    570: "Dota 2",
    1100600: "Dusk",
    1245620: "Elden Ring",
    311690: "Enter the Gungeon",
    263340: "FTL Faster Than Light",
    1296610: "Gordian Quest",
    1942280: "Griftlands",
    774361: "Gris",
    553850: "HELLDIVERS 2",
    1145360: "Hades",
    1240440: "Hades II",
    1817070: "Hogwarts Legacy",
    367520: "Hollow Knight",
    1151640: "Horizon Zero Dawn",
    1092790: "Inscryption",
    412020: "Insurgency Sandstorm",
    774781: "Just Shapes and Beats",
    1284410: "Legends of Runeterra",
    1966720: "Lethal Company",
    1888160: "Lies of P",
    319630: "Life is Strange",
    532210: "Life is Strange 2",
    1222690: "Life is Strange True Colors",
    1051510: "Little Misfortune",
    1599340: "Lost Ark",
    2357570: "Marvel Rivals",
    1328670: "Mass Effect Legendary",
    1548850: "Monster Train",
    1203620: "Mortal Shell",
    1677740: "Neon White",
    524220: "Nier Automata",
    1315690: "Nightmare Reaper",
    1490720: "Noita",
    242680: "Nuclear Throne",
    1057090: "Octopath Traveler",
    1096530: "Octopath Traveler II",
    1150690: "Omori",
    420530: "OneShot",
    282140: "Oxenfree",
    1274570: "Oxenfree II",
    444090: "Paladins",
    238960: "Path of Exile",
    1637730: "Peglin",
    555160: "Phasmophobia",
    1070560: "Pizza Tower",
    620: "Portal 2",
    736220: "Post Void",
    1200570: "Prodeus",
    418370: "Rising Storm 2 Vietnam",
    632360: "Risk of Rain 2",
    1145350: "Rogue Legacy 2",
    252490: "Rust",
    322500: "SUPERHOT",
    937010: "Salt and Sanctuary",
    1449850: "Sea of Stars",
    1172620: "Sea of Thieves",
    814380: "Sekiro",
    1089350: "Shovel Knight Dig",
    646570: "Slay the Spire",
    1091500: "Smite 2",
    1113560: "Spiritfarer",
    1494830: "Stacklands",
    1151340: "Steelrising",
    1332010: "Stray",
    774361: "TROUBLESHOOTER",
    286160: "Tabletop Simulator",
    1276390: "The Messenger",
    207610: "The Walking Dead",
    261030: "The Wolf Among Us",
    1245430: "Thymesia",
    394510: "Tower Defense Simulator",
    394690: "Tower Unite",
    1250410: "Turbo Overkill",
    1229490: "Ultrakill",
    1116740: "Unrailed",
    438100: "VRChat",
    1794680: "Vampire Survivors",
    1102190: "Vault of the Void",
    230410: "Warframe",
    1121910: "Yakuza Like a Dragon",
    1182480: "Yu-Gi-Oh Master Duel",
}


class SmartSteamScraper:
    def __init__(self, existing_file='cleaned_reviews.csv', target_per_game=104):
        """
        Initialize scraper with existing data
        
        Args:
            existing_file: Path to cleaned_reviews.csv
            target_per_game: Target number of reviews per game (default: 100)
        """
        self.target_per_game = target_per_game
        self.existing_reviews = set()  # Set of (user_id, game_name) tuples
        self.game_stats = {}  # Track pos/neg counts per game
        
        # Load existing data
        try:
            self.df_existing = pd.read_csv(existing_file)
            print(f"Loaded {len(self.df_existing):,} existing reviews from {existing_file}")
            
            # Build duplicate detection set
            for _, row in self.df_existing.iterrows():
                if row['user_id'] != 'N/A':  # Only track real users
                    self.existing_reviews.add((row['user_id'], row['game_name']))
            
            print(f"Tracking {len(self.existing_reviews):,} unique (user, game) pairs for duplicate detection")
            
            # Calculate current stats per game
            for game in self.df_existing['game_name'].unique():
                game_data = self.df_existing[self.df_existing['game_name'] == game]
                pos_count = game_data['voted_up'].sum()
                neg_count = len(game_data) - pos_count
                self.game_stats[game] = {'positive': pos_count, 'negative': neg_count}
            
        except FileNotFoundError:
            print(f"No existing file found. Starting fresh.")
            self.df_existing = pd.DataFrame()
            self.existing_reviews = set()
            self.game_stats = {}
    
    def is_duplicate(self, user_id: str, game_name: str) -> bool:
        """Check if this user already reviewed this game"""
        return (user_id, game_name) in self.existing_reviews
    
    def calculate_needed(self, game_name: str) -> Dict[str, int]:
        """Calculate how many positive and negative reviews are needed for this game"""
        if game_name in self.game_stats:
            current_pos = self.game_stats[game_name]['positive']
            current_neg = self.game_stats[game_name]['negative']
        else:
            current_pos = 0
            current_neg = 0
        
        target_pos = self.target_per_game // 2
        target_neg = self.target_per_game // 2
        
        needed_pos = max(0, target_pos - current_pos)
        needed_neg = max(0, target_neg - current_neg)
        
        return {
            'positive': needed_pos,
            'negative': needed_neg,
            'current_positive': current_pos,
            'current_negative': current_neg
        }
    
    def fetch_game_metadata(self, app_id: int) -> Dict:
        """Fetch game metadata from Steam"""
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
            
            # Extract metadata
            price_data = game_data.get("price_overview", {})
            if game_data.get("is_free"):
                price_usd = 0.0
            else:
                price_usd = price_data.get("final", 0) / 100
            
            age_rating = game_data.get("required_age", 0)
            
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
            
            genres = game_data.get("genres", [])
            genre_names = ", ".join([g.get("description", "") for g in genres])
            
            return {
                "price_usd": price_usd,
                "age_rating": age_rating,
                "game_mode": game_mode,
                "genres": genre_names
            }
        except Exception as e:
            print(f"  Error fetching metadata: {e}")
            return {}
    
    def scrape_game(self, app_id: int, game_name: str) -> list:
        """
        Scrape reviews for a single game with duplicate detection
        
        Returns list of new reviews (not duplicates)
        """
        print(f"\n{'='*60}")
        print(f"Game: {game_name} (App ID: {app_id})")
        print(f"{'='*60}")
        
        # Calculate what we need
        needed = self.calculate_needed(game_name)
        needed_pos = needed['positive']
        needed_neg = needed['negative']
        
        print(f"Current: {needed['current_positive']} positive, {needed['current_negative']} negative")
        print(f"Target:  {self.target_per_game//2} positive, {self.target_per_game//2} negative")
        print(f"Need:    {needed_pos} positive, {needed_neg} negative")
        
        if needed_pos == 0 and needed_neg == 0:
            print("✓ Target already reached for this game. Skipping.")
            return []
        
        # Fetch metadata
        metadata = self.fetch_game_metadata(app_id)
        time.sleep(0.3)
        
        new_reviews = []
        collected_pos = 0
        collected_neg = 0
        
        # Scrape positive reviews if needed
        if needed_pos > 0:
            print(f"\nScraping {needed_pos} positive reviews...")
            pos_reviews = self._scrape_by_type(app_id, game_name, "positive", needed_pos, metadata)
            new_reviews.extend(pos_reviews)
            collected_pos = len(pos_reviews)
        
        # Scrape negative reviews if needed
        if needed_neg > 0:
            print(f"\nScraping {needed_neg} negative reviews...")
            neg_reviews = self._scrape_by_type(app_id, game_name, "negative", needed_neg, metadata)
            new_reviews.extend(neg_reviews)
            collected_neg = len(neg_reviews)
        
        print(f"\n✓ Collected {len(new_reviews)} NEW reviews ({collected_pos} pos, {collected_neg} neg)")
        
        # Update stats
        if game_name not in self.game_stats:
            self.game_stats[game_name] = {'positive': 0, 'negative': 0}
        self.game_stats[game_name]['positive'] += collected_pos
        self.game_stats[game_name]['negative'] += collected_neg
        
        return new_reviews
    
    def _scrape_by_type(self, app_id: int, game_name: str, review_type: str, 
                       target_count: int, metadata: Dict) -> list:
        """
        Scrape reviews of a specific type (positive/negative) with duplicate detection
        """
        reviews = []
        cursor = "*"
        pages_checked = 0
        duplicates_found = 0
        max_pages = 50
        
        params = {
            "json": 1,
            "language": "english",
            "filter": "recent",
            "review_type": review_type,
            "num_per_page": 100,
            "purchase_type": "all"
        }
        
        while len(reviews) < target_count and pages_checked < max_pages:
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
                
                # Process each review with duplicate checking
                for review in batch_reviews:
                    review_text = review.get("review", "")
                    user_id = review["author"]["steamid"]
                    
                    # Check for duplicate
                    if self.is_duplicate(user_id, game_name):
                        duplicates_found += 1
                        continue  # Skip this duplicate
                    
                    # Check minimum length
                    if len(review_text.split()) < 5:
                        continue
                    
                    # Add review
                    reviews.append({
                        "game_name": game_name,
                        "app_id": app_id,
                        "price_usd": metadata.get("price_usd", 'N/A'),
                        "age_rating": metadata.get("age_rating", 'N/A'),
                        "game_mode": metadata.get("game_mode", 'N/A'),
                        "genres": metadata.get("genres", 'N/A'),
                        "user_id": user_id,
                        "review_text": review_text,
                        "voted_up": review["voted_up"],
                        "votes_helpful": review["votes_up"],
                        "votes_funny": review["votes_funny"],
                        "created": review["timestamp_created"]
                    })
                    
                    # Add to duplicate tracker
                    self.existing_reviews.add((user_id, game_name))
                    
                    if len(reviews) >= target_count:
                        break
                
                print(f"  Page {pages_checked+1}: {len(reviews)}/{target_count} collected, {duplicates_found} duplicates skipped", end='\r')
                
                pages_checked += 1
                cursor = data.get("cursor")
                
                if not cursor or cursor == "*":
                    break
                
                time.sleep(0.5)
                
            except Exception as e:
                print(f"\n  Error: {e}")
                break
        
        print(f"\n  Final: {len(reviews)} reviews collected, {duplicates_found} duplicates skipped")
        return reviews
    
    def scrape_all_games(self, game_ids: Dict[int, str]) -> pd.DataFrame:
        """Scrape all games and return DataFrame of new reviews"""
        all_new_reviews = []
        
        print("="*60)
        print("SMART STEAM SCRAPER - STARTING")
        print("="*60)
        print(f"Target: {self.target_per_game} reviews per game ({self.target_per_game//2} pos + {self.target_per_game//2} neg)")
        print(f"Games to scrape: {len(game_ids)}")
        print("="*60)
        
        for i, (app_id, game_name) in enumerate(game_ids.items(), 1):
            print(f"\n[{i}/{len(game_ids)}]")
            new_reviews = self.scrape_game(app_id, game_name)
            all_new_reviews.extend(new_reviews)
            
            print(f"\nRunning total: {len(all_new_reviews)} new reviews collected")
            time.sleep(1)
        
        return pd.DataFrame(all_new_reviews)

def main():
    # Configuration
    EXISTING_FILE = 'C:\\Users\\User\\OneDrive\\Desktop\\Data Science\\DataScienceDataset\\DataCleaning\\cleaned_reviews.csv'
    OUTPUT_FILE = 'steam_additional_reviews.csv'
    TARGET_PER_GAME = 104  # Adjust this based on your needs
    
    # Initialize scraper
    scraper = SmartSteamScraper(
        existing_file=EXISTING_FILE,
        target_per_game=TARGET_PER_GAME
    )
    
    # Scrape new reviews
    df_new = scraper.scrape_all_games(GAME_IDS)
    
    # Save results
    print("\n" + "="*60)
    print("SCRAPING COMPLETE")
    print("="*60)
    print(f"\nNew reviews collected: {len(df_new):,}")
    
    if len(df_new) > 0:
        pos_count = df_new['voted_up'].sum()
        neg_count = len(df_new) - pos_count
        print(f"  Positive: {pos_count:,} ({pos_count/len(df_new)*100:.1f}%)")
        print(f"  Negative: {neg_count:,} ({neg_count/len(df_new)*100:.1f}%)")
        
        # Save new reviews
        df_new.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
        print(f"\n✓ Saved to {OUTPUT_FILE}")
        
        # Optionally, combine with existing
        response = input("\nCombine with existing cleaned_reviews.csv? (yes/no): ").strip().lower()
        if response == 'yes':
            df_existing = pd.read_csv(EXISTING_FILE)
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            df_combined.to_csv('C:\\Users\\User\\OneDrive\\Desktop\\Data Science\\DataScienceDataset\\DataCleaning\\cleaned_reviews_updated.csv', index=False, encoding='utf-8')
            print(f"✓ Saved combined dataset to cleaned_reviews_updated.csv ({len(df_combined):,} total reviews)")
    else:
        print("\nNo new reviews collected (all targets already met)")

if __name__ == "__main__":
    main()