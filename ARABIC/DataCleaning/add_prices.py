import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import re
import json

def get_steam_api_price(game_name, max_retries=3):
    """
    Use Steam Web API to search for game and get price.
    This is more reliable than scraping HTML.
    """
    
    # Step 1: Search for the game to get app_id
    search_url = f"https://store.steampowered.com/api/storesearch/?term={requests.utils.quote(game_name)}&cc=US&l=en"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    }
    
    for attempt in range(max_retries):
        try:
            print(f"    Searching Steam API (attempt {attempt + 1}/{max_retries})...")
            
            response = requests.get(search_url, headers=headers, timeout=10)
            
            if response.status_code == 429:
                print(f"    ⚠ Rate limited. Waiting 15 seconds...")
                time.sleep(15)
                continue
            
            if response.status_code != 200:
                print(f"    ✗ Status code: {response.status_code}")
                return None, None
            
            data = response.json()
            
            if not data.get('items'):
                print(f"    ✗ No results found")
                return None, None
            
            # Get first result
            first_result = data['items'][0]
            found_name = first_result['name']
            app_id = first_result['id']
            
            print(f"    Found: '{found_name}' (App ID: {app_id})")
            
            # Step 2: Get price details from Steam API
            price_url = f"https://store.steampowered.com/api/appdetails?appids={app_id}&cc=US"
            
            price_response = requests.get(price_url, headers=headers, timeout=10)
            
            if price_response.status_code != 200:
                print(f"    ✗ Price API status: {price_response.status_code}")
                return None, app_id
            
            price_data = price_response.json()
            
            if str(app_id) not in price_data or not price_data[str(app_id)]['success']:
                print(f"    ✗ Price data not available")
                return None, app_id
            
            game_data = price_data[str(app_id)]['data']
            
            # Check if free
            if game_data.get('is_free', False):
                print(f"    ✓ Found price: $0.00 (Free to Play)")
                return 0.0, app_id
            
            # Get price
            price_overview = game_data.get('price_overview')
            
            if not price_overview:
                print(f"    ✗ No price overview available")
                return None, app_id
            
            # Get INITIAL price (original price, not discounted)
            initial_price = price_overview.get('initial', 0) / 100.0  # Prices are in cents
            final_price = price_overview.get('final', 0) / 100.0
            
            discount_percent = price_overview.get('discount_percent', 0)
            
            if discount_percent > 0:
                print(f"    ✓ Found ORIGINAL price: ${initial_price:.2f} (currently ${final_price:.2f}, -{discount_percent}% off)")
                return initial_price, app_id
            else:
                print(f"    ✓ Found price: ${final_price:.2f}")
                return final_price, app_id
            
        except requests.Timeout:
            print(f"    ⚠ Request timeout")
            if attempt < max_retries - 1:
                time.sleep(3)
        except Exception as e:
            print(f"    ✗ Error: {e}")
            if attempt < max_retries - 1:
                time.sleep(3)
            else:
                return None, None
    
    print(f"    ✗ Failed after {max_retries} attempts")
    return None, None


def scrape_steam_html_price(game_name, max_retries=2):
    """
    Fallback: Scrape HTML if API fails.
    Search by name, extract app_id, then get price.
    """
    
    search_query = game_name.replace(' ', '+')
    url = f"https://store.steampowered.com/search/?term={search_query}"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cookie': 'birthtime=0; mature_content=1; wants_mature_content=1'
    }
    
    try:
        print(f"    Scraping HTML as fallback...")
        
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code != 200:
            return None, None
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find first result
        first_result = soup.find('a', class_='search_result_row')
        
        if not first_result:
            print(f"    ✗ No search results")
            return None, None
        
        # Get title
        title_tag = first_result.find('span', class_='title')
        if title_tag:
            found_title = title_tag.get_text(strip=True)
            print(f"    Found: '{found_title}'")
        
        # Extract app_id
        app_id_match = re.search(r'/app/(\d+)/', first_result.get('href', ''))
        app_id = app_id_match.group(1) if app_id_match else None
        
        if app_id:
            print(f"    App ID: {app_id}")
        
        # Get original price from search results
        discount_original = first_result.find('div', class_='discount_original_price')
        
        if discount_original:
            price_text = discount_original.get_text(strip=True)
            price_match = re.search(r'\$?([\d,]+\.?\d*)', price_text)
            if price_match:
                price_str = price_match.group(1).replace(',', '')
                price = float(price_str)
                print(f"    ✓ Found ORIGINAL price: ${price:.2f} (on sale)")
                return price, app_id
        
        # Get regular price
        search_price = first_result.find('div', class_='search_price')
        
        if search_price:
            price_text = search_price.get_text(strip=True)
            
            if 'Free' in price_text:
                print(f"    ✓ Found price: $0.00 (Free)")
                return 0.0, app_id
            
            # Extract first price (original if on sale)
            prices = re.findall(r'\$?([\d,]+\.?\d*)', price_text)
            if prices:
                price_str = prices[0].replace(',', '')
                price = float(price_str)
                print(f"    ✓ Found price: ${price:.2f}")
                return price, app_id
        
        print(f"    ✗ No price found")
        return None, app_id
        
    except Exception as e:
        print(f"    ✗ HTML scraping error: {e}")
        return None, None


def get_game_price(game_name):
    """
    Get game price using multiple methods:
    1. Steam API (most reliable)
    2. HTML scraping (fallback)
    """
    
    # Try API first
    price, app_id = get_steam_api_price(game_name)
    
    # If API fails, try HTML scraping
    if price is None:
        print(f"    API failed, trying HTML scraping...")
        price, app_id = scrape_steam_html_price(game_name)
    
    return price, app_id


def main(input_csv, output_csv, delay=1.5):
    print("="*70)
    print("STEAM PRICE SCRAPER - SEARCH BY NAME")
    print("="*70)
    print("⚠ This scraper IGNORES app_id column and searches by game name")
    print("⚠ Gets ORIGINAL prices (before discounts)")
    
    # Load CSV
    print(f"\nLoading {input_csv}...")
    df = pd.read_csv(input_csv, encoding='utf-8-sig')
    print(f"  ✓ Loaded {len(df):,} rows")
    
    # Check for game_name column
    if 'game_name' not in df.columns:
        raise SystemExit("Error: CSV must contain 'game_name' column")
    
    # Add/reset columns
    if 'price' not in df.columns:
        df['price'] = None
    
    # Add correct_app_id column to store the real Steam app IDs
    if 'correct_app_id' not in df.columns:
        df['correct_app_id'] = None
    
    # Get unique games
    unique_games = df['game_name'].dropna().unique()
    print(f"  Found {len(unique_games)} unique games")
    
    # Create caches
    price_cache = {}
    appid_cache = {}
    
    # Check existing prices
    existing_prices = df[df['price'].notna()]['game_name'].unique()
    if len(existing_prices) > 0:
        print(f"  Already have prices for {len(existing_prices)} games")
        # Load existing prices into cache
        for game in existing_prices:
            price_cache[game] = df[df['game_name'] == game]['price'].iloc[0]
            if 'correct_app_id' in df.columns:
                appid = df[df['game_name'] == game]['correct_app_id'].iloc[0]
                if pd.notna(appid):
                    appid_cache[game] = appid
    
    # Scrape prices
    print(f"\nScraping Steam prices BY GAME NAME...")
    print(f"⚠ This may take a while ({len(unique_games)} * {delay}s = {len(unique_games)*delay/60:.1f} min)\n")
    
    found_prices = 0
    failed_games = []
    
    for idx, game_name in enumerate(unique_games, 1):
        print(f"\n[{idx}/{len(unique_games)}] {game_name}")
        
        # Skip if already in cache
        if game_name in price_cache:
            print(f"  ✓ Using cached price: ${price_cache[game_name]:.2f}")
            if game_name in appid_cache:
                print(f"  ✓ Using cached App ID: {appid_cache[game_name]}")
            continue
        
        # Get price
        price, app_id = get_game_price(game_name)
        
        if price is not None:
            price_cache[game_name] = price
            found_prices += 1
            
            if app_id:
                appid_cache[game_name] = app_id
        else:
            price_cache[game_name] = None
            failed_games.append(game_name)
        
        # Progress
        progress = (idx / len(unique_games)) * 100
        success_rate = (found_prices / idx) * 100
        print(f"  Progress: {progress:.1f}% ({found_prices}/{idx} found = {success_rate:.1f}% success)")
        
        # Delay between requests
        if idx < len(unique_games):
            time.sleep(delay)
    
    # Map results to dataframe
    print(f"\nMapping results to all rows...")
    df['price'] = df['game_name'].map(lambda g: price_cache.get(g, None))
    df['correct_app_id'] = df['game_name'].map(lambda g: appid_cache.get(g, None))
    
    # Statistics
    total_with_price = df['price'].notna().sum()
    total_without = df['price'].isna().sum()
    
    print(f"  ✓ Prices mapped")
    print(f"    With price: {total_with_price:,} ({total_with_price/len(df)*100:.1f}%)")
    print(f"    Without: {total_without:,} ({total_without/len(df)*100:.1f}%)")
    
    if total_with_price > 0:
        prices_df = df[df['price'].notna()]
        avg_price = prices_df['price'].mean()
        min_price = prices_df['price'].min()
        max_price = prices_df['price'].max()
        median_price = prices_df['price'].median()
        free_games = (prices_df['price'] == 0).sum()
        
        print(f"\n  Price statistics:")
        print(f"    Average: ${avg_price:.2f}")
        print(f"    Median: ${median_price:.2f}")
        print(f"    Min: ${min_price:.2f}")
        print(f"    Max: ${max_price:.2f}")
        print(f"    Free games: {free_games}")
        
        # Price distribution
        under_10 = (prices_df['price'] < 10).sum()
        between_10_30 = ((prices_df['price'] >= 10) & (prices_df['price'] < 30)).sum()
        between_30_60 = ((prices_df['price'] >= 30) & (prices_df['price'] < 60)).sum()
        over_60 = (prices_df['price'] >= 60).sum()
        
        print(f"\n  Price distribution:")
        print(f"    Under $10: {under_10} ({under_10/len(prices_df)*100:.1f}%)")
        print(f"    $10-$30: {between_10_30} ({between_10_30/len(prices_df)*100:.1f}%)")
        print(f"    $30-$60: {between_30_60} ({between_30_60/len(prices_df)*100:.1f}%)")
        print(f"    Over $60: {over_60} ({over_60/len(prices_df)*100:.1f}%)")
    
    # Show failed games
    if failed_games:
        print(f"\n  ⚠ Failed to get prices for {len(failed_games)} games:")
        for game in failed_games[:15]:
            print(f"    - {game}")
        if len(failed_games) > 15:
            print(f"    ... and {len(failed_games) - 15} more")
        
        # Save failed games list
        with open('failed_games.txt', 'w', encoding='utf-8') as f:
            for game in failed_games:
                f.write(f"{game}\n")
        print(f"\n  ✓ Saved failed games to: failed_games.txt")
    
    # Save
    print(f"\nSaving to {output_csv}...")
    df.to_csv(output_csv, index=False, encoding='utf-8-sig')
    print(f"  ✓ Saved {len(df):,} rows")
    print(f"  ✓ Added 'correct_app_id' column with real Steam app IDs")
    
    print("\n" + "="*70)
    print("COMPLETE!")
    print("="*70)


if __name__ == "__main__":
    
    input_file = 'combined_arabic_cleaned1.csv'
    output_file = 'combined_arabic_cleaned1_with_prices.csv'
    delay = 1.5  # Steam API is faster, so shorter delay
    
    main(input_file, output_file, delay)