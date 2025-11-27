#!/usr/bin/env python3
"""
Combine Steam, IGN, and Metacritic datasets into one master dataset
"""

import pandas as pd
import numpy as np

def standardize_na_values(df):
    """
    Standardize all N/A representations to 'N/A'
    Converts: ###, NaN, None, empty string, null → 'N/A'
    """
    # Replace various N/A representations
    df = df.replace(['###', '', 'nan', 'NaN', 'null', 'NULL', 'None'], 'N/A')
    # Replace actual NaN/None values
    df = df.fillna('N/A')
    return df

def load_datasets(steam_file, steam_enhanced_file, ign_file, metacritic_file):
    """Load all four datasets"""
    print("Loading datasets...")
    steam_df = pd.read_csv(steam_file)
    steam_enhanced_df = pd.read_csv(steam_enhanced_file)
    ign_df = pd.read_csv(ign_file)
    metacritic_df = pd.read_csv(metacritic_file)
    
    # Standardize N/A values in all datasets
    print("\nStandardizing N/A values (###, NaN, empty → N/A)...")
    steam_df = standardize_na_values(steam_df)
    steam_enhanced_df = standardize_na_values(steam_enhanced_df)
    ign_df = standardize_na_values(ign_df)
    metacritic_df = standardize_na_values(metacritic_df)
    
    print(f"\nDataset sizes:")
    print(f"  Steam (original):  {len(steam_df):,} reviews")
    print(f"  Steam (enhanced):  {len(steam_enhanced_df):,} reviews")
    print(f"  IGN:               {len(ign_df):,} reviews")
    print(f"  Metacritic:        {len(metacritic_df):,} reviews")
    print(f"  Total:             {len(steam_df) + len(steam_enhanced_df) + len(ign_df) + len(metacritic_df):,} reviews")
    
    return steam_df, steam_enhanced_df, ign_df, metacritic_df

def create_metadata_lookup(steam_df, steam_enhanced_df):
    """Create a lookup table of game metadata from both Steam datasets"""
    print("\nCreating metadata lookup from Steam data...")
    
    # Combine both Steam datasets for metadata
    combined_steam = pd.concat([steam_df, steam_enhanced_df], ignore_index=True)
    
    # Get unique games with their metadata (use first occurrence)
    metadata = combined_steam.groupby('game_name').agg({
        'app_id': 'first',
        'price_usd': 'first',
        'age_rating': 'first',
        'game_mode': 'first',
        'genres': 'first'
    }).reset_index()
    
    print(f"  Found metadata for {len(metadata)} unique games")
    return metadata

def standardize_ign_metacritic(df, metadata_lookup, source_name):
    """
    Standardize IGN/Metacritic datasets to match Steam format
    
    Current columns: game_name, app_id, author, review_text, user_score, voted_up, date
    Target columns:  game_name, app_id, price_usd, age_rating, game_mode, genres, 
                     user_id, review_text, voted_up, votes_helpful, votes_funny, created
    """
    print(f"\nStandardizing {source_name} dataset...")
    
    # Start with a copy
    df = df.copy()
    
    # 1. Merge with metadata lookup to get price, age_rating, game_mode, genres
    df = df.merge(
        metadata_lookup[['game_name', 'price_usd', 'age_rating', 'game_mode', 'genres']], 
        on='game_name', 
        how='left'
    )
    
    # Check how many games matched
    matched = df['price_usd'].notna().sum()
    print(f"  Matched {matched}/{len(df)} reviews to Steam metadata ({matched/len(df)*100:.1f}%)")
    
    # 2. Rename 'author' to 'user_id' (keep the author name as user_id)
    if 'author' in df.columns:
        df['user_id'] = df['author']
        df = df.drop(columns=['author'])
    
    # 3. Rename 'date' to 'created' if it exists
    if 'date' in df.columns:
        df['created'] = df['date']
        df = df.drop(columns=['date'])
    
    # 4. Add missing columns with N/A
    df['votes_helpful'] = 'N/A'
    df['votes_funny'] = 'N/A'
    
    # If user_id still doesn't exist, add it
    if 'user_id' not in df.columns:
        df['user_id'] = 'N/A'
    
    # 5. Ensure all target columns exist in correct order
    target_columns = [
        'game_name', 'app_id', 'price_usd', 'age_rating', 'game_mode', 'genres',
        'user_id', 'review_text', 'voted_up', 'votes_helpful', 'votes_funny', 'created'
    ]
    
    # Add any missing columns
    for col in target_columns:
        if col not in df.columns:
            df[col] = 'N/A'
    
    # Reorder columns
    df = df[target_columns]
    
    # Final N/A standardization
    df = standardize_na_values(df)
    
    print(f"  ✓ Standardized {len(df)} reviews")
    
    return df

def combine_datasets(steam_df, steam_enhanced_df, ign_df, metacritic_df):
    """Combine all four datasets into one master dataset"""
    print("\n" + "="*60)
    print("COMBINING DATASETS")
    print("="*60)
    
    # 1. Create metadata lookup from both Steam datasets
    metadata_lookup = create_metadata_lookup(steam_df, steam_enhanced_df)
    
    # 2. Standardize IGN and Metacritic
    ign_standardized = standardize_ign_metacritic(ign_df, metadata_lookup, "IGN")
    metacritic_standardized = standardize_ign_metacritic(metacritic_df, metadata_lookup, "Metacritic")
    
    # 3. Ensure both Steam datasets have all columns in correct order
    print("\nStandardizing Steam datasets...")
    target_columns = [
        'game_name', 'app_id', 'price_usd', 'age_rating', 'game_mode', 'genres',
        'user_id', 'review_text', 'voted_up', 'votes_helpful', 'votes_funny', 'created'
    ]
    steam_df = steam_df[target_columns]
    steam_enhanced_df = steam_enhanced_df[target_columns]
    print(f"  ✓ Steam (original) ready: {len(steam_df):,} reviews")
    print(f"  ✓ Steam (enhanced) ready: {len(steam_enhanced_df):,} reviews")
    
    # 4. Combine all four datasets
    print("\nCombining all datasets...")
    combined_df = pd.concat([steam_df, steam_enhanced_df, ign_standardized, metacritic_standardized], ignore_index=True)
    
    print(f"  ✓ Combined {len(combined_df):,} total reviews")
    
    # 5. Statistics
    print("\n" + "="*60)
    print("COMBINED DATASET STATISTICS")
    print("="*60)
    
    print(f"\nTotal reviews: {len(combined_df):,}")
    print(f"  From Steam (original):  {len(steam_df):,}")
    print(f"  From Steam (enhanced):  {len(steam_enhanced_df):,}")
    print(f"  From IGN:               {len(ign_standardized):,}")
    print(f"  From Metacritic:        {len(metacritic_standardized):,}")
    
    print(f"\nLabel distribution:")
    pos_count = combined_df['voted_up'].sum()
    neg_count = len(combined_df) - pos_count
    print(f"  Positive: {pos_count:,} ({pos_count/len(combined_df)*100:.1f}%)")
    print(f"  Negative: {neg_count:,} ({neg_count/len(combined_df)*100:.1f}%)")
    
    print(f"\nUnique games: {combined_df['game_name'].nunique()}")
    
    # Check for missing metadata
    print(f"\nMetadata completeness:")
    for col in ['price_usd', 'age_rating', 'game_mode', 'genres']:
        not_na = (combined_df[col] != 'N/A').sum()
        print(f"  {col:12} {not_na:,} / {len(combined_df):,} "
              f"({not_na/len(combined_df)*100:.1f}%)")
    
    return combined_df

def main():
    # File paths - UPDATE THESE
    STEAM_FILE = "C:\\Users\\User\\OneDrive\\Desktop\\Data Science\\DataScienceDataset\\steam_scrapping\\steam_reviews.csv"                    # Original Steam dataset
    STEAM_ENHANCED_FILE = "C:\\Users\\User\\OneDrive\\Desktop\\Data Science\\DataScienceDataset\\steam_scrapping\\steam_reviews_enhanced.csv"  # Enhanced Steam dataset (1,372 reviews)
    IGN_FILE = "C:\\Users\\User\\OneDrive\\Desktop\\Data Science\\DataScienceDataset\\IGN\\ign_reviews.csv"
    METACRITIC_FILE = "C:\\Users\\User\\OneDrive\\Desktop\\Data Science\\DataScienceDataset\\metacritic\\metacritic_reviews.csv"
    OUTPUT_FILE = "combined_reviews.csv"
    
    print("="*60)
    print("DATASET COMBINER - 4 Datasets Edition")
    print("="*60)
    
    # Load all 4 datasets
    steam_df, steam_enhanced_df, ign_df, metacritic_df = load_datasets(
        STEAM_FILE, STEAM_ENHANCED_FILE, IGN_FILE, METACRITIC_FILE
    )
    
    # Combine
    combined_df = combine_datasets(steam_df, steam_enhanced_df, ign_df, metacritic_df)
    
    # Save
    print(f"\nSaving combined dataset to {OUTPUT_FILE}...")
    combined_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
    print(f"✓ Saved {len(combined_df):,} reviews")
    
    print("\n" + "="*60)
    print("DONE!")
    print("="*60)
    
    return combined_df

if __name__ == "__main__":
    df = main()