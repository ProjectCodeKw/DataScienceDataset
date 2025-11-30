#!/usr/bin/env python3
"""
Combine Steam and Metacritic datasets
"""

import pandas as pd

def combine_datasets(steam_file1, metacritic_file, output_file):
    # Load datasets
    steam1 = pd.read_csv(steam_file1)
    metacritic = pd.read_csv(metacritic_file)
    
    # Add source column
    steam1['source'] = 'Steam'
    metacritic['source'] = 'Metacritic'
    
    # Rename columns to standardize
    metacritic = metacritic.rename(columns={
        'author': 'user_id',
        'date': 'created'
    })
    
    # Define all possible columns
    all_columns = [
        'game_name', 'app_id', 'price_usd', 'age_rating', 'game_mode', 
        'genres', 'user_id', 'review_text', 'voted_up', 'votes_helpful', 
        'votes_funny', 'created', 'user_score', 'source'
    ]
    
    # Add missing columns with N/A
    for df in [steam1, metacritic]:
        for col in all_columns:
            if col not in df.columns:
                df[col] = 'N/A'
    
    # Reorder columns
    steam1 = steam1[all_columns]
    metacritic = metacritic[all_columns]
    
    # Combine
    combined = pd.concat([steam1, metacritic], ignore_index=True)
    
    # Replace NaN with N/A
    combined = combined.fillna('N/A')
    
    # Save
    combined.to_csv(output_file, index=False, encoding='utf-8')
    
    print(f"Combined {len(combined):,} reviews")
    print(f"  Steam (file 1): {len(steam1):,}")
    print(f"  Metacritic:     {len(metacritic):,}")
    print(f"\nSaved to {output_file}")
    
    return combined

# Run
df = combine_datasets(
    'steam_additional_reviews.csv',
    'metacritic_reviews_2500.csv',
    'combined_reviews.csv'
)