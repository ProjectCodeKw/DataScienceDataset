#!/usr/bin/env python3
"""
Clean combined reviews dataset
1. Remove duplicates
2. Remove short reviews (< 3 words)
3. Remove sarcastic reviews (votes_funny > 2)
4. Remove super long reviews (> 300 words)
5. Preprocess text (lowercase, remove special chars, whitespace)
"""

import pandas as pd
import re

def clean_reviews(input_file, output_file):
    df = pd.read_csv(input_file)
    
    print(f"Starting with {len(df):,} reviews")
    
    # 1. Remove duplicates (exact text)
    before = len(df)
    df = df.drop_duplicates(subset=['review_text'], keep='first')
    print(f"Removed {before - len(df):,} duplicate reviews")
    
    # 2. Preprocess text
    print("Preprocessing text...")
    df['review_text'] = df['review_text'].astype(str)
    df['review_text'] = df['review_text'].str.lower()
    df['review_text'] = df['review_text'].str.replace(r'\n+', ' ', regex=True)  # newlines
    df['review_text'] = df['review_text'].str.replace(r'\s+', ' ', regex=True)  # extra spaces
    df['review_text'] = df['review_text'].str.strip()
    
    # 3. Remove short reviews (< 3 words)
    df['word_count'] = df['review_text'].str.split().str.len()
    before = len(df)
    df = df[df['word_count'] >= 3]
    print(f"Removed {before - len(df):,} reviews (< 3 words)")
    
    # 4. Remove sarcastic reviews (votes_funny > 2)
    before = len(df)
    df['votes_funny_num'] = pd.to_numeric(df['votes_funny'], errors='coerce').fillna(0)
    df = df[df['votes_funny_num'] <= 2]
    print(f"Removed {before - len(df):,} reviews (funny > 2)")
    
    # 5. Remove super long reviews (> 300 words)
    before = len(df)
    df = df[df['word_count'] <= 300]
    print(f"Removed {before - len(df):,} reviews (> 300 words)")
    
    # Drop temporary columns
    df = df.drop(columns=['word_count', 'votes_funny_num'])
    
    # Save
    df.to_csv(output_file, index=False, encoding='utf-8')
    
    # Final statistics
    print(f"\n{'='*60}")
    print(f"FINAL DATASET")
    print(f"{'='*60}")
    print(f"Total reviews: {len(df):,}")
    
    steam_count = (df['source'] == 'Steam').sum()
    metacritic_count = (df['source'] == 'Metacritic').sum()
    
    print(f"\nSource distribution:")
    print(f"  Steam:      {steam_count:,} ({steam_count/len(df)*100:.1f}%)")
    print(f"  Metacritic: {metacritic_count:,} ({metacritic_count/len(df)*100:.1f}%)")
    
    print(f"\nSaved to {output_file}")
    
    return df

# Run
df = clean_reviews('final_datasets/combined_reviews.csv', 'cleaned_reviews.csv')