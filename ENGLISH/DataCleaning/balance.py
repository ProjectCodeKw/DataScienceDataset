"""
Balance the dataset to have 65%, 35% of Steam and Metacritic reviews
"""

import pandas as pd

df = pd.read_csv('cleaned_reviews.csv')

steam = df[df['source'] == 'Steam'].copy()
metacritic = df[df['source'] == 'Metacritic']

target_steam = 3351
current_count = len(steam)

print(f"Steam reviews: {current_count:,}")
print(f"Target: {target_steam:,}")
print(f"Need to remove: {current_count - target_steam:,}")

# Remove one review per game until we reach target
while len(steam) > target_steam:
    # Get one random review from each game
    to_remove = steam.groupby('game_name').sample(n=1, random_state=42)
    # Remove them
    steam = steam.drop(to_remove.index)
    # Stop if we've reached target
    if len(steam) <= target_steam:
        break

# If we removed too many, add some back
if len(steam) < target_steam:
    need_to_add = target_steam - len(steam)
    removed_reviews = df[df['source'] == 'Steam'].drop(steam.index)
    add_back = removed_reviews.sample(n=need_to_add, random_state=42)
    steam = pd.concat([steam, add_back])

# Combine
df_balanced = pd.concat([steam, metacritic], ignore_index=True)

df_balanced.to_csv('balanced_reviews.csv', index=False, encoding='utf-8')

print(f"\nFinal: {len(df_balanced):,} reviews")
print(f"  Steam: {len(steam):,}")
print(f"  Metacritic: {len(metacritic):,}")
print(f"\nGames in dataset: {df_balanced['game_name'].nunique()}")