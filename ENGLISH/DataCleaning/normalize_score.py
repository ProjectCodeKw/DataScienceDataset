import pandas as pd

df = pd.read_csv('balanced_reviews.csv')

# Calculate average user_score per game from Metacritic - SEPARATED by voted_up
metacritic = df[df['source'] == 'Metacritic']

# Positive reviews average per game
game_avg_positive = metacritic[metacritic['voted_up'] == True].groupby('game_name')['user_score'].mean().to_dict()

# Negative reviews average per game
game_avg_negative = metacritic[metacritic['voted_up'] == False].groupby('game_name')['user_score'].mean().to_dict()

# Apply to Steam reviews
def assign_user_score(row):
    if row['source'] == 'Metacritic':
        return row['user_score']
    
    if row['voted_up']:
        game_avg = game_avg_positive.get(row['game_name'])
        default = 7
    else:
        game_avg = game_avg_negative.get(row['game_name'])
        default = 3
    
    if game_avg is None:
        return default
    
    return int(round(game_avg))

df['user_score'] = df.apply(assign_user_score, axis=1)

# NEW: Calculate game average score across entire dataset
game_overall_avg = df.groupby('game_name')['user_score'].mean().to_dict()

# Add as new column
df['game_avg_score'] = df['game_name'].map(game_overall_avg).round(2)

print(f"Total reviews: {len(df):,}")
print(f"\nGame average scores:")
for game, score in sorted(game_overall_avg.items()):
    print(f"  {game}: {score:.2f}")

df.to_csv('normalized_reviews.csv', index=False, encoding='utf-8')
print(f"\nSaved to normalized_reviews.csv")