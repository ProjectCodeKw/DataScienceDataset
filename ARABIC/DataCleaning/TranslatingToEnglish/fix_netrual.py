import pandas as pd
import torch
from transformers import pipeline
from tqdm import tqdm
import gc

class NeutralReviewSplitter:
    def __init__(self, model="facebook/bart-large-cnn", device="cuda", batch_size=4):
        """
        Split Neutral reviews using LLM to extract positive and negative opinions.
        
        Args:
            model: HuggingFace model for text generation/summarization
            device: 'cuda' or 'cpu'
            batch_size: Reviews to process at once
        """
        self.device = device
        self.batch_size = batch_size
        
        print("="*70)
        print("NEUTRAL REVIEW SPLITTER - LLM-BASED")
        print("="*70)
        
        # Check GPU
        if device == "cuda" and not torch.cuda.is_available():
            print("⚠ CUDA not available, using CPU")
            self.device = "cpu"
        
        if self.device == "cuda":
            gpu_name = torch.cuda.get_device_name(0)
            gpu_memory = torch.cuda.get_device_properties(0).total_memory / 1024**3
            print(f"\n✓ GPU: {gpu_name}")
            print(f"✓ VRAM: {gpu_memory:.1f} GB")
        
        # Load model for opinion extraction
        print(f"\nLoading model: {model}")
        self.generator = pipeline(
            "text2text-generation",
            model="google/flan-t5-base",  # Better for instruction following
            device=0 if device == "cuda" else -1,
            torch_dtype=torch.float16 if device == "cuda" else torch.float32,
            max_length=512
        )
        print("  ✓ Model loaded")
        
        if self.device == "cuda":
            allocated = torch.cuda.memory_allocated(0) / 1024**3
            print(f"\n✓ VRAM allocated: {allocated:.2f} GB")
        
        print("\n" + "="*70)
    
    def extract_positive_opinions(self, review_text):
        """Extract only positive opinions from review"""
        
        prompt = f"""Extract ONLY the positive opinions and praises from this review. 
Focus on what the reviewer liked, enjoyed, or praised.
Ignore any negative comments.

Review: {review_text}

Positive opinions:"""
        
        try:
            result = self.generator(
                prompt,
                max_length=300,
                min_length=10,
                do_sample=False,
                truncation=True
            )
            return result[0]['generated_text'].strip()
        except Exception as e:
            print(f"    ⚠ Positive extraction error: {e}")
            return review_text  # Fallback to original
    
    def extract_negative_opinions(self, review_text):
        """Extract only negative opinions from review"""
        
        prompt = f"""Extract ONLY the negative opinions and criticisms from this review.
Focus on what the reviewer disliked, complained about, or criticized.
Ignore any positive comments.

Review: {review_text}

Negative opinions:"""
        
        try:
            result = self.generator(
                prompt,
                max_length=300,
                min_length=10,
                do_sample=False,
                truncation=True
            )
            return result[0]['generated_text'].strip()
        except Exception as e:
            print(f"    ⚠ Negative extraction error: {e}")
            return review_text  # Fallback to original
    
    def split_reviews(self, df):
        """
        Split Neutral reviews into positive and negative.
        
        Returns:
            DataFrame with neutral reviews split into two rows each
        """
        
        # Find neutral reviews
        neutral_mask = df['voted_up'] == 'Neutral'
        neutral_count = neutral_mask.sum()
        
        print(f"\n✓ Found {neutral_count:,} Neutral reviews to split")
        
        if neutral_count == 0:
            print("No Neutral reviews found!")
            return df
        
        # Separate data
        neutral_df = df[neutral_mask].copy()
        non_neutral_df = df[~neutral_mask].copy()
        
        print(f"\nProcessing {neutral_count} reviews with LLM...")
        print(f"This will create {neutral_count * 2:,} new reviews\n")
        
        positive_reviews = []
        negative_reviews = []
        
        # Process each neutral review
        for idx, row in tqdm(neutral_df.iterrows(), total=len(neutral_df), desc="Splitting"):
            review_text = row['review_text']
            original_score = row['user_score'] if pd.notna(row['user_score']) else 5.0
            
            # Extract positive opinions
            positive_text = self.extract_positive_opinions(review_text)
            
            # Extract negative opinions
            negative_text = self.extract_negative_opinions(review_text)
            
            # Create positive review
            pos_row = row.copy()
            pos_row['review_text'] = positive_text
            pos_row['voted_up'] = True
            pos_row['user_score'] = min(10, original_score + 2)  # Boost by 2
            positive_reviews.append(pos_row)
            
            # Create negative review
            neg_row = row.copy()
            neg_row['review_text'] = negative_text
            neg_row['voted_up'] = False
            neg_row['user_score'] = max(0, original_score - 2)  # Lower by 2
            negative_reviews.append(neg_row)
            
            # Clear GPU cache periodically
            if self.device == "cuda" and idx % 50 == 0:
                torch.cuda.empty_cache()
                gc.collect()
        
        # Combine all reviews
        positive_df = pd.DataFrame(positive_reviews)
        negative_df = pd.DataFrame(negative_reviews)
        
        final_df = pd.concat([non_neutral_df, positive_df, negative_df], ignore_index=True)
        
        return final_df


def main(input_csv, output_csv, device="cuda", batch_size=4):
    print("="*70)
    print("SPLIT NEUTRAL REVIEWS WITH LLM")
    print("="*70)
    
    # Load CSV
    print(f"\nLoading {input_csv}...")
    df = pd.read_csv(input_csv, encoding='utf-8-sig')
    print(f"  ✓ Loaded {len(df):,} rows")
    
    # Check columns
    if 'voted_up' not in df.columns:
        raise ValueError("Column 'voted_up' not found")
    if 'review_text' not in df.columns:
        raise ValueError("Column 'review_text' not found")
    if 'user_score' not in df.columns:
        print("  ⚠ No 'user_score' column, will use default values")
        df['user_score'] = 5.0
    
    # Current distribution
    print(f"\nCurrent distribution:")
    value_counts = df['voted_up'].value_counts()
    for vote, count in value_counts.items():
        print(f"  {vote}: {count:,} ({count/len(df)*100:.1f}%)")
    
    # Initialize splitter
    splitter = NeutralReviewSplitter(device=device, batch_size=batch_size)
    
    # Split reviews
    final_df = splitter.split_reviews(df)
    
    # Stats
    print(f"\n{'='*70}")
    print("RESULTS")
    print(f"{'='*70}")
    print(f"\nOriginal rows: {len(df):,}")
    print(f"Final rows: {len(final_df):,} (+{len(final_df) - len(df):,})")
    
    print(f"\nNew distribution:")
    value_counts_new = final_df['voted_up'].value_counts()
    for vote, count in value_counts_new.items():
        print(f"  {vote}: {count:,} ({count/len(final_df)*100:.1f}%)")
    
    # Score stats
    if 'user_score' in final_df.columns:
        valid_scores = final_df[final_df['user_score'].notna()]['user_score']
        print(f"\nUser score statistics:")
        print(f"  Average: {valid_scores.mean():.2f}")
        print(f"  Median: {valid_scores.median():.2f}")
        print(f"  Min: {valid_scores.min():.2f}")
        print(f"  Max: {valid_scores.max():.2f}")
    
    # Save
    print(f"\nSaving to {output_csv}...")
    final_df.to_csv(output_csv, index=False, encoding='utf-8-sig')
    print(f"  ✓ Saved {len(final_df):,} rows")
    
    print(f"\n{'='*70}")
    print("DONE!")
    print(f"{'='*70}")
    
    # Show examples
    if len(df[df['voted_up'] == 'Neutral']) > 0:
        print(f"\nExample split (first neutral review):")
        first_neutral = df[df['voted_up'] == 'Neutral'].iloc[0]
        neutral_idx = first_neutral.name
        
        # Find corresponding positive and negative in final_df
        pos_match = final_df[(final_df['voted_up'] == True) & 
                             (final_df.index >= len(df[df['voted_up'] != 'Neutral']))]
        neg_match = final_df[(final_df['voted_up'] == False) & 
                             (final_df.index >= len(df[df['voted_up'] != 'Neutral']))]
        
        if len(pos_match) > 0 and len(neg_match) > 0:
            print(f"\nOriginal (Neutral):")
            print(f"  Score: {first_neutral['user_score']:.1f}/10")
            print(f"  Text: {first_neutral['review_text'][:200]}...")
            
            print(f"\n→ Positive review:")
            print(f"  Score: {pos_match.iloc[0]['user_score']:.1f}/10")
            print(f"  Text: {pos_match.iloc[0]['review_text'][:200]}...")
            
            print(f"\n→ Negative review:")
            print(f"  Score: {neg_match.iloc[0]['user_score']:.1f}/10")
            print(f"  Text: {neg_match.iloc[0]['review_text'][:200]}...")


if __name__ == "__main__":
    main(
        input_csv='translated_dataset.csv',
        output_csv='translated_dataset_final.csv',
        device='cuda',
        batch_size=4
    )