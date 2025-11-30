import pandas as pd
import torch
from transformers import pipeline
from tqdm import tqdm
import random
import gc
import json

class ReviewAugmenter:
    def __init__(self, model="google/flan-t5-base", device="cuda", batch_size=4):
        """
        Augment reviews using paraphrasing, style transfer, and length variation.
        """
        self.device = device
        self.batch_size = batch_size
        
        print("="*70)
        print("REVIEW DATA AUGMENTATION")
        print("="*70)
        print("Methods: Paraphrasing, Style Transfer, Length Variation")
        
        # Check GPU
        if device == "cuda" and not torch.cuda.is_available():
            print("⚠ CUDA not available, using CPU")
            self.device = "cpu"
        
        if self.device == "cuda":
            gpu_name = torch.cuda.get_device_name(0)
            gpu_memory = torch.cuda.get_device_properties(0).total_memory / 1024**3
            print(f"\n✓ GPU: {gpu_name}")
            print(f"✓ VRAM: {gpu_memory:.1f} GB")
        
        # Load model
        print(f"\nLoading model: {model}")
        self.generator = pipeline(
            "text2text-generation",
            model=model,
            device=0 if device == "cuda" else -1,
            torch_dtype=torch.float16 if device == "cuda" else torch.float32,
            max_length=512
        )
        print("  ✓ Model loaded")
        
        if self.device == "cuda":
            allocated = torch.cuda.memory_allocated(0) / 1024**3
            print(f"\n✓ VRAM allocated: {allocated:.2f} GB")
        
        # Style templates
        self.styles = [
            "enthusiastic",
            "critical", 
            "casual",
            "professional"
        ]
        
        print("\n" + "="*70)
    
    def paraphrase(self, text):
        """Method 1: Paraphrase the review"""
        
        prompt = f"""Rewrite this game review using different words but keeping the same meaning and sentiment.
Make it sound natural and genuine.

Original: {text}

Paraphrased:"""
        
        try:
            result = self.generator(
                prompt,
                max_length=300,
                min_length=10,
                do_sample=True,
                temperature=0.7,
                top_p=0.9
            )
            return result[0]['generated_text'].strip()
        except Exception as e:
            print(f"    ⚠ Paraphrase error: {e}")
            return text
    
    def style_transfer(self, text, style):
        """Method 2: Rewrite in different style"""
        
        style_prompts = {
            "enthusiastic": "Rewrite this review in an enthusiastic, excited tone. Use exclamation marks and positive energy.",
            "critical": "Rewrite this review in a critical, analytical tone. Be more measured and thoughtful.",
            "casual": "Rewrite this review in a casual, conversational tone. Sound like talking to a friend.",
            "professional": "Rewrite this review in a professional, formal tone. Sound like a game journalist."
        }
        
        prompt = f"""{style_prompts[style]}
Keep the same overall sentiment (positive/negative).

Original: {text}

Rewritten ({style}):"""
        
        try:
            result = self.generator(
                prompt,
                max_length=300,
                min_length=10,
                do_sample=True,
                temperature=0.8,
                top_p=0.9
            )
            return result[0]['generated_text'].strip()
        except Exception as e:
            print(f"    ⚠ Style transfer error: {e}")
            return text
    
    def length_variation(self, text, variation_type):
        """Method 3: Expand or compress review"""
        
        if variation_type == "expand":
            prompt = f"""Expand this game review with more details and elaboration.
Add specific examples and descriptions while keeping the same sentiment.
Make it 2-3x longer.

Original: {text}

Expanded version:"""
        else:  # compress
            prompt = f"""Make this game review more concise and to the point.
Keep the key opinions but remove filler words.

Original: {text}

Concise version:"""
        
        try:
            max_len = 400 if variation_type == "expand" else 150
            min_len = 50 if variation_type == "expand" else 10
            
            result = self.generator(
                prompt,
                max_length=max_len,
                min_length=min_len,
                do_sample=True,
                temperature=0.7
            )
            return result[0]['generated_text'].strip()
        except Exception as e:
            print(f"    ⚠ Length variation error: {e}")
            return text
    
    def augment_review(self, row, method, style=None, length_type=None):
        """Augment a single review"""
        
        text = row['review_text']
        
        if method == "paraphrase":
            new_text = self.paraphrase(text)
        elif method == "style":
            new_text = self.style_transfer(text, style)
        elif method == "length":
            new_text = self.length_variation(text, length_type)
        else:
            new_text = text
        
        # Create new row
        new_row = row.copy()
        new_row['review_text'] = new_text
        new_row['augmentation_method'] = f"{method}_{style or length_type or ''}"
        
        return new_row


def augment_dataset(input_csv, output_csv, target_count=5000, device="cuda"):
    """
    Augment dataset to reach target count using multiple methods.
    """
    
    print("="*70)
    print("DATA AUGMENTATION TO 5000 REVIEWS")
    print("="*70)
    
    # Load CSV
    print(f"\nLoading {input_csv}...")
    df = pd.read_csv(input_csv, encoding='utf-8-sig')
    original_count = len(df)
    print(f"  ✓ Loaded {original_count:,} reviews")
    
    if 'review_text' not in df.columns:
        raise ValueError("Column 'review_text' not found")
    
    # Add augmentation tracking column
    df['augmentation_method'] = 'original'
    
    # Check if we need augmentation
    if original_count >= target_count:
        print(f"\n✓ Already have {original_count:,} reviews (target: {target_count:,})")
        print("No augmentation needed!")
        return
    
    needed = target_count - original_count
    print(f"\nNeed to generate: {needed:,} new reviews")
    
    # Calculate distribution
    # Strategy: 40% paraphrase, 40% style (10% each style), 20% length
    paraphrase_count = int(needed * 0.4)
    style_count = int(needed * 0.4)
    length_count = needed - paraphrase_count - style_count
    
    print(f"\nAugmentation plan:")
    print(f"  Paraphrasing: {paraphrase_count:,} ({paraphrase_count/needed*100:.1f}%)")
    print(f"  Style Transfer: {style_count:,} ({style_count/needed*100:.1f}%)")
    print(f"  Length Variation: {length_count:,} ({length_count/needed*100:.1f}%)")
    
    # Initialize augmenter
    augmenter = ReviewAugmenter(device=device)
    
    # Sample reviews to augment (randomly)
    reviews_to_augment = df.sample(n=min(needed, original_count), replace=True, random_state=42)
    
    augmented_reviews = []
    
    # 1. Paraphrasing
    print(f"\n[1/3] Generating paraphrased reviews...")
    paraphrase_samples = reviews_to_augment.head(paraphrase_count)
    
    for idx, row in tqdm(paraphrase_samples.iterrows(), total=len(paraphrase_samples), desc="Paraphrasing"):
        new_row = augmenter.augment_review(row, method="paraphrase")
        augmented_reviews.append(new_row)
        
        if idx % 50 == 0 and device == "cuda":
            torch.cuda.empty_cache()
            gc.collect()
    
    # 2. Style Transfer
    print(f"\n[2/3] Generating style-transferred reviews...")
    style_samples = reviews_to_augment.iloc[paraphrase_count:paraphrase_count+style_count]
    
    styles = ["enthusiastic", "critical", "casual", "professional"]
    per_style = len(style_samples) // len(styles)
    
    for style_idx, style in enumerate(styles):
        start = style_idx * per_style
        end = start + per_style if style_idx < len(styles)-1 else len(style_samples)
        style_subset = style_samples.iloc[start:end]
        
        for idx, row in tqdm(style_subset.iterrows(), total=len(style_subset), desc=f"Style: {style}"):
            new_row = augmenter.augment_review(row, method="style", style=style)
            augmented_reviews.append(new_row)
            
            if idx % 50 == 0 and device == "cuda":
                torch.cuda.empty_cache()
                gc.collect()
    
    # 3. Length Variation
    print(f"\n[3/3] Generating length-varied reviews...")
    length_samples = reviews_to_augment.iloc[paraphrase_count+style_count:paraphrase_count+style_count+length_count]
    
    half = len(length_samples) // 2
    
    # Expand first half
    for idx, row in tqdm(length_samples.head(half).iterrows(), total=half, desc="Expanding"):
        new_row = augmenter.augment_review(row, method="length", length_type="expand")
        augmented_reviews.append(new_row)
        
        if idx % 50 == 0 and device == "cuda":
            torch.cuda.empty_cache()
            gc.collect()
    
    # Compress second half
    for idx, row in tqdm(length_samples.tail(len(length_samples)-half).iterrows(), total=len(length_samples)-half, desc="Compressing"):
        new_row = augmenter.augment_review(row, method="length", length_type="compress")
        augmented_reviews.append(new_row)
        
        if idx % 50 == 0 and device == "cuda":
            torch.cuda.empty_cache()
            gc.collect()
    
    # Combine original + augmented
    augmented_df = pd.DataFrame(augmented_reviews)
    final_df = pd.concat([df, augmented_df], ignore_index=True)
    
    # Shuffle to mix augmented with original
    final_df = final_df.sample(frac=1, random_state=42).reset_index(drop=True)
    
    # Stats
    print(f"\n{'='*70}")
    print("RESULTS")
    print(f"{'='*70}")
    print(f"\nOriginal reviews: {original_count:,}")
    print(f"Augmented reviews: {len(augmented_reviews):,}")
    print(f"Final total: {len(final_df):,}")
    print(f"Target: {target_count:,}")
    
    if len(final_df) >= target_count:
        print(f"\n✓ Target reached! ({len(final_df):,} >= {target_count:,})")
    else:
        print(f"\n⚠ Short by {target_count - len(final_df):,} reviews")
    
    # Augmentation breakdown
    print(f"\nAugmentation method breakdown:")
    method_counts = final_df['augmentation_method'].value_counts()
    for method, count in method_counts.items():
        print(f"  {method}: {count:,} ({count/len(final_df)*100:.1f}%)")
    
    # Sentiment distribution
    if 'voted_up' in final_df.columns:
        print(f"\nSentiment distribution:")
        sentiment_counts = final_df['voted_up'].value_counts()
        for sentiment, count in sentiment_counts.items():
            print(f"  {sentiment}: {count:,} ({count/len(final_df)*100:.1f}%)")
    
    # Save
    print(f"\nSaving to {output_csv}...")
    final_df.to_csv(output_csv, index=False, encoding='utf-8-sig')
    print(f"  ✓ Saved {len(final_df):,} rows")
    
    print(f"\n{'='*70}")
    print("AUGMENTATION COMPLETE!")
    print(f"{'='*70}")
    
    # Show examples
    print(f"\nExample augmentations:")
    print(f"\n{'='*70}")
    
    if len(augmented_reviews) > 0:
        # Show one example of each method
        methods_shown = set()
        
        for aug_row in augmented_reviews[:20]:  # Check first 20
            method = aug_row['augmentation_method']
            base_method = method.split('_')[0]
            
            if base_method not in methods_shown and base_method in ['paraphrase', 'style', 'length']:
                methods_shown.add(base_method)
                
                # Find original (same game if possible)
                original = df[df['game_name'] == aug_row.get('game_name', '')].iloc[0] if 'game_name' in df.columns else df.iloc[0]
                
                print(f"\nMethod: {method}")
                print(f"Original: {original['review_text'][:150]}...")
                print(f"Augmented: {aug_row['review_text'][:150]}...")
                print(f"{'-'*70}")
                
                if len(methods_shown) >= 3:
                    break


if __name__ == "__main__":
    augment_dataset(
        input_csv='combined_arabic_cleaned1.csv',
        output_csv='combined_arabic_augmented_5000.csv',
        target_count=5000,
        device='cuda'
    )