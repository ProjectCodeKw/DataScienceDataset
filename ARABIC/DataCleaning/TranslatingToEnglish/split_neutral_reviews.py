import pandas as pd
import torch
from transformers import pipeline
from tqdm import tqdm
import gc
from datetime import datetime


class NeutralReviewSplitter:
    def __init__(self, model="google/flan-t5-base", device="cuda", batch_size=4):
        """
        Split neutral reviews into positive and negative sections.
        Reviews are already translated to English.
        """
        self.device = device
        self.batch_size = batch_size

        print("=" * 70)
        print("NEUTRAL REVIEW SPLITTER")
        print("=" * 70)

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
        self.extractor = pipeline(
            "text2text-generation",
            model=model,
            device=0 if device == "cuda" else -1,
            torch_dtype=torch.float16 if device == "cuda" else torch.float32,
        )
        print(f"✓ Model loaded")

        if self.device == "cuda":
            allocated = torch.cuda.memory_allocated(0) / 1024**3
            print(f"\n✓ VRAM allocated: {allocated:.2f} GB")

        print("\n" + "=" * 70)

    def extract_positive_only(self, text):
        """
        Extract ONLY positive opinions.
        CRITICAL: Must not include ANY negative opinions.
        """

        prompt = f"""Extract ONLY the positive opinions from this game review.
Focus EXCLUSIVELY on what the reviewer liked, praised, enjoyed, or appreciated.
List only the key positive points. Be concise.
DO NOT mention anything negative, criticism, or complaints.
Ignore HTML tags and gibberish.

Review: {text}

Positive points:"""

        try:
            result = self.extractor(
                prompt,
                max_new_tokens=250,
                min_length=10,
                do_sample=False,
                truncation=True,
            )
            extracted = result[0]["generated_text"].strip()
            return extracted if extracted else "No positive opinions found."
        except Exception as e:
            print(f"    ⚠ Positive extraction error: {e}")
            return "No positive opinions found."

    def extract_negative_only(self, text):
        """
        Extract ONLY negative opinions.
        CRITICAL: Must not include ANY positive opinions.
        """

        prompt = f"""Extract ONLY the negative opinions from this game review.
Focus EXCLUSIVELY on what the reviewer disliked, criticized, complained about, or found disappointing.
List only the key negative points. Be concise.
DO NOT mention anything positive, praise, or things the reviewer liked.
Ignore HTML tags and gibberish.

Review: {text}

Negative points:"""

        try:
            result = self.extractor(
                prompt,
                max_new_tokens=250,
                min_length=10,
                do_sample=False,
                truncation=True,
            )
            extracted = result[0]["generated_text"].strip()
            return extracted if extracted else "No negative opinions found."
        except Exception as e:
            print(f"    ⚠ Negative extraction error: {e}")
            return "No negative opinions found."

    def calculate_scores(self, original_score):
        """
        Calculate derived scores from original neutral score.

        Formula:
        - Positive score = original + 2 (max 10)
        - Negative score = original - 2 (min 0)
        - Average: (pos + neg) / 2 = original ✓
        """
        if pd.isna(original_score):
            original_score = 5.0

        positive_score = min(10.0, float(original_score) + 2.0)
        negative_score = max(0.0, float(original_score) - 2.0)

        return positive_score, negative_score

    def split_neutral_reviews(
        self,
        df,
        review_column="review_text",
        sentiment_column="voted_up",
        score_column="user_score",
    ):
        """
        Split neutral reviews into positive and negative rows.
        Non-neutral reviews remain unchanged.

        Returns: New dataframe with neutral reviews split
        """

        print(f"\nProcessing {len(df):,} reviews...")
        print(f"Review column: {review_column}")
        print(f"Sentiment column: {sentiment_column}")
        print(f"Score column: {score_column}\n")

        # Check columns
        if review_column not in df.columns:
            raise ValueError(f"Column '{review_column}' not found")
        if sentiment_column not in df.columns:
            raise ValueError(f"Column '{sentiment_column}' not found")

        # Count neutrals
        neutral_count = (df[sentiment_column] == "Neutral").sum()
        print(f"Found {neutral_count:,} neutral reviews to split\n")

        results = []

        # Process each review
        for idx, row in tqdm(df.iterrows(), total=len(df), desc="Splitting"):

            sentiment = row[sentiment_column]

            # If not neutral, keep as-is
            if sentiment != "Neutral":
                results.append(row.to_dict())
                continue

            # Process neutral review
            original_text = row[review_column]
            original_score = row.get(score_column, 5.0)

            if pd.isna(original_text) or not str(original_text).strip():
                # Empty neutral review - keep as-is
                results.append(row.to_dict())
                continue

            # Extract positive and negative sections
            positive_text = self.extract_positive_only(str(original_text))
            negative_text = self.extract_negative_only(str(original_text))

            # Calculate scores
            positive_score, negative_score = self.calculate_scores(original_score)

            # Create POSITIVE review row
            pos_row = row.to_dict()
            pos_row[review_column] = positive_text
            pos_row[sentiment_column] = True  # voted_up = True
            pos_row[score_column] = positive_score
            pos_row["original_sentiment"] = "Neutral"
            pos_row["split_type"] = "positive"
            results.append(pos_row)

            # Create NEGATIVE review row
            neg_row = row.to_dict()
            neg_row[review_column] = negative_text
            neg_row[sentiment_column] = False  # voted_up = False
            neg_row[score_column] = negative_score
            neg_row["original_sentiment"] = "Neutral"
            neg_row["split_type"] = "negative"
            results.append(neg_row)

            # Clear GPU cache periodically
            if self.device == "cuda" and idx % 50 == 0:
                torch.cuda.empty_cache()
                gc.collect()

        # Create new dataframe
        result_df = pd.DataFrame(results)

        # Statistics
        print("\n" + "=" * 70)
        print("SPLIT COMPLETE")
        print("=" * 70)

        print(f"\nOriginal reviews: {len(df):,}")
        print(f"Final reviews: {len(result_df):,} (+{len(result_df) - len(df):,})")

        if "split_type" in result_df.columns:
            split_counts = result_df["split_type"].value_counts()
            print(f"\nSplit breakdown:")
            for split_type, count in split_counts.items():
                print(f"  {split_type}: {count:,}")

        if sentiment_column in result_df.columns:
            sentiment_counts = result_df[sentiment_column].value_counts()
            print(f"\nFinal sentiment distribution:")
            for sentiment, count in sentiment_counts.items():
                print(f"  {sentiment}: {count:,} ({count/len(result_df)*100:.1f}%)")

        return result_df


def process_csv(
    input_csv,
    output_csv,
    review_column="review_text",
    sentiment_column="voted_up",
    score_column="user_score",
):
    """
    Main function to split neutral reviews.
    Input CSV should have ALREADY TRANSLATED reviews.
    """

    print("=" * 70)
    print("NEUTRAL REVIEW SPLITTING")
    print("=" * 70)

    # Load CSV
    print(f"\nLoading {input_csv}...")
    df = pd.read_csv(input_csv, encoding="utf-8-sig")
    print(f"✓ Loaded {len(df):,} rows")

    # Show current distribution
    if sentiment_column in df.columns:
        print(f"\nCurrent sentiment distribution:")
        sentiment_counts = df[sentiment_column].value_counts()
        for sentiment, count in sentiment_counts.items():
            print(f"  {sentiment}: {count:,} ({count/len(df)*100:.1f}%)")

    # Initialize splitter
    splitter = NeutralReviewSplitter(
        model="google/flan-t5-base", device="cuda", batch_size=4
    )

    # Split neutral reviews
    result_df = splitter.split_neutral_reviews(
        df,
        review_column=review_column,
        sentiment_column=sentiment_column,
        score_column=score_column,
    )

    # Save
    print(f"\nSaving to {output_csv}...")
    result_df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"✓ Saved {len(result_df):,} rows")

    print("\n" + "=" * 70)
    print("DONE!")
    print("=" * 70)

    return result_df


if __name__ == "__main__":

    # Input: Already translated reviews
    INPUT_CSV = "translated_20251130_230833.csv"  # Change to your file
    OUTPUT_CSV = f"split_neutral_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    result_df = process_csv(
        input_csv=INPUT_CSV,
        output_csv=OUTPUT_CSV,
        review_column="review_text",
        sentiment_column="voted_up",
        score_column="user_score",
    )