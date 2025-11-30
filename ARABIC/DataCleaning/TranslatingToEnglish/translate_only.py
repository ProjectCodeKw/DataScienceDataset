import pandas as pd
import torch
import re
import html as _html
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
from tqdm import tqdm
import gc
from datetime import datetime


class TranslationPipeline:
    def __init__(
        self,
        translation_model="facebook/nllb-200-distilled-600M",
        device="cuda",
        batch_size=4,
    ):
        """
        Initialize pipeline for Arabic to English translation.
        """
        self.device = device
        self.batch_size = batch_size

        print("=" * 70)
        print("ARABIC TO ENGLISH TRANSLATION")
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

        # Load translation model
        print(f"\nLoading translation model: {translation_model}")

        self.src_lang = "ara_Arab"
        self.tgt_lang = "eng_Latn"

        self.translation_tokenizer = AutoTokenizer.from_pretrained(
            translation_model, src_lang=self.src_lang
        )

        self.translation_model = AutoModelForSeq2SeqLM.from_pretrained(
            translation_model,
            torch_dtype=torch.float16 if device == "cuda" else torch.float32,
        ).to(self.device)
        self.translation_model.eval()

        self.tgt_lang_id = self.translation_tokenizer.convert_tokens_to_ids(
            self.tgt_lang
        )

        print(f"✓ Translation model loaded ({self.src_lang} → {self.tgt_lang})")

        if self.device == "cuda":
            allocated = torch.cuda.memory_allocated(0) / 1024**3
            print(f"\n✓ VRAM allocated: {allocated:.2f} GB")

        print("\n" + "=" * 70)

    def strip_html(self, text: str) -> str:
        """Remove HTML tags and unescape HTML entities, returning plain text."""
        if text is None:
            return ""
        s = str(text)
        # Remove HTML tags
        s = re.sub(r"<[^>]+>", " ", s)
        # Unescape HTML entities
        s = _html.unescape(s)
        # Normalize whitespace
        s = " ".join(s.split())
        return s

    def translate_batch(self, texts):
        """Translate Arabic to English"""

        inputs = self.translation_tokenizer(
            texts, return_tensors="pt", padding=True, truncation=True, max_length=512
        ).to(self.device)

        with torch.no_grad():
            translated = self.translation_model.generate(
                **inputs,
                forced_bos_token_id=self.tgt_lang_id,
                max_length=512,
                num_beams=5,
                early_stopping=True,
            )

        translations = self.translation_tokenizer.batch_decode(
            translated, skip_special_tokens=True
        )

        return translations

    def process_dataframe(self, df, review_column="review_text"):
        """
        Process dataframe:
        - Strip HTML from reviews
        - Translate Arabic to English
        """

        print(f"\nProcessing {len(df):,} reviews...")
        print(f"Review column: {review_column}\n")

        # Check column
        if review_column not in df.columns:
            raise ValueError(f"Column '{review_column}' not found")

        results = []

        # Process each review
        for idx, row in tqdm(df.iterrows(), total=len(df), desc="Translating"):

            original_text = row[review_column]

            if pd.isna(original_text) or not str(original_text).strip():
                # Keep empty as-is
                results.append(row.to_dict())
                continue

            # Strip HTML and normalize text
            cleaned_input = self.strip_html(original_text)

            # Translate to English
            try:
                translated = self.translate_batch([cleaned_input])[0]
            except Exception as e:
                print(f"\n⚠ Translation error at row {idx}: {e}")
                translated = cleaned_input

            # Update review column with translated text
            new_row = row.to_dict()
            new_row[review_column] = translated
            results.append(new_row)

            # Clear GPU cache periodically
            if self.device == "cuda" and idx % 50 == 0:
                torch.cuda.empty_cache()
                gc.collect()

        # Create new dataframe
        result_df = pd.DataFrame(results)

        print("\n" + "=" * 70)
        print("PROCESSING COMPLETE")
        print("=" * 70)
        print(f"\nTranslated reviews: {len(result_df):,}")

        return result_df


def process_csv(input_csv, output_csv, review_column="review_text"):
    """
    Main function to translate Arabic reviews to English.
    """

    print("=" * 70)
    print("ARABIC TO ENGLISH TRANSLATION")
    print("=" * 70)

    # Load CSV
    print(f"\nLoading {input_csv}...")
    df = pd.read_csv(input_csv, encoding="utf-8-sig")
    print(f"✓ Loaded {len(df):,} rows")

    # Initialize pipeline
    pipeline = TranslationPipeline(
        translation_model="facebook/nllb-200-distilled-600M",
        device="cuda",
        batch_size=4,
    )

    # Process
    result_df = pipeline.process_dataframe(df, review_column=review_column)

    # Save
    print(f"\nSaving to {output_csv}...")
    result_df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"✓ Saved {len(result_df):,} rows")

    print("\n" + "=" * 70)
    print("DONE!")
    print("=" * 70)

    return result_df


if __name__ == "__main__":

    INPUT_CSV = "combined_arabic_cleaned1_with_prices.csv"
    OUTPUT_CSV = f"translated_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    result_df = process_csv(
        input_csv=INPUT_CSV,
        output_csv=OUTPUT_CSV,
        review_column="review_text",
    )