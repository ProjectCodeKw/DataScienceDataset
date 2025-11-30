import pandas as pd
import torch
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM, pipeline
from tqdm import tqdm
import gc
import json
from datetime import datetime

class TranslationExtractionPipeline:
    def __init__(self, 
                 translation_model="facebook/nllb-200-distilled-600M",
                 summarization_model="facebook/bart-large-cnn",
                 device="cuda",
                 batch_size=8,
                 min_words=5,
                 max_words=300):
        """
        Initialize the pipeline with translation and opinion extraction models.
        
        Args:
            translation_model: HuggingFace model for Arabic to English translation
            summarization_model: HuggingFace model for extracting key opinions
            device: 'cuda' for GPU or 'cpu'
            batch_size: Number of reviews to process at once
            min_words: Minimum word count for final output
            max_words: Maximum word count for final output
        """
        self.device = device
        self.batch_size = batch_size
        self.min_words = min_words
        self.max_words = max_words
        
        print("="*70)
        print("INITIALIZING TRANSLATION & OPINION EXTRACTION PIPELINE")
        print("="*70)
        
        # Check GPU availability
        if device == "cuda" and not torch.cuda.is_available():
            print("⚠ CUDA not available, falling back to CPU")
            self.device = "cpu"
        
        if self.device == "cuda":
            gpu_name = torch.cuda.get_device_name(0)
            gpu_memory = torch.cuda.get_device_properties(0).total_memory / 1024**3
            print(f"\n✓ GPU: {gpu_name}")
            print(f"✓ VRAM: {gpu_memory:.1f} GB")
        
        # Load translation model (Arabic to English)
        print(f"\n[1/2] Loading translation model: {translation_model}")
        
        # NLLB language codes: Arabic (ara_Arab) -> English (eng_Latn)
        self.src_lang = "ara_Arab"
        self.tgt_lang = "eng_Latn"
        
        # Load tokenizer with source language
        self.translation_tokenizer = AutoTokenizer.from_pretrained(
            translation_model,
            src_lang=self.src_lang
        )
        
        self.translation_model = AutoModelForSeq2SeqLM.from_pretrained(
            translation_model,
            torch_dtype=torch.float16 if device == "cuda" else torch.float32
        ).to(self.device)
        self.translation_model.eval()
        
        # Get target language token ID
        self.tgt_lang_id = self.translation_tokenizer.convert_tokens_to_ids(self.tgt_lang)
        
        print(f"  ✓ Translation model loaded ({self.src_lang} → {self.tgt_lang})")
        
        # Load summarization/opinion extraction model
        print(f"\n[2/2] Loading opinion extraction model: {summarization_model}")
        self.summarizer = pipeline(
            "summarization",
            model=summarization_model,
            device=0 if device == "cuda" else -1,
            torch_dtype=torch.float16 if device == "cuda" else torch.float32
        )
        print(f"  ✓ Opinion extraction model loaded")
        
        if self.device == "cuda":
            allocated = torch.cuda.memory_allocated(0) / 1024**3
            print(f"\n✓ VRAM allocated: {allocated:.2f} GB")
        
        print(f"\n⚙ Word count constraints: {self.min_words}-{self.max_words} words")
        print("\n" + "="*70)
    
    def count_words(self, text):
        """Count words in text"""
        return len(text.split())
    
    def translate_batch(self, texts):
        """Translate a batch of Arabic texts to English"""
        
        # Tokenize
        inputs = self.translation_tokenizer(
            texts,
            return_tensors="pt",
            padding=True,
            truncation=True,
            max_length=512
        ).to(self.device)
        
        # Generate translations with target language
        with torch.no_grad():
            translated = self.translation_model.generate(
                **inputs,
                forced_bos_token_id=self.tgt_lang_id,
                max_length=512,
                num_beams=5,
                early_stopping=True
            )
        
        # Decode
        translations = self.translation_tokenizer.batch_decode(
            translated,
            skip_special_tokens=True
        )
        
        return translations
    
    def extract_opinions_batch(self, texts):
        """Extract key opinions from translated texts with word count constraints"""
        
        opinions = []
        
        for text in texts:
            word_count = self.count_words(text)
            
            # If text is already within bounds, keep it
            if self.min_words <= word_count <= self.max_words:
                opinions.append(text)
                continue
            
            # If text is too short, keep as is (can't expand)
            if word_count < self.min_words:
                opinions.append(text)
                continue
            
            # If text is too long, summarize it
            if word_count > self.max_words:
                try:
                    # Calculate target length (80% of max to be safe)
                    target_length = int(self.max_words * 0.8)
                    
                    summary = self.summarizer(
                        text,
                        max_length=target_length,
                        min_length=self.min_words,
                        do_sample=False,
                        truncation=True
                    )
                    
                    extracted = summary[0]['summary_text']
                    
                    # Verify word count
                    extracted_words = self.count_words(extracted)
                    
                    # If still too long, truncate
                    if extracted_words > self.max_words:
                        words = extracted.split()[:self.max_words]
                        extracted = ' '.join(words)
                    
                    opinions.append(extracted)
                    
                except Exception as e:
                    # If summarization fails, truncate manually
                    words = text.split()[:self.max_words]
                    opinions.append(' '.join(words))
            else:
                opinions.append(text)
        
        return opinions
    
    def process_reviews(self, reviews, output_file="processed_reviews.jsonl"):
        """
        Process reviews through translation and opinion extraction pipeline.
        Results are saved incrementally to JSONL file.
        
        Args:
            reviews: List of Arabic review texts
            output_file: Output file path (JSONL format for incremental saving)
        
        Returns:
            List of processed results
        """
        
        total_reviews = len(reviews)
        print(f"\nProcessing {total_reviews:,} reviews...")
        print(f"Batch size: {self.batch_size}")
        print(f"Output file: {output_file}")
        print(f"Word constraints: {self.min_words}-{self.max_words} words\n")
        
        results = []
        
        # Open output file for incremental writing
        with open(output_file, 'w', encoding='utf-8') as f:
            
            # Process in batches
            for i in tqdm(range(0, total_reviews, self.batch_size), desc="Processing"):
                batch = reviews[i:i + self.batch_size]
                
                # Step 1: Translate Arabic to English
                try:
                    translations = self.translate_batch(batch)
                except Exception as e:
                    print(f"\n⚠ Translation error at batch {i}: {e}")
                    import traceback
                    traceback.print_exc()
                    translations = ["[TRANSLATION_ERROR]"] * len(batch)
                
                # Step 2: Extract key opinions
                try:
                    opinions = self.extract_opinions_batch(translations)
                except Exception as e:
                    print(f"\n⚠ Opinion extraction error at batch {i}: {e}")
                    opinions = translations  # Use translations as fallback
                
                # Save results incrementally
                for j, (original, translated, opinion) in enumerate(zip(batch, translations, opinions)):
                    result = {
                        'index': i + j,
                        'original': original,
                        'translated': translated,
                        'final_opinion': opinion,
                        'word_count': self.count_words(opinion)
                    }
                    
                    # Write to file immediately (JSONL format)
                    f.write(json.dumps(result, ensure_ascii=False) + '\n')
                    f.flush()  # Ensure it's written to disk
                    
                    results.append(result)
                
                # Clear GPU cache periodically
                if self.device == "cuda" and i % (self.batch_size * 10) == 0:
                    torch.cuda.empty_cache()
                    gc.collect()
        
        # Statistics
        print("\n" + "="*70)
        print("PROCESSING COMPLETE")
        print("="*70)
        
        word_counts = [r['word_count'] for r in results]
        
        print(f"\nTotal processed: {len(results):,}")
        print(f"Average word count: {sum(word_counts)/len(word_counts):.1f}")
        print(f"Min word count: {min(word_counts)}")
        print(f"Max word count: {max(word_counts)}")
        
        # Count reviews within target range
        in_range = sum(1 for wc in word_counts if self.min_words <= wc <= self.max_words)
        print(f"\nWithin target range ({self.min_words}-{self.max_words} words): {in_range:,} ({in_range/len(results)*100:.1f}%)")
        
        below_min = sum(1 for wc in word_counts if wc < self.min_words)
        above_max = sum(1 for wc in word_counts if wc > self.max_words)
        
        if below_min > 0:
            print(f"Below minimum: {below_min:,} ({below_min/len(results)*100:.1f}%)")
        if above_max > 0:
            print(f"Above maximum: {above_max:,} ({above_max/len(results)*100:.1f}%)")
        
        print(f"\nResults saved to: {output_file}")
        
        return results


def process_csv(input_csv, output_jsonl, output_csv=None, review_column='review_text'):
    """
    Main function to process reviews from CSV file.
    
    Args:
        input_csv: Input CSV file with Arabic reviews
        output_jsonl: Output JSONL file for processed results
        output_csv: Optional output CSV file (updates review_text column)
        review_column: Name of the column containing review text
    """
    
    print("="*70)
    print("ARABIC REVIEW PROCESSING PIPELINE")
    print("="*70)
    
    # Load CSV
    print(f"\nLoading {input_csv}...")
    df = pd.read_csv(input_csv, encoding='utf-8-sig')
    print(f"  ✓ Loaded {len(df):,} rows")
    
    if review_column not in df.columns:
        raise ValueError(f"Column '{review_column}' not found in CSV")
    
    # Get reviews
    reviews = df[review_column].fillna("").tolist()
    non_empty = sum(1 for r in reviews if r.strip())
    print(f"  ✓ Found {non_empty:,} non-empty reviews")
    
    # Initialize pipeline
    pipeline = TranslationExtractionPipeline(
        translation_model="facebook/nllb-200-distilled-600M",
        summarization_model="facebook/bart-large-cnn",
        device="cuda",
        batch_size=8,
        min_words=5,
        max_words=300
    )
    
    # Process reviews
    results = pipeline.process_reviews(reviews, output_file=output_jsonl)
    
    # Update CSV if requested
    if output_csv:
        print(f"\nUpdating CSV file...")
        df['review_text_original'] = df[review_column]  # Backup original
        df[review_column] = [r['final_opinion'] for r in results]
        df['word_count'] = [r['word_count'] for r in results]
        
        df.to_csv(output_csv, index=False, encoding='utf-8-sig')
        print(f"  ✓ Saved updated CSV to: {output_csv}")
        print(f"  ✓ Original reviews backed up to 'review_text_original' column")
    
    print("\n" + "="*70)
    print("ALL DONE!")
    print("="*70)


if __name__ == "__main__":
    
    # Configuration
    INPUT_CSV = "combined_arabic_cleaned1_with_prices.csv"
    OUTPUT_JSONL = f"processed_reviews_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
    OUTPUT_CSV = "translated_dataset.csv"
    
    # Run pipeline
    process_csv(
        input_csv=INPUT_CSV,
        output_jsonl=OUTPUT_JSONL,
        output_csv=OUTPUT_CSV,
        review_column='review_text'
    )