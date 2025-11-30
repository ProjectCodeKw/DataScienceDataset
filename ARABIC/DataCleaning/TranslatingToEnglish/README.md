# Arabic to English Translation & Opinion Extraction Pipeline

## 🎯 What it does
1. **Translates** Arabic reviews to English using NLLB-200 (Meta's state-of-the-art translation model)
2. **Extracts** key opinions using BART summarization
3. **Enforces** word count constraints: **5-300 words**
4. **Saves** results incrementally (safe from crashes)

## 📋 Requirements
- RTX 3080 (10GB VRAM) ✅
- Python 3.8+
- CUDA-enabled PyTorch

## 🚀 Quick Start

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Run the pipeline
```bash
python translate_and_extract_pipeline.py
```

The script will:
- Load `combined_arabic_cleaned1.csv`
- Process 1800 reviews in batches of 8
- Save results to:
  - `processed_reviews_YYYYMMDD_HHMMSS.jsonl` (incremental backup)
  - `combined_arabic_cleaned1_translated.csv` (final output)

## ⚙️ Configuration

Edit the script's `__main__` section:

```python
if __name__ == "__main__":
    
    # Configuration
    INPUT_CSV = "combined_arabic_cleaned1.csv"
    OUTPUT_JSONL = f"processed_reviews_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
    OUTPUT_CSV = "combined_arabic_cleaned1_translated.csv"
    
    # Run pipeline
    process_csv(
        input_csv=INPUT_CSV,
        output_jsonl=OUTPUT_JSONL,
        output_csv=OUTPUT_CSV,
        review_column='review_text'
    )
```

## 🎛️ Advanced Settings

In the `TranslationExtractionPipeline` initialization:

```python
pipeline = TranslationExtractionPipeline(
    translation_model="facebook/nllb-200-distilled-600M",  # Translation model
    summarization_model="facebook/bart-large-cnn",          # Opinion extraction model
    device="cuda",                                          # GPU or CPU
    batch_size=8,                                           # Reviews per batch (adjust for VRAM)
    min_words=5,                                            # Minimum word count
    max_words=300                                           # Maximum word count
)
```

### Batch Size Tuning (RTX 3080 - 10GB VRAM)
- **batch_size=8**: Safe, ~7GB VRAM usage
- **batch_size=12**: Optimal, ~9GB VRAM usage
- **batch_size=16**: May cause OOM errors

## 📊 Output Format

### JSONL file (incremental backup)
Each line is a JSON object:
```json
{
  "index": 0,
  "original": "النص العربي الأصلي...",
  "translated": "The translated English text...",
  "final_opinion": "Key opinions extracted...",
  "word_count": 45
}
```

### CSV file (final output)
Updated columns:
- `review_text`: Replaced with translated & extracted opinions
- `review_text_original`: Backup of original Arabic text
- `word_count`: Word count of final opinion

## 🔄 Pipeline Flow

```
Arabic Review
    ↓
[NLLB-200 Translation]
    ↓
English Translation
    ↓
[Word Count Check]
    ↓
├─ < 5 words → Keep as is
├─ 5-300 words → Keep as is
└─ > 300 words → [BART Summarization] → Extract key opinions
    ↓
Final Opinion (5-300 words)
    ↓
Save to JSONL & CSV
```

## ⏱️ Performance Estimates

**RTX 3080 (10GB VRAM):**
- **Batch size 8**: ~2-3 reviews/second
- **1800 reviews**: ~10-15 minutes total

**CPU (fallback):**
- **Batch size 2**: ~0.3 reviews/second
- **1800 reviews**: ~90-120 minutes total

## 🛡️ Safety Features

1. **Incremental saving**: Results saved to JSONL after each batch
2. **Error handling**: Continues processing even if some reviews fail
3. **GPU memory management**: Clears cache every 80 reviews
4. **Word count validation**: Ensures all outputs meet constraints
5. **Backup original**: Keeps original Arabic text in separate column

## 📈 Statistics Provided

After processing, you'll see:
- Total processed reviews
- Average/min/max word counts
- Percentage within target range (5-300 words)
- Percentage below/above limits
- VRAM usage

## 🐛 Troubleshooting

### Out of Memory (OOM)
```python
# Reduce batch size
batch_size=4  # or even 2
```

### Models not downloading
```bash
# Pre-download models
python -c "from transformers import AutoTokenizer, AutoModelForSeq2SeqLM; AutoTokenizer.from_pretrained('facebook/nllb-200-distilled-600M'); AutoModelForSeq2SeqLM.from_pretrained('facebook/nllb-200-distilled-600M')"
```

### CUDA errors
```bash
# Check CUDA availability
python -c "import torch; print(torch.cuda.is_available())"
```

## 📝 Notes

- First run will download models (~2.4GB total)
- Models are cached in `~/.cache/huggingface/`
- Processing is deterministic (same input = same output)
- JSONL format allows resuming from crashes

## 🔧 Alternative Models

### Smaller/Faster Translation
```python
translation_model="facebook/nllb-200-distilled-600M"  # Current (recommended)
translation_model="Helsinki-NLP/opus-mt-ar-en"        # Faster, less accurate
```

### Different Summarization
```python
summarization_model="facebook/bart-large-cnn"  # Current (balanced)
summarization_model="google/pegasus-xsum"      # More extractive
summarization_model="t5-base"                  # More abstractive
```

## 📧 Support

If you encounter issues:
1. Check GPU memory: `nvidia-smi`
2. Reduce batch size
3. Check JSONL file for partial results
4. Verify input CSV has 'review_text' column
