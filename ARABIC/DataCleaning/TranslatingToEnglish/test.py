import torch
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM

print("Testing NLLB translation...")

# Sample Arabic text
arabic_text = "اللعبة رائعة جداً. الجرافيكس مذهل والقصة ممتازة. أنصح الجميع بتجربتها."
print(f"\nArabic input: {arabic_text}\n")

# Load model
print("Loading NLLB model...")
model_name = "facebook/nllb-200-distilled-600M"
tokenizer = AutoTokenizer.from_pretrained(model_name, src_lang="ara_Arab")
model = AutoModelForSeq2SeqLM.from_pretrained(
    model_name,
    torch_dtype=torch.float16 if torch.cuda.is_available() else torch.float32
)

device = "cuda" if torch.cuda.is_available() else "cpu"
model = model.to(device)
model.eval()

print(f"Model loaded on: {device}\n")

# Language codes
src_lang = "ara_Arab"
tgt_lang = "eng_Latn"

print(f"Translation: {src_lang} → {tgt_lang}\n")

# Tokenize with source language
print("Tokenizing...")
try:
    inputs = tokenizer(
        arabic_text,
        return_tensors="pt",
        padding=True,
        truncation=True,
        max_length=512
    )
    
    inputs = {k: v.to(device) for k, v in inputs.items()}
    
    print(f"✓ Input tokens: {inputs['input_ids'].shape}")
    
except Exception as e:
    print(f"❌ Tokenization error: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

# Generate with target language
print("\nGenerating translation...")
try:
    # Get the token ID for target language
    tgt_lang_id = tokenizer.convert_tokens_to_ids(tgt_lang)
    print(f"Target language token ID: {tgt_lang_id}")
    
    with torch.no_grad():
        translated = model.generate(
            **inputs,
            forced_bos_token_id=tgt_lang_id,
            max_length=512,
            num_beams=5,
            early_stopping=True
        )
    
    print(f"✓ Output tokens: {translated.shape}")
    
except Exception as e:
    print(f"❌ Generation error: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

# Decode
print("\nDecoding...")
try:
    translation = tokenizer.batch_decode(translated, skip_special_tokens=True)[0]
    
    print(f"\n{'='*70}")
    print("SUCCESS!")
    print(f"{'='*70}")
    print(f"\nArabic: {arabic_text}")
    print(f"\nEnglish: {translation}")
    print(f"\nWord count: {len(translation.split())}")
    print(f"\n{'='*70}")
    
except Exception as e:
    print(f"❌ Decoding error: {e}")
    import traceback
    traceback.print_exc()
    exit(1)