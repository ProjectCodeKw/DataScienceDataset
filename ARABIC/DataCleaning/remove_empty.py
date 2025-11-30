(
    """Remove rows with empty `review_text` in a CSV.

Usage:
  python remove_empty.py -i input.csv -o output.csv

If `--output` is not provided the script will overwrite the input file
after saving a backup named `<input>.bak`.

The script treats rows as empty when the `review_text` column is missing,
NaN, or contains only whitespace.
"""
)

import os
import shutil
import pandas as pd


def remove_empty_reviews(input_path: str, output_path: str = None, backup: bool = True):
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    df = pd.read_csv(input_path, encoding="utf-8-sig")

    if "review_text" not in df.columns:
        raise ValueError("Input CSV must contain a 'review_text' column")

    before = len(df)

    # Normalize review_text to string and strip whitespace
    # Keep rows where stripped text length > 0
    df["_review_text_norm"] = (
        df["review_text"].fillna("").astype(str).str.replace("\u00a0", " ").str.strip()
    )
    cleaned = df[df["_review_text_norm"].str.len() > 0].copy()
    cleaned.drop(columns=["_review_text_norm"], inplace=True)

    removed = before - len(cleaned)

    # If no output_path provided, overwrite input after backing up
    if output_path:
        out_path = output_path
    else:
        out_path = input_path

    if out_path == input_path and backup:
        bak_path = input_path + ".bak"
        shutil.copy2(input_path, bak_path)
        print(f"Backup saved to {bak_path}")

    cleaned.to_csv(out_path, index=False, encoding="utf-8-sig")

    print(f"Rows before: {before}")
    print(f"Rows after : {len(cleaned)}")
    print(f"Rows removed: {removed}")


if __name__ == "__main__":
    # Simple non-CLI usage: set input/output paths here and run
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Edit these variables as needed for your environment
    INPUT_CSV = os.path.join(script_dir, "combined_with_genres.csv")
    OUTPUT_CSV = os.path.join(script_dir, "combined_arabic_cleaned1.csv")

    # If you want to overwrite the input file instead, set OUTPUT_CSV = INPUT_CSV
    # and keep `backup=True` to create a <input>.bak before overwriting.

    print(f"Removing empty reviews from: {INPUT_CSV}")
    remove_empty_reviews(INPUT_CSV, output_path=OUTPUT_CSV, backup=True)
    print("Done.")
