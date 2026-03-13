"""
index_english_txt.py
Chunk English .txt files from True Parents' Word collection and save as JSONL
for insertion via insert_jp_chunks.py.

Usage:
    python3 scripts/index_english_txt.py

Output:
    output/english_chunks/<filename>.jsonl
"""

import glob
import json
import os
import re

TXT_DIR = "English Version of (Malssum) True Parents' Word"
OUTPUT_DIR = "output/english_chunks"
CHUNK_SIZE = 800    # chars per chunk
CHUNK_OVERLAP = 80  # overlap between chunks


def clean_text(text: str) -> str:
    # Collapse multiple blank lines to one
    text = re.sub(r'\n{3,}', '\n\n', text)
    # Strip lines that are only page numbers (e.g. "8\n" alone)
    text = re.sub(r'(?m)^\d+\s*$', '', text)
    return text.strip()


def chunk_text(text: str, size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP):
    chunks = []
    start = 0
    while start < len(text):
        end = min(start + size, len(text))
        # Try to break at sentence boundary
        if end < len(text):
            for sep in ('. ', '.\n', '\n\n', ' '):
                pos = text.rfind(sep, start + size // 2, end)
                if pos != -1:
                    end = pos + len(sep)
                    break
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        if end >= len(text):
            break
        start += size - overlap
    return chunks


def process_file(txt_path: str, out_dir: str) -> int:
    name = os.path.basename(txt_path)
    stem = os.path.splitext(name)[0]
    out_path = os.path.join(out_dir, f"{stem}.jsonl")

    if os.path.exists(out_path):
        print(f"  [skip] {name} — already processed")
        return 0

    print(f"  [read] {name}")
    with open(txt_path, encoding="utf-8", errors="replace") as f:
        raw = f.read()

    text = clean_text(raw)
    chunks = chunk_text(text)
    print(f"  [chunk] {len(chunks)} chunks")

    # Try to detect volume number from filename
    vol_match = re.search(r'(\d+)', name)
    vol_num = vol_match.group(1) if vol_match else "1"

    with open(out_path, "w", encoding="utf-8") as f:
        for i, chunk in enumerate(chunks):
            record = {
                "filename": name,
                "s3_key": f"english_malsm/{name}",
                "chunk_index": i,
                "chunk_text": chunk,
                "metadata": {
                    "language": "en",
                    "source_type": "english_txt",
                    "original_language": "en",
                    "book_name": f"True Parents' Word Collection Vol.{vol_num}",
                    "source_filename": name,
                },
            }
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    print(f"  [done] {name} → {out_path} ({len(chunks)} chunks)")
    return len(chunks)


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    txt_files = sorted(glob.glob(os.path.join(TXT_DIR, "*.txt")))
    if not txt_files:
        print(f"No .txt files found in: {TXT_DIR}")
        return

    print(f"Found {len(txt_files)} English txt file(s)")
    total = 0
    for path in txt_files:
        total += process_file(path, OUTPUT_DIR)

    print(f"\nDone. Total chunks: {total:,}")
    print(f"Now run:  python3 scripts/insert_jp_chunks.py --dir {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
