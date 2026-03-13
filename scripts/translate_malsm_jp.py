"""
translate_malsm_jp.py
Extract Korean text from malsm_pdfs, chunk, translate KO→JP via OpenAI, save JSONL.

Usage:
    python3 scripts/translate_malsm_jp.py [--start 1] [--end 615] [--batch-size 5]

Output:
    output/malsm_jp_chunks/<N>.jsonl  — one file per PDF, each line is a chunk dict

Resume-safe: skips PDFs whose output JSONL already exists.
"""

import argparse
import glob
import json
import os
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

import pdfplumber

# ── Config ────────────────────────────────────────────────────────────────────

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
TRANSLATE_MODEL = "gpt-4o-mini"
CHUNK_SIZE = 800       # chars of Korean text per chunk
CHUNK_OVERLAP = 80     # overlap between consecutive chunks
TRANSLATE_BATCH = 8    # chunks translated per API call (keeps prompts manageable)
RATE_SLEEP = 0.1       # seconds between API calls
PARALLEL_WORKERS = 3   # PDFs processed concurrently

PDF_DIR = "malsm_pdfs"
OUTPUT_DIR = "output/malsm_jp_chunks"


# ── Text extraction ───────────────────────────────────────────────────────────

def extract_text_from_pdf(path: str) -> str:
    pages = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            t = page.extract_text() or ""
            if t.strip():
                pages.append(t.strip())
    return "\n\n".join(pages)


# ── Chunking ──────────────────────────────────────────────────────────────────

def chunk_text(text: str, size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP):
    chunks = []
    start = 0
    while start < len(text):
        end = min(start + size, len(text))
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        if end >= len(text):
            break
        start += size - overlap
    return chunks


# ── OpenAI helpers ────────────────────────────────────────────────────────────

def _openai_post(endpoint: str, payload: dict) -> dict:
    url = f"https://api.openai.com/v1/{endpoint}"
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        url, data=data,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "User-Agent": "MiniHanPipeline/1.0",
        },
        method="POST",
    )
    for attempt in range(4):
        try:
            with urllib.request.urlopen(req, timeout=180) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            if e.code == 429:
                wait = 20 * (attempt + 1)
                print(f"    Rate limited — waiting {wait}s...")
                time.sleep(wait)
            else:
                raise RuntimeError(f"OpenAI {e.code}: {body[:200]}")
    raise RuntimeError("OpenAI: too many retries")


def translate_chunks_to_jp(chunks: list[str]) -> list[str]:
    """Translate a batch of Korean chunks to Japanese in one API call."""
    numbered = "\n\n".join(f"[{i+1}]\n{c}" for i, c in enumerate(chunks))
    prompt = (
        "You are a professional Korean-to-Japanese translator specializing in religious and spiritual texts "
        "by Reverend Sun Myung Moon (True Parents). Translate each numbered passage from Korean to Japanese. "
        "Preserve theological terms (참부모님, 탕감, 사위기대, etc.) with standard Japanese Unification Church terminology. "
        "Return ONLY the numbered translations in the same format, nothing else.\n\n"
        + numbered
    )
    resp = _openai_post("chat/completions", {
        "model": TRANSLATE_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1,
    })
    content = resp["choices"][0]["message"]["content"].strip()

    # Parse numbered output back into list
    results = []
    current_idx = None
    current_lines = []
    for line in content.splitlines():
        stripped = line.strip()
        if stripped.startswith("[") and "]" in stripped:
            try:
                idx = int(stripped[1:stripped.index("]")])
                if current_idx is not None:
                    results.append((current_idx, "\n".join(current_lines).strip()))
                current_idx = idx
                current_lines = [stripped[stripped.index("]")+1:].strip()]
            except ValueError:
                if current_idx is not None:
                    current_lines.append(line)
        else:
            if current_idx is not None:
                current_lines.append(line)
    if current_idx is not None:
        results.append((current_idx, "\n".join(current_lines).strip()))

    # Build ordered list matching input length
    lookup = {idx: text for idx, text in results}
    return [lookup.get(i + 1, chunks[i]) for i in range(len(chunks))]


# ── Pipeline ──────────────────────────────────────────────────────────────────

def process_pdf(pdf_path: str, out_dir: str) -> int:
    name = os.path.basename(pdf_path)
    stem = os.path.splitext(name)[0]
    out_path = os.path.join(out_dir, f"{stem}.jsonl")

    if os.path.exists(out_path):
        print(f"  [skip] {name} — already processed")
        return 0

    print(f"  [extract] {name}")
    text = extract_text_from_pdf(pdf_path)
    if len(text.strip()) < 50:
        print(f"  [warn] {name} — very little text ({len(text)} chars), skipping")
        return 0

    chunks = chunk_text(text)
    print(f"  [translate] {name} — {len(chunks)} chunks")

    translated_chunks = []
    for i in range(0, len(chunks), TRANSLATE_BATCH):
        batch = chunks[i:i + TRANSLATE_BATCH]
        jp_batch = translate_chunks_to_jp(batch)
        translated_chunks.extend(zip(range(i, i + len(batch)), batch, jp_batch))
        time.sleep(RATE_SLEEP)

    # Write JSONL
    with open(out_path, "w", encoding="utf-8") as f:
        for chunk_idx, ko_text, jp_text in translated_chunks:
            record = {
                "filename": name,
                "s3_key": f"malsmCollection615Korean_pdfs/{name}",
                "chunk_index": chunk_idx,
                "chunk_text": jp_text,
                "metadata": {
                    "language": "ja",
                    "source_type": "korean_pdf_translation",
                    "original_language": "ko",
                    "book_name": f"말씀選集 第{stem}巻",
                    "source_filename": name,
                },
            }
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    print(f"  [done] {name} — {len(translated_chunks)} chunks written to {out_path}")
    return len(translated_chunks)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=int, default=1, help="First PDF number to process")
    parser.add_argument("--end", type=int, default=615, help="Last PDF number to process")
    args = parser.parse_args()

    if not OPENAI_API_KEY:
        raise SystemExit("ERROR: OPENAI_API_KEY not set")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    pdfs = sorted(glob.glob(os.path.join(PDF_DIR, "*.pdf")),
                  key=lambda p: int(os.path.splitext(os.path.basename(p))[0])
                  if os.path.splitext(os.path.basename(p))[0].isdigit() else 0)

    subset = [p for p in pdfs
              if os.path.splitext(os.path.basename(p))[0].isdigit()
              and args.start <= int(os.path.splitext(os.path.basename(p))[0]) <= args.end]

    print(f"Processing {len(subset)} PDFs ({args.start}–{args.end}) → {OUTPUT_DIR} [workers={PARALLEL_WORKERS}]")
    total_chunks = 0
    completed = 0

    with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:
        future_to_path = {executor.submit(process_pdf, path, OUTPUT_DIR): path for path in subset}
        for future in as_completed(future_to_path):
            path = future_to_path[future]
            completed += 1
            try:
                n = future.result()
                total_chunks += n
                print(f"[{completed}/{len(subset)}] ✓ {os.path.basename(path)} ({n} chunks) — total so far: {total_chunks:,}")
            except Exception as e:
                print(f"[{completed}/{len(subset)}] ✗ {os.path.basename(path)} FAILED: {e}")

    print(f"\nDone. Total chunks written: {total_chunks:,}")


if __name__ == "__main__":
    main()
