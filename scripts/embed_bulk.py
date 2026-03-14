"""
embed_bulk.py
Local batch embedding + Aurora insert via Lambda.

Steps:
1. Read chunks from JSONL files (truebooks / truelove / any dir)
2. Call OpenAI embeddings API in batches of EMBED_BATCH locally
3. Send pre-embedded chunks to Lambda in INSERT_BATCH groups (Lambda skips OpenAI)
4. Track progress per source dir

Usage:
    python3 scripts/embed_bulk.py --dir output/truebooks_chunks
    python3 scripts/embed_bulk.py --dir output/truelove_chunks
    python3 scripts/embed_bulk.py --dir output/truebooks_chunks --dir output/truelove_chunks
"""

import argparse
import glob
import json
import os
import sys
import time
import warnings

warnings.filterwarnings("ignore")

import boto3
from botocore.config import Config
import urllib.request

LAMBDA_FUNCTION = "mini-han-chat"
EMBED_BATCH = 100        # texts per OpenAI call
INSERT_BATCH = 30        # pre-embedded chunks per Lambda call
SLEEP_BETWEEN = 0.05     # seconds between Lambda calls
OPENAI_SECRET_ARN = "arn:aws:secretsmanager:ap-northeast-1:221646756615:secret:rag/openai-api-key-4Mrj6W"
EMBEDDING_MODEL = "text-embedding-3-small"
PROGRESS_FILE = "output/embed_bulk_progress.json"


def get_openai_key() -> str:
    sm = boto3.client("secretsmanager", region_name="ap-northeast-1")
    secret = sm.get_secret_value(SecretId=OPENAI_SECRET_ARN)
    val = secret["SecretString"]
    try:
        return json.loads(val).get("api_key") or json.loads(val).get("OPENAI_API_KEY") or val
    except Exception:
        return val.strip()


def embed_texts(texts: list, openai_key: str) -> list:
    """Batch embed texts using OpenAI API. Returns list of 1536-dim vectors."""
    payload = json.dumps({"model": EMBEDDING_MODEL, "input": texts}).encode()
    req = urllib.request.Request(
        "https://api.openai.com/v1/embeddings",
        data=payload,
        headers={"Authorization": f"Bearer {openai_key}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = json.loads(resp.read())
    items = sorted(data["data"], key=lambda x: x["index"])
    return [item["embedding"] for item in items]


def invoke_insert(client, chunks: list) -> dict:
    """Send pre-embedded chunks to Lambda for DB insert only."""
    payload = json.dumps(
        {"body": json.dumps({"message": "__insert_chunks__", "chunks": chunks}, ensure_ascii=False)},
        ensure_ascii=False,
    ).encode()
    resp = client.invoke(FunctionName=LAMBDA_FUNCTION, InvocationType="RequestResponse", Payload=payload)
    fn_error = resp.get("FunctionError")
    result = json.loads(resp["Payload"].read())
    if fn_error:
        err_msg = result.get("errorMessage", str(result))
        raise RuntimeError(f"Lambda FunctionError: {err_msg[:200]}")
    body = result.get("body", "{}")
    return json.loads(body) if isinstance(body, str) else body


def load_progress() -> set:
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            return set(json.load(f))
    return set()


def save_progress(done: set):
    os.makedirs(os.path.dirname(PROGRESS_FILE), exist_ok=True)
    with open(PROGRESS_FILE, "w") as f:
        json.dump(sorted(done), f)


def process_dir(src_dir: str, openai_key: str, lambda_client) -> tuple:
    done = load_progress()
    files = sorted(glob.glob(os.path.join(src_dir, "*.jsonl")))
    pending = [f for f in files if os.path.basename(f) not in done]
    total_files = len(files)
    print(f"\n[{src_dir}] {len(pending)} pending / {total_files} total")

    total_inserted = 0
    total_errors = 0

    # Accumulate chunks across files, flush in INSERT_BATCH groups
    buffer = []

    def flush_buffer(buf):
        nonlocal total_inserted, total_errors
        if not buf:
            return
        # Embed
        texts = [c.get("text") or c.get("chunk_text", "") for c in buf]
        for attempt in range(3):
            try:
                embeddings = embed_texts(texts, openai_key)
                break
            except Exception as e:
                if attempt == 2:
                    print(f"  [embed] FAILED: {e}", flush=True)
                    total_errors += len(buf)
                    return
                time.sleep(5)

        # Attach embeddings to chunks
        embedded = []
        for chunk, emb in zip(buf, embeddings):
            c = dict(chunk)
            c["embedding"] = emb
            embedded.append(c)

        # Insert via Lambda
        for attempt in range(3):
            try:
                result = invoke_insert(lambda_client, embedded)
                total_inserted += result.get("inserted", 0)
                total_errors += result.get("errors", 0)
                break
            except Exception as e:
                if attempt == 2:
                    print(f"  [insert] FAILED: {e}", flush=True)
                    total_errors += len(buf)
                else:
                    time.sleep(5)
        time.sleep(SLEEP_BETWEEN)

    file_buffer = []  # files currently contributing to buffer
    for idx, path in enumerate(pending, 1):
        fname = os.path.basename(path)
        chunks = None
        for _r in range(3):
            try:
                with open(path, encoding="utf-8") as f:
                    chunks = [json.loads(line) for line in f if line.strip()]
                break
            except Exception as _e:
                if _r == 2:
                    print(f"  [read] SKIP {fname}: {_e}", flush=True)
                    chunks = []
                else:
                    time.sleep(3)

        buffer.extend(chunks)
        file_buffer.append(fname)

        # Flush when buffer is large enough
        while len(buffer) >= INSERT_BATCH:
            flush_buffer(buffer[:INSERT_BATCH])
            buffer = buffer[INSERT_BATCH:]
            # Mark files fully consumed
            for fn in file_buffer:
                done.add(fn)
            file_buffer = []
            save_progress(done)

        if idx % 50 == 0 or idx == len(pending):
            pct = idx / len(pending) * 100
            print(f"  [{idx}/{len(pending)} {pct:.1f}%] inserted={total_inserted:,} errors={total_errors}", flush=True)

    # Flush remainder
    if buffer:
        flush_buffer(buffer)
        for fn in file_buffer:
            done.add(fn)
        save_progress(done)

    return total_inserted, total_errors


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", action="append", required=True, help="chunk dir(s) to process")
    args = parser.parse_args()

    print("Fetching OpenAI key...", flush=True)
    openai_key = get_openai_key()
    lambda_client = boto3.client(
        "lambda", region_name="ap-northeast-1",
        config=Config(read_timeout=300, connect_timeout=10, retries={"max_attempts": 1}),
    )
    # Warm up Lambda (avoid cold-start timeout on first real call)
    print("Warming up Lambda...", flush=True)
    try:
        lambda_client.invoke(
            FunctionName=LAMBDA_FUNCTION, InvocationType="RequestResponse",
            Payload=json.dumps({"httpMethod": "GET", "path": "/health"}).encode()
        )
    except Exception:
        pass
    print("Warm-up done.", flush=True)

    grand_inserted = 0
    grand_errors = 0
    t0 = time.time()
    for src_dir in args.dir:
        ins, err = process_dir(src_dir, openai_key, lambda_client)
        grand_inserted += ins
        grand_errors += err

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed/60:.1f} min  inserted={grand_inserted:,}  errors={grand_errors}", flush=True)


if __name__ == "__main__":
    main()
