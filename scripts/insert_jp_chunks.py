"""
insert_jp_chunks.py
Read JSONL files from output/malsm_jp_chunks/ and insert into Aurora DB via Lambda.

Usage:
    python3 scripts/insert_jp_chunks.py [--dir output/malsm_jp_chunks] [--batch 5]

The Lambda __insert_chunks__ handler generates embeddings and inserts each chunk.
Batch size of 5 chunks keeps Lambda payload well within limits and avoids timeouts.
"""

import argparse
import glob
import json
import os
import time
import warnings

warnings.filterwarnings("ignore")
import boto3

LAMBDA_FUNCTION = "mini-han-chat"
DEFAULT_BATCH = 5
OUTPUT_DIR = "output/malsm_jp_chunks"
PROGRESS_FILE = "output/insert_progress.json"


def load_progress() -> set:
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            return set(json.load(f))
    return set()


def save_progress(done: set):
    os.makedirs(os.path.dirname(PROGRESS_FILE), exist_ok=True)
    with open(PROGRESS_FILE, "w") as f:
        json.dump(sorted(done), f)


def invoke_insert(client, chunks: list) -> dict:
    payload = json.dumps({
        "body": json.dumps({"message": "__insert_chunks__", "chunks": chunks}, ensure_ascii=False)
    }).encode()
    resp = client.invoke(
        FunctionName=LAMBDA_FUNCTION,
        InvocationType="RequestResponse",
        Payload=payload,
    )
    result = json.loads(resp["Payload"].read())
    body = result.get("body", "{}")
    return json.loads(body) if isinstance(body, str) else body


def process_file(client, jsonl_path: str, batch_size: int) -> tuple[int, int]:
    with open(jsonl_path, encoding="utf-8") as f:
        chunks = [json.loads(line) for line in f if line.strip()]

    total_inserted = 0
    total_errors = 0
    for i in range(0, len(chunks), batch_size):
        batch = chunks[i:i + batch_size]
        for attempt in range(3):
            try:
                result = invoke_insert(client, batch)
                total_inserted += result.get("inserted", 0)
                total_errors += result.get("errors", 0)
                print(f"    batch {i//batch_size + 1}: inserted={result.get('inserted',0)} errors={result.get('errors',0)}")
                break
            except Exception as e:
                if attempt == 2:
                    print(f"    batch {i//batch_size + 1}: FAILED after 3 attempts — {e}")
                    total_errors += len(batch)
                else:
                    print(f"    batch {i//batch_size + 1}: error (retry {attempt+1}): {e}")
                    time.sleep(5)
        time.sleep(0.5)

    return total_inserted, total_errors


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", default=OUTPUT_DIR)
    parser.add_argument("--batch", type=int, default=DEFAULT_BATCH)
    args = parser.parse_args()

    client = boto3.client("lambda", region_name="ap-northeast-1")
    done = load_progress()

    jsonl_files = sorted(
        glob.glob(os.path.join(args.dir, "*.jsonl")),
        key=lambda p: int(os.path.splitext(os.path.basename(p))[0])
        if os.path.splitext(os.path.basename(p))[0].isdigit() else 0
    )

    pending = [f for f in jsonl_files if os.path.basename(f) not in done]
    print(f"Files to insert: {len(pending)} (already done: {len(done)})")

    total_inserted = 0
    total_errors = 0
    for idx, path in enumerate(pending, 1):
        name = os.path.basename(path)
        print(f"[{idx}/{len(pending)}] {name}")
        ins, err = process_file(client, path, args.batch)
        total_inserted += ins
        total_errors += err
        done.add(name)
        save_progress(done)
        print(f"  → inserted={ins} errors={err} (cumulative: {total_inserted:,})")

    print(f"\nFinished. Total inserted: {total_inserted:,}  errors: {total_errors:,}")


if __name__ == "__main__":
    main()
