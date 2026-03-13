#!/usr/bin/env python3
"""
韓国語 PDF → 日本語翻訳パイプライン

既に英語に翻訳されたチャンクを日本語に翻訳してDBに追加

Usage:
    python translate_korean_to_japanese.py --batch 100
    python translate_korean_to_japanese.py --resume
"""

import argparse
import json
import os
import time
from typing import List, Dict, Any

import boto3
from openai import OpenAI

# ============================================================
# Configuration
# ============================================================

AWS_REGION = "ap-northeast-1"
EMBEDDING_MODEL = "text-embedding-3-large"
TRANSLATION_MODEL = "gpt-4o"

PROGRESS_FILE = "/tmp/korean_to_japanese_progress.json"
GLOSSARY_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "glossary.json")

# ============================================================
# Clients
# ============================================================

secrets_client = None
lambda_client = None
openai_client = None
glossary = {}


def init_clients():
    global secrets_client, lambda_client, openai_client, glossary
    secrets_client = boto3.client("secretsmanager", region_name=AWS_REGION)
    lambda_client = boto3.client("lambda", region_name=AWS_REGION)
    
    response = secrets_client.get_secret_value(SecretId="rag/openai-api-key")
    openai_client = OpenAI(api_key=response["SecretString"])
    
    glossary = load_glossary()
    print(f"用語集: {len(glossary)} 項目をロード")


def load_glossary() -> Dict[str, str]:
    """Load English to Japanese glossary."""
    if not os.path.exists(GLOSSARY_FILE):
        return {}
    
    with open(GLOSSARY_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    result = {}
    if "en_to_ja" in data:
        result.update(data["en_to_ja"])
    
    # Build from ko_to_en and ko_to_ja
    for section in ["locations_ko_to_en", "titles_ko_to_en", "concepts_ko_to_en", "events_ko_to_en"]:
        if section in data:
            ja_section = section.replace("ko_to_en", "ko_to_ja")
            if ja_section in data:
                for ko_key, en_val in data[section].items():
                    if ko_key in data[ja_section]:
                        result[en_val] = data[ja_section][ko_key]
    
    return result


def get_glossary_prompt() -> str:
    """Generate glossary instructions for translation."""
    if not glossary:
        return ""
    
    lines = ["以下の用語は指定された日本語訳を使用してください："]
    for en, ja in list(glossary.items())[:30]:
        lines.append(f"  - {en} → {ja}")
    return "\n".join(lines)


# ============================================================
# Progress Management
# ============================================================

def load_progress() -> Dict[str, Any]:
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {
        "processed_keys": [],
        "success": 0,
        "errors": 0,
        "total_chunks": 0,
        "status": "idle"
    }


def save_progress(progress: Dict[str, Any]):
    progress["last_update"] = time.strftime("%Y-%m-%d %H:%M:%S")
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2, ensure_ascii=False)


# ============================================================
# Database Operations (via dedicated Lambda)
# ============================================================

STORAGE_LAMBDA = "rag-storage-helper"

def get_korean_translated_chunks(offset: int = 0, limit: int = 100) -> List[Dict]:
    """Get chunks that were translated from Korean (have original_language=ko)."""
    try:
        response = lambda_client.invoke(
            FunctionName=STORAGE_LAMBDA,
            InvocationType="RequestResponse",
            Payload=json.dumps({
                "action": "query",
                "offset": offset,
                "limit": limit,
                "prefix": "translated/malsmCollection615Korean_pdfs/"
            })
        )
        result = json.loads(response["Payload"].read())
        
        if result.get("statusCode") == 200:
            body = json.loads(result.get("body", "{}"))
            return body.get("chunks", []), body.get("total", 0)
        else:
            print(f"Query error: {result}")
            return [], 0
    except Exception as e:
        print(f"Lambda invoke error: {e}")
        return [], 0


def translate_to_japanese(text: str) -> str:
    """Translate English text to Japanese."""
    glossary_prompt = get_glossary_prompt()
    
    prompt = f"""以下の英語テキストを日本語に翻訳してください。
これは韓国語から英語に翻訳された統一教会の御言葉です。

{glossary_prompt}

【翻訳ルール】
1. 「True Father」→「真のお父様」
2. 「True Mother」→「真のお母様」
3. 「True Parents」→「真の父母様」
4. 「Heavenly Parent」→「天の父母様」
5. 「God」→「神様」
6. 宗教用語は正確に翻訳
7. 自然で読みやすい日本語に

【テキスト】
{text}

【日本語訳】"""

    try:
        response = openai_client.chat.completions.create(
            model=TRANSLATION_MODEL,
            messages=[
                {"role": "system", "content": "あなたは統一運動の専門翻訳者です。"},
                {"role": "user", "content": prompt}
            ],
            max_tokens=2000,
            temperature=0.3
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"    翻訳エラー: {e}")
        return ""


def get_embedding(text: str) -> List[float]:
    """Get embedding for text."""
    try:
        response = openai_client.embeddings.create(model=EMBEDDING_MODEL, input=[text])
        return response.data[0].embedding
    except Exception as e:
        print(f"    Embedding エラー: {e}")
        return [0.0] * 3072


def store_chunk_via_lambda(chunk_data: Dict) -> bool:
    """Store a single chunk in DB using dedicated storage Lambda."""
    try:
        response = lambda_client.invoke(
            FunctionName=STORAGE_LAMBDA,
            InvocationType="RequestResponse",
            Payload=json.dumps({"action": "store", "chunks": [chunk_data]})
        )
        result = json.loads(response["Payload"].read())
        return result.get("statusCode") == 200
    except Exception as e:
        print(f"    Storage error: {e}")
        return False


# ============================================================
# Main Processing
# ============================================================

def process_batch(chunks: List[Dict], progress: Dict) -> int:
    """Process a batch of chunks - translate to Japanese and store."""
    stored = 0
    
    for i, chunk in enumerate(chunks):
        key = f"{chunk['s3_key']}:{chunk['chunk_index']}"
        if key in progress["processed_keys"]:
            continue
        
        print(f"  [{i+1}/{len(chunks)}] {chunk['filename'][:50]}...", flush=True)
        
        # Translate to Japanese
        ja_text = translate_to_japanese(chunk["chunk_text"])
        if not ja_text:
            progress["errors"] += 1
            progress["processed_keys"].append(key)
            save_progress(progress)
            continue
        
        # Get embedding
        embedding = get_embedding(ja_text)
        
        # Prepare metadata
        metadata = chunk.get("metadata", {})
        if isinstance(metadata, str):
            metadata = json.loads(metadata)
        metadata["language"] = "ja"
        metadata["translated_from"] = "en"
        
        # Create Japanese version
        ja_s3_key = chunk["s3_key"].replace("translated/", "translated_ja/")
        ja_filename = chunk["filename"].replace("[EN-KO]", "[JA-KO]")
        
        ja_chunk = {
            "s3_key": ja_s3_key,
            "filename": ja_filename,
            "chunk_index": chunk["chunk_index"],
            "chunk_text": ja_text,
            "embedding": embedding,
            "metadata": metadata
        }
        
        # Store
        if store_chunk_via_lambda(ja_chunk):
            stored += 1
            progress["success"] += 1
            progress["total_chunks"] += 1
        else:
            progress["errors"] += 1
        
        progress["processed_keys"].append(key)
        save_progress(progress)
        
        time.sleep(0.3)  # Rate limiting
    
    return stored


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", type=int, default=100, help="Batch size")
    parser.add_argument("--resume", action="store_true", help="Resume from progress")
    args = parser.parse_args()
    
    print("=" * 60)
    print("韓国語翻訳 → 日本語パイプライン")
    print("(既存の英語翻訳チャンクを日本語に変換)")
    print("=" * 60)
    
    init_clients()
    
    progress = load_progress() if args.resume else {
        "processed_keys": [],
        "success": 0,
        "errors": 0,
        "total_chunks": 0,
        "status": "running"
    }
    progress["status"] = "running"
    save_progress(progress)
    
    # Get chunks to process - use processed count as offset
    offset = len(progress.get("processed_keys", []))
    print(f"DBからチャンクを取得中... (offset: {offset})")
    chunks, total = get_korean_translated_chunks(offset, args.batch)
    remaining = total - offset
    print(f"残りチャンク: {remaining}/{total}")
    print(f"今回処理: {len(chunks)}")
    
    if not chunks:
        print("処理するチャンクがありません")
        return
    
    # Process
    stored = process_batch(chunks, progress)
    
    progress["status"] = "completed"
    save_progress(progress)
    
    print("\n" + "=" * 60)
    print("完了")
    print(f"成功: {progress['success']}")
    print(f"エラー: {progress['errors']}")
    print(f"合計チャンク: {progress['total_chunks']}")


if __name__ == "__main__":
    main()
