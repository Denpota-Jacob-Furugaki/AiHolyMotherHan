#!/usr/bin/env python3
"""
原理講論 3言語処理パイプライン

韓国語テキストから:
1. 韓国語原文 → DB保存
2. 英語翻訳（既存）→ DB保存
3. 韓国語 → 日本語翻訳 → DB保存

Usage:
    python process_divine_principle.py --batch 10
    python process_divine_principle.py --resume
"""

import argparse
import json
import os
import re
import time
import zipfile
import urllib.request
from pathlib import Path
from typing import List, Dict, Any, Tuple

import boto3
from openai import OpenAI

# ============================================================
# Configuration
# ============================================================

AWS_REGION = "ap-northeast-1"
S3_BUCKET = "rag-pdf-bucket-221646756615-ap-northeast-1"

CHUNK_SIZE = 1500
CHUNK_OVERLAP = 200
EMBEDDING_MODEL = "text-embedding-3-large"
TRANSLATION_MODEL = "gpt-4o"

# Paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent
CHAPTERS_DIR = PROJECT_DIR / "tparents_pdfs" / "principle_ko" / "chapters"
METADATA_FILE = PROJECT_DIR / "tparents_pdfs" / "principle_ko" / "chapters_metadata.json"
GLOSSARY_FILE = PROJECT_DIR / "data" / "glossary.json"
PROGRESS_FILE = Path("/tmp/divine_principle_progress.json")

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
    """Load Korean to Japanese glossary."""
    if not GLOSSARY_FILE.exists():
        return {}
    
    with open(GLOSSARY_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    result = {}
    
    # Korean to Japanese mappings
    for section in ["locations_ko_to_ja", "titles_ko_to_ja", "concepts_ko_to_ja", "events_ko_to_ja"]:
        if section in data:
            result.update(data[section])
    
    return result


def get_glossary_prompt() -> str:
    """Generate glossary instructions for translation."""
    if not glossary:
        return ""
    
    lines = ["以下の用語は指定された日本語訳を使用してください："]
    for ko, ja in list(glossary.items())[:30]:  # Limit to prevent token overflow
        lines.append(f"  - {ko} → {ja}")
    return "\n".join(lines)


# ============================================================
# Progress Management
# ============================================================

def load_progress() -> Dict[str, Any]:
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {
        "processed_chapters": [],
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
# Text Processing
# ============================================================

def load_chapter_metadata() -> Dict[str, Any]:
    """Load chapter metadata."""
    if not METADATA_FILE.exists():
        return {}
    
    with open(METADATA_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    # Index by URL path
    return {item["url_path"]: item for item in data}


def parse_chapter_file(filepath: Path) -> Dict[str, Any]:
    """Parse a chapter file and extract Korean and English content."""
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()
    
    lines = content.split("\n")
    
    # Extract title from first line
    title = ""
    if lines and lines[0].startswith("Title:"):
        title = lines[0].replace("Title:", "").strip()
    
    # Extract URL
    url = ""
    if len(lines) > 1 and lines[1].startswith("URL:"):
        url = lines[1].replace("URL:", "").strip()
    
    # Find content start (after the header)
    content_start = 0
    for i, line in enumerate(lines):
        if line.startswith("=" * 10):
            content_start = i + 1
            break
    
    # Split content into Korean and English
    korean_parts = []
    english_parts = []
    
    # Filter out UI elements and navigation
    skip_patterns = [
        "Language", "한국어", "English", "슬라이드", "Highlights",
        "빨간색", "파란색", "노란색", "(Clear All Filters)",
        "목차", "←", "→"
    ]
    
    current_text = []
    for line in lines[content_start:]:
        line = line.strip()
        
        # Skip empty lines and UI elements
        if not line or any(line.startswith(p) for p in skip_patterns):
            continue
        
        # Detect Korean (contains Hangul)
        has_hangul = any('\uac00' <= c <= '\ud7af' for c in line)
        # Detect English (mostly ASCII letters)
        ascii_ratio = sum(1 for c in line if c.isascii()) / max(len(line), 1)
        
        if has_hangul and ascii_ratio < 0.7:
            korean_parts.append(line)
        elif ascii_ratio > 0.7 and len(line) > 10:
            english_parts.append(line)
    
    return {
        "title": title,
        "url": url,
        "korean_text": "\n\n".join(korean_parts),
        "english_text": "\n\n".join(english_parts),
        "filename": filepath.name
    }


def translate_to_japanese(korean_text: str, title: str) -> str:
    """Translate Korean text to Japanese using GPT-4o."""
    glossary_prompt = get_glossary_prompt()
    
    # Translate in chunks if text is too long
    max_chars = 6000
    if len(korean_text) > max_chars:
        parts = []
        for i in range(0, len(korean_text), max_chars):
            chunk = korean_text[i:i+max_chars]
            translated = _translate_chunk(chunk, title, glossary_prompt)
            if translated:
                parts.append(translated)
            time.sleep(0.5)
        return "\n\n".join(parts)
    else:
        return _translate_chunk(korean_text, title, glossary_prompt)


def _translate_chunk(text: str, title: str, glossary_prompt: str) -> str:
    """Translate a single chunk."""
    prompt = f"""以下の韓国語の原理講論（Divine Principle）のテキストを日本語に翻訳してください。

{glossary_prompt}

【翻訳ルール】
1. 宗教用語・専門用語は正確に翻訳
2. 「하나님」→「神様」
3. 「참 부모님」→「真の父母様」
4. 「참 아버지」→「真のお父様」
5. 「참 어머니」→「真のお母様」
6. 「원리」→「原理」
7. 「타락」→「堕落」
8. 「복귀」→「復帰」
9. 「탕감」→「蕩減」
10. 「창조」→「創造」
11. 章のタイトル: 「{title}」
12. 自然で読みやすい日本語に

【韓国語テキスト】
{text}

【日本語訳】"""

    try:
        response = openai_client.chat.completions.create(
            model=TRANSLATION_MODEL,
            messages=[
                {"role": "system", "content": "あなたは統一原理（Divine Principle）の専門翻訳者です。韓国語の原理講論を正確かつ自然な日本語に翻訳します。"},
                {"role": "user", "content": prompt}
            ],
            max_tokens=4000,
            temperature=0.3
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"    翻訳エラー: {e}")
        return ""


def chunk_text(text: str) -> List[str]:
    """Split text into overlapping chunks."""
    text = text.replace("\x00", "")
    chunks = []
    
    # Split by paragraphs first
    paragraphs = text.split("\n\n")
    current_chunk = ""
    
    for para in paragraphs:
        para = para.strip()
        if not para:
            continue
        
        if len(current_chunk) + len(para) < CHUNK_SIZE:
            current_chunk += para + "\n\n"
        else:
            if current_chunk and len(current_chunk) > 100:
                chunks.append(current_chunk.strip())
            current_chunk = para + "\n\n"
    
    if current_chunk and len(current_chunk) > 100:
        chunks.append(current_chunk.strip())
    
    return chunks


def get_embeddings(texts: List[str]) -> List[List[float]]:
    """Get embeddings for texts."""
    embeddings = []
    for i in range(0, len(texts), 20):
        batch = texts[i:i+20]
        try:
            response = openai_client.embeddings.create(model=EMBEDDING_MODEL, input=batch)
            for item in response.data:
                embeddings.append(item.embedding)
        except Exception as e:
            print(f"    Embedding エラー: {e}")
            for _ in batch:
                embeddings.append([0.0] * 3072)
    return embeddings


# ============================================================
# Database Storage
# ============================================================

def store_chunks_via_lambda(chunks_data: List[Dict]) -> int:
    """Store chunks in DB using Lambda."""
    
    store_code = '''
import json, os, boto3, psycopg2
from psycopg2.extras import execute_values

secrets_client = boto3.client("secretsmanager")

def lambda_handler(event, context):
    try:
        secret_arn = os.environ.get("DB_SECRET_ARN")
        response = secrets_client.get_secret_value(SecretId=secret_arn)
        secret = json.loads(response["SecretString"])
        
        conn = psycopg2.connect(
            host=os.environ.get("DB_HOST"),
            port=os.environ.get("DB_PORT", "5432"),
            dbname=os.environ.get("DB_NAME", "ragdb"),
            user=os.environ.get("DB_USER", "ragadmin"),
            password=secret.get("password", secret.get("ragadmin")),
        )
        
        chunks = event.get("chunks", [])
        
        with conn.cursor() as cursor:
            values = []
            for chunk in chunks:
                embedding_str = "[" + ",".join(map(str, chunk["embedding"])) + "]"
                metadata_str = json.dumps(chunk["metadata"])
                values.append((
                    chunk["s3_key"],
                    chunk["filename"],
                    chunk["chunk_index"],
                    chunk["chunk_text"],
                    embedding_str,
                    metadata_str
                ))
            
            insert_query = """
                INSERT INTO documents (s3_key, filename, chunk_index, chunk_text, embedding, metadata)
                VALUES %s
                ON CONFLICT (s3_key, chunk_index) 
                DO UPDATE SET 
                    chunk_text = EXCLUDED.chunk_text,
                    embedding = EXCLUDED.embedding,
                    metadata = EXCLUDED.metadata
            """
            execute_values(cursor, insert_query, values)
            conn.commit()
        
        conn.close()
        return {"statusCode": 200, "body": json.dumps({"stored": len(chunks)})}
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
'''
    
    # Get current Lambda code
    current = lambda_client.get_function(FunctionName="mini-han-chat")
    urllib.request.urlretrieve(current["Code"]["Location"], "/tmp/mhc-dp.zip")
    
    # Create temp Lambda with store code
    with zipfile.ZipFile("/tmp/mhc-dp.zip", "r") as zin:
        with zipfile.ZipFile("/tmp/mhc-dp-store.zip", "w") as zout:
            for item in zin.infolist():
                if item.filename != "lambda_function.py":
                    zout.writestr(item, zin.read(item.filename))
            zout.writestr("lambda_function.py", store_code)
    
    with open("/tmp/mhc-dp-store.zip", "rb") as f:
        lambda_client.update_function_code(FunctionName="mini-han-chat", ZipFile=f.read())
    
    time.sleep(8)
    
    # Store in batches
    stored = 0
    batch_size = 30
    for i in range(0, len(chunks_data), batch_size):
        batch = chunks_data[i:i+batch_size]
        response = lambda_client.invoke(
            FunctionName="mini-han-chat",
            InvocationType="RequestResponse",
            Payload=json.dumps({"chunks": batch})
        )
        result = json.loads(response["Payload"].read())
        if result.get("statusCode") == 200:
            body = json.loads(result.get("body", "{}"))
            stored += body.get("stored", 0)
        time.sleep(0.3)
    
    # Restore original Lambda
    with open("/tmp/mhc-dp.zip", "rb") as f:
        lambda_client.update_function_code(FunctionName="mini-han-chat", ZipFile=f.read())
    
    return stored


# ============================================================
# Main Processing
# ============================================================

def process_chapter(chapter_path: Path) -> Dict[str, Any]:
    """Process a single chapter in all three languages."""
    result = {
        "filename": chapter_path.name,
        "success": False,
        "chunks": {"ko": 0, "en": 0, "ja": 0}
    }
    
    try:
        # Parse chapter
        print(f"    解析中...", flush=True)
        chapter = parse_chapter_file(chapter_path)
        
        if len(chapter["korean_text"]) < 100:
            result["error"] = "韓国語テキストが短すぎる"
            return result
        
        title = chapter["title"]
        
        # Base metadata
        base_metadata = {
            "source_type": "book",
            "book_name": "原理講論",
            "book_name_en": "Exposition of the Divine Principle",
            "book_name_ko": "원리강론",
            "chapter_title": title,
            "author": "文鮮明",
            "url": chapter["url"]
        }
        
        all_chunks_data = []
        
        # 1. Process Korean text
        print(f"    韓国語処理中...", flush=True)
        ko_chunks = chunk_text(chapter["korean_text"])
        if ko_chunks:
            ko_embeddings = get_embeddings(ko_chunks)
            for idx, (chunk, embedding) in enumerate(zip(ko_chunks, ko_embeddings)):
                metadata = base_metadata.copy()
                metadata["language"] = "ko"
                metadata["original_language"] = "ko"
                
                all_chunks_data.append({
                    "s3_key": f"divine_principle/ko/{chapter_path.stem}",
                    "filename": f"[KO] 원리강론 - {title}",
                    "chunk_index": idx,
                    "chunk_text": chunk,
                    "embedding": embedding,
                    "metadata": metadata
                })
            result["chunks"]["ko"] = len(ko_chunks)
        
        # 2. Process English text (if available)
        if len(chapter["english_text"]) > 200:
            print(f"    英語処理中...", flush=True)
            en_chunks = chunk_text(chapter["english_text"])
            if en_chunks:
                en_embeddings = get_embeddings(en_chunks)
                for idx, (chunk, embedding) in enumerate(zip(en_chunks, en_embeddings)):
                    metadata = base_metadata.copy()
                    metadata["language"] = "en"
                    metadata["original_language"] = "ko"
                    
                    all_chunks_data.append({
                        "s3_key": f"divine_principle/en/{chapter_path.stem}",
                        "filename": f"[EN] Exposition of Divine Principle - {title}",
                        "chunk_index": idx,
                        "chunk_text": chunk,
                        "embedding": embedding,
                        "metadata": metadata
                    })
                result["chunks"]["en"] = len(en_chunks)
        
        # 3. Translate to Japanese
        print(f"    日本語翻訳中...", flush=True)
        ja_text = translate_to_japanese(chapter["korean_text"], title)
        if ja_text and len(ja_text) > 100:
            ja_chunks = chunk_text(ja_text)
            if ja_chunks:
                ja_embeddings = get_embeddings(ja_chunks)
                for idx, (chunk, embedding) in enumerate(zip(ja_chunks, ja_embeddings)):
                    metadata = base_metadata.copy()
                    metadata["language"] = "ja"
                    metadata["original_language"] = "ko"
                    
                    all_chunks_data.append({
                        "s3_key": f"divine_principle/ja/{chapter_path.stem}",
                        "filename": f"[JA] 原理講論 - {title}",
                        "chunk_index": idx,
                        "chunk_text": chunk,
                        "embedding": embedding,
                        "metadata": metadata
                    })
                result["chunks"]["ja"] = len(ja_chunks)
        
        # Store all chunks
        if all_chunks_data:
            print(f"    DB保存中 ({len(all_chunks_data)} チャンク)...", flush=True)
            stored = store_chunks_via_lambda(all_chunks_data)
            result["success"] = True
            result["total_stored"] = stored
        else:
            result["error"] = "チャンクなし"
        
    except Exception as e:
        result["error"] = str(e)
    
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", type=int, default=10, help="Batch size")
    parser.add_argument("--resume", action="store_true", help="Resume from progress")
    args = parser.parse_args()
    
    print("=" * 60)
    print("原理講論 3言語処理パイプライン")
    print("=" * 60)
    
    init_clients()
    
    # Load progress
    progress = load_progress() if args.resume else {
        "processed_chapters": [],
        "success": 0,
        "errors": 0,
        "total_chunks": 0,
        "status": "running"
    }
    progress["status"] = "running"
    save_progress(progress)
    
    # Get all chapter files
    chapter_files = sorted(CHAPTERS_DIR.glob("*.txt"))
    print(f"Total chapters: {len(chapter_files)}")
    
    # Filter already processed
    to_process = [f for f in chapter_files if f.name not in progress["processed_chapters"]]
    to_process = to_process[:args.batch]
    print(f"To process: {len(to_process)}")
    
    # Process
    for i, chapter_path in enumerate(to_process):
        print(f"\n[{i+1}/{len(to_process)}] {chapter_path.name}", flush=True)
        
        result = process_chapter(chapter_path)
        
        progress["processed_chapters"].append(chapter_path.name)
        if result["success"]:
            progress["success"] += 1
            total = sum(result["chunks"].values())
            progress["total_chunks"] += total
            print(f"    ✓ KO:{result['chunks']['ko']} EN:{result['chunks']['en']} JA:{result['chunks']['ja']}", flush=True)
        else:
            progress["errors"] += 1
            print(f"    ✗ {result.get('error', 'unknown')}", flush=True)
        
        save_progress(progress)
    
    progress["status"] = "completed"
    save_progress(progress)
    
    print("\n" + "=" * 60)
    print("完了")
    print(f"成功: {progress['success']}")
    print(f"エラー: {progress['errors']}")
    print(f"合計チャンク: {progress['total_chunks']}")


if __name__ == "__main__":
    main()
