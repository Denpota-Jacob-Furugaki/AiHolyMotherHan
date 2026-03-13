#!/usr/bin/env python3
"""
英語 PDF → 日本語翻訳パイプライン

英語テキスト抽出 → GPT-4oで日本語翻訳 → チャンク → 埋め込み → DB保存

Usage:
    python translate_english_to_japanese.py --prefix tparents_pdfs/ --batch 50
    python translate_english_to_japanese.py --resume --batch 50
"""

import argparse
import json
import os
import re
import time
import zipfile
import urllib.request
from typing import List, Dict, Any, Optional

import boto3
import fitz  # PyMuPDF
from openai import OpenAI

# ============================================================
# Configuration
# ============================================================

AWS_REGION = "ap-northeast-1"
S3_BUCKET = "rag-pdf-bucket-221646756615-ap-northeast-1"

CHUNK_SIZE = 1500  # Characters per chunk
CHUNK_OVERLAP = 200
EMBEDDING_MODEL = "text-embedding-3-large"
TRANSLATION_MODEL = "gpt-4o"

PROGRESS_FILE = "/tmp/english_to_japanese_progress.json"
GLOSSARY_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "glossary.json")

# ============================================================
# Clients
# ============================================================

s3_client = None
secrets_client = None
lambda_client = None
openai_client = None
glossary = {}


def init_clients():
    global s3_client, secrets_client, lambda_client, openai_client, glossary
    s3_client = boto3.client("s3", region_name=AWS_REGION)
    secrets_client = boto3.client("secretsmanager", region_name=AWS_REGION)
    lambda_client = boto3.client("lambda", region_name=AWS_REGION)
    
    response = secrets_client.get_secret_value(SecretId="rag/openai-api-key")
    openai_client = OpenAI(api_key=response["SecretString"])
    
    # Load glossary
    glossary = load_glossary()
    print(f"用語集: {len(glossary)} 項目をロード")


def load_glossary() -> Dict[str, str]:
    """Load English to Japanese glossary."""
    if not os.path.exists(GLOSSARY_FILE):
        return {}
    
    with open(GLOSSARY_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    # Combine en_to_ja mappings
    result = {}
    
    # Direct en_to_ja
    if "en_to_ja" in data:
        result.update(data["en_to_ja"])
    
    # Build from other sections
    for section in ["locations_ko_to_en", "titles_ko_to_en", "concepts_ko_to_en", "events_ko_to_en"]:
        if section in data:
            ja_section = section.replace("ko_to_en", "ko_to_ja")
            if ja_section in data:
                # Map English values to Japanese values
                for ko_key, en_val in data[section].items():
                    if ko_key in data[ja_section]:
                        result[en_val] = data[ja_section][ko_key]
    
    return result


def get_glossary_prompt() -> str:
    """Generate glossary instructions for translation."""
    if not glossary:
        return ""
    
    lines = ["以下の用語は指定された日本語訳を使用してください："]
    for en, ja in glossary.items():
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
        "processed_pdfs": [],
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
# PDF Processing
# ============================================================

def list_pdfs(prefix: str) -> List[str]:
    """List all PDFs with given prefix."""
    paginator = s3_client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].lower().endswith(".pdf"):
                keys.append(obj["Key"])
    return sorted(keys)


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF using PyMuPDF."""
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    text_parts = []
    
    for page_num in range(len(doc)):
        page = doc[page_num]
        text = page.get_text()
        if text.strip():
            text_parts.append(text)
    
    doc.close()
    return "\n\n".join(text_parts)


def detect_language(text: str) -> str:
    """Detect language based on character patterns."""
    hangul = sum(1 for c in text if '\uac00' <= c <= '\ud7af')
    hiragana = sum(1 for c in text if '\u3040' <= c <= '\u309f')
    katakana = sum(1 for c in text if '\u30a0' <= c <= '\u30ff')
    
    total = len(text)
    if total == 0:
        return "en"
    
    if hangul / total > 0.1:
        return "ko"
    elif (hiragana + katakana) / total > 0.05:
        return "ja"
    else:
        return "en"


def extract_metadata_from_text(text: str, filename: str) -> Dict[str, Any]:
    """Extract metadata from text content."""
    metadata = {
        "original_language": "en",
        "translation_language": "ja",
        "source_type": "speech"  # Default
    }
    
    # Try to extract date
    date_patterns = [
        r"(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})",
        r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(\d{4})",
        r"(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})"
    ]
    
    for pattern in date_patterns:
        match = re.search(pattern, text[:2000], re.IGNORECASE)
        if match:
            metadata["speech_date"] = match.group(0)
            break
    
    # Try to extract location from common patterns
    location_patterns = [
        r"(?:at|in)\s+([A-Z][a-zA-Z\s]+(?:Center|Church|Hall|Palace|Hotel|Building))",
        r"Belvedere|East Garden|Tarrytown|Korea|Seoul|Washington|New York"
    ]
    
    for pattern in location_patterns:
        match = re.search(pattern, text[:2000], re.IGNORECASE)
        if match:
            metadata["speech_location"] = match.group(0).strip()
            break
    
    # Detect if it's a book based on filename
    book_patterns = ["Divine Principle", "Principle", "Exposition", "천성경", "天聖経"]
    for bp in book_patterns:
        if bp.lower() in filename.lower():
            metadata["source_type"] = "book"
            break
    
    # Clean up filename for title
    title = filename.replace(".pdf", "").replace("_", " ").replace("-", " ")
    metadata["speech_title_en"] = title
    
    return metadata


# ============================================================
# Translation
# ============================================================

def translate_text_to_japanese(text: str, metadata: Dict[str, Any]) -> str:
    """Translate English text to Japanese using GPT-4o."""
    glossary_prompt = get_glossary_prompt()
    
    prompt = f"""以下の英語のテキストを日本語に翻訳してください。

{glossary_prompt}

【翻訳ルール】
1. 宗教用語・専門用語は正確に翻訳
2. 「True Father」→「真のお父様」
3. 「True Mother」→「真のお母様」
4. 「True Parents」→「真の父母様」
5. 「Heavenly Parent」→「天の父母様」
6. 「God」→「神様」
7. 固有名詞は用語集に従う
8. 自然で読みやすい日本語に

【テキスト】
{text}

【日本語訳】"""

    try:
        response = openai_client.chat.completions.create(
            model=TRANSLATION_MODEL,
            messages=[
                {"role": "system", "content": "あなたは統一運動の専門翻訳者です。宗教文献を正確かつ自然な日本語に翻訳します。"},
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
            # Return empty embeddings for this batch
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
    urllib.request.urlretrieve(current["Code"]["Location"], "/tmp/mhc-ja.zip")
    
    # Create temp Lambda with store code
    with zipfile.ZipFile("/tmp/mhc-ja.zip", "r") as zin:
        with zipfile.ZipFile("/tmp/mhc-ja-store.zip", "w") as zout:
            for item in zin.infolist():
                if item.filename != "lambda_function.py":
                    zout.writestr(item, zin.read(item.filename))
            zout.writestr("lambda_function.py", store_code)
    
    with open("/tmp/mhc-ja-store.zip", "rb") as f:
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
    with open("/tmp/mhc-ja.zip", "rb") as f:
        lambda_client.update_function_code(FunctionName="mini-han-chat", ZipFile=f.read())
    
    return stored


# ============================================================
# Main Processing
# ============================================================

def process_single_pdf(s3_key: str) -> Dict[str, Any]:
    """Process a single PDF: extract, translate, embed, store."""
    filename = os.path.basename(s3_key)
    result = {"s3_key": s3_key, "filename": filename, "success": False, "chunks": 0}
    
    try:
        # Download PDF
        print(f"    ダウンロード中...", flush=True)
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        pdf_bytes = response["Body"].read()
        
        # Extract text
        print(f"    テキスト抽出中...", flush=True)
        text = extract_text_from_pdf(pdf_bytes)
        
        if len(text) < 200:
            result["error"] = "テキストが短すぎる"
            return result
        
        # Check if already Japanese
        lang = detect_language(text)
        if lang == "ja":
            result["error"] = "既に日本語"
            return result
        
        if lang == "ko":
            result["error"] = "韓国語（別パイプライン）"
            return result
        
        # Extract metadata
        metadata = extract_metadata_from_text(text, filename)
        
        # Translate to Japanese (in chunks to avoid token limits)
        print(f"    日本語に翻訳中...", flush=True)
        
        # Split into translation chunks (larger than storage chunks)
        translation_chunks = []
        for i in range(0, len(text), 3000):
            chunk = text[i:i+3000]
            if len(chunk) > 100:
                translation_chunks.append(chunk)
        
        translated_parts = []
        for i, chunk in enumerate(translation_chunks):
            print(f"      翻訳 {i+1}/{len(translation_chunks)}...", flush=True)
            translated = translate_text_to_japanese(chunk, metadata)
            if translated:
                translated_parts.append(translated)
            time.sleep(0.5)  # Rate limiting
        
        if not translated_parts:
            result["error"] = "翻訳失敗"
            return result
        
        full_translation = "\n\n".join(translated_parts)
        
        # Chunk the translated text
        print(f"    チャンク作成中...", flush=True)
        chunks = chunk_text(full_translation)
        
        if not chunks:
            result["error"] = "チャンク作成失敗"
            return result
        
        # Embed
        print(f"    埋め込み生成中...", flush=True)
        embeddings = get_embeddings(chunks)
        
        # Prepare data
        # Use modified s3_key to indicate Japanese translation
        ja_s3_key = f"[JA] {s3_key}"
        display_name = f"[JA] {metadata.get('speech_title_en', filename.replace('.pdf', ''))}"
        
        chunks_data = []
        for idx, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
            chunk_metadata = metadata.copy()
            chunk_metadata["language"] = "ja"
            
            chunks_data.append({
                "s3_key": ja_s3_key,
                "filename": display_name,
                "chunk_index": idx,
                "chunk_text": chunk,
                "embedding": embedding,
                "metadata": chunk_metadata
            })
        
        # Store
        print(f"    DB保存中...", flush=True)
        stored = store_chunks_via_lambda(chunks_data)
        
        result["success"] = True
        result["chunks"] = stored
        result["original_length"] = len(text)
        result["translated_length"] = len(full_translation)
        
    except Exception as e:
        result["error"] = str(e)
    
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--prefix", type=str, default="tparents_pdfs/", help="S3 prefix")
    parser.add_argument("--start", type=int, default=0, help="Start index")
    parser.add_argument("--batch", type=int, default=50, help="Batch size")
    parser.add_argument("--resume", action="store_true", help="Resume from progress")
    args = parser.parse_args()
    
    print("=" * 60)
    print("英語 PDF → 日本語翻訳パイプライン")
    print("=" * 60)
    
    init_clients()
    
    # Load progress
    progress = load_progress() if args.resume else {
        "processed_pdfs": [],
        "success": 0,
        "errors": 0,
        "total_chunks": 0,
        "status": "running"
    }
    progress["status"] = "running"
    save_progress(progress)
    
    # List PDFs
    all_pdfs = list_pdfs(args.prefix)
    print(f"Total PDFs: {len(all_pdfs)}")
    
    # Filter already processed
    to_process = [k for k in all_pdfs if k not in progress["processed_pdfs"]]
    to_process = to_process[args.start:args.start + args.batch]
    print(f"To process: {len(to_process)}")
    
    # Process
    for i, s3_key in enumerate(to_process):
        filename = os.path.basename(s3_key)
        print(f"\n[{i+1}/{len(to_process)}] {filename}", flush=True)
        
        result = process_single_pdf(s3_key)
        
        progress["processed_pdfs"].append(s3_key)
        if result["success"]:
            progress["success"] += 1
            progress["total_chunks"] += result["chunks"]
            print(f"    ✓ {result['chunks']} チャンク保存", flush=True)
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
