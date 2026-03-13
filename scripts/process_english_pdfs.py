#!/usr/bin/env python3
"""
英語 PDF 処理パイプライン（翻訳不要）

直接テキスト抽出 → チャンク → 埋め込み → DB保存

Usage:
    python process_english_pdfs.py --prefix tparents_pdfs/ --batch 100
"""

import argparse
import json
import os
import time
import zipfile
import urllib.request
from typing import List, Dict, Any

import boto3
import fitz  # PyMuPDF
from openai import OpenAI

# ============================================================
# Configuration
# ============================================================

AWS_REGION = "ap-northeast-1"
S3_BUCKET = "rag-pdf-bucket-221646756615-ap-northeast-1"

CHUNK_SIZE = 1000
CHUNK_OVERLAP = 200
EMBEDDING_MODEL = "text-embedding-3-large"

PROGRESS_FILE = "/tmp/english_pdf_progress.json"

# ============================================================
# Clients
# ============================================================

s3_client = None
secrets_client = None
lambda_client = None
openai_client = None


def init_clients():
    global s3_client, secrets_client, lambda_client, openai_client
    s3_client = boto3.client("s3", region_name=AWS_REGION)
    secrets_client = boto3.client("secretsmanager", region_name=AWS_REGION)
    lambda_client = boto3.client("lambda", region_name=AWS_REGION)
    
    response = secrets_client.get_secret_value(SecretId="rag/openai-api-key")
    openai_client = OpenAI(api_key=response["SecretString"])


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
            text_parts.append(f"[Page {page_num + 1}]\n{text}")
    
    doc.close()
    return "\n\n".join(text_parts)


def detect_language(text: str) -> str:
    """Detect language based on character patterns."""
    # Count character types
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


def chunk_text(text: str) -> List[str]:
    """Split text into overlapping chunks."""
    text = text.replace("\x00", "")
    chunks = []
    start = 0
    while start < len(text):
        end = start + CHUNK_SIZE
        chunk = text[start:end].strip()
        if chunk and len(chunk) > 50:  # Skip very short chunks
            chunks.append(chunk)
        start = end - CHUNK_OVERLAP
        if start >= len(text):
            break
    return chunks


def get_embeddings(texts: List[str]) -> List[List[float]]:
    """Get embeddings for texts."""
    embeddings = []
    for i in range(0, len(texts), 20):
        batch = texts[i:i+20]
        response = openai_client.embeddings.create(model=EMBEDDING_MODEL, input=batch)
        for item in response.data:
            embeddings.append(item.embedding)
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
    urllib.request.urlretrieve(current["Code"]["Location"], "/tmp/mhc-eng.zip")
    
    # Create temp Lambda with store code
    with zipfile.ZipFile("/tmp/mhc-eng.zip", "r") as zin:
        with zipfile.ZipFile("/tmp/mhc-eng-store.zip", "w") as zout:
            for item in zin.infolist():
                if item.filename != "lambda_function.py":
                    zout.writestr(item, zin.read(item.filename))
            zout.writestr("lambda_function.py", store_code)
    
    with open("/tmp/mhc-eng-store.zip", "rb") as f:
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
    with open("/tmp/mhc-eng.zip", "rb") as f:
        lambda_client.update_function_code(FunctionName="mini-han-chat", ZipFile=f.read())
    
    return stored


# ============================================================
# Main Processing
# ============================================================

def process_single_pdf(s3_key: str) -> Dict[str, Any]:
    """Process a single PDF."""
    filename = os.path.basename(s3_key)
    result = {"s3_key": s3_key, "filename": filename, "success": False, "chunks": 0}
    
    try:
        # Download PDF
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        pdf_bytes = response["Body"].read()
        
        # Extract text
        text = extract_text_from_pdf(pdf_bytes)
        
        if len(text) < 100:
            result["error"] = "Text too short"
            return result
        
        # Detect language
        language = detect_language(text)
        
        # Chunk
        chunks = chunk_text(text)
        
        if not chunks:
            result["error"] = "No chunks created"
            return result
        
        # Embed
        embeddings = get_embeddings(chunks)
        
        # Prepare data
        chunks_data = []
        for idx, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
            chunks_data.append({
                "s3_key": s3_key,
                "filename": filename,
                "chunk_index": idx,
                "chunk_text": chunk,
                "embedding": embedding,
                "metadata": {
                    "language": language,
                    "source_type": "pdf",
                }
            })
        
        # Store
        stored = store_chunks_via_lambda(chunks_data)
        
        result["success"] = True
        result["chunks"] = stored
        result["language"] = language
        
    except Exception as e:
        result["error"] = str(e)
    
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--prefix", type=str, default="tparents_pdfs/", help="S3 prefix")
    parser.add_argument("--start", type=int, default=0, help="Start index")
    parser.add_argument("--batch", type=int, default=100, help="Batch size")
    parser.add_argument("--resume", action="store_true", help="Resume from progress")
    args = parser.parse_args()
    
    print("=" * 60)
    print("英語 PDF 処理パイプライン")
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
            print(f"    ✓ {result['chunks']} chunks ({result.get('language', 'en')})", flush=True)
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
