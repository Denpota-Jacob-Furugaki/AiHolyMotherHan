#!/usr/bin/env python3
"""
韓国語 PDF を GPT-4 Vision で英語に翻訳し、DB に保存するスクリプト

Usage:
    python translate_korean_pdfs.py --limit 50
"""

import argparse
import base64
import io
import json
import os
import sys
import time
from typing import List, Dict, Any, Optional

import boto3
from openai import OpenAI
import psycopg2
from psycopg2.extras import execute_values

# PDF to image conversion
try:
    import fitz  # PyMuPDF
    HAS_PYMUPDF = True
except ImportError:
    HAS_PYMUPDF = False
    print("Warning: PyMuPDF not installed. Run: pip install pymupdf")

# ============================================================
# Configuration
# ============================================================

AWS_REGION = "ap-northeast-1"
S3_BUCKET = "rag-pdf-bucket-221646756615-ap-northeast-1"
S3_PREFIX = "malsmCollection615Korean_pdfs/"

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
VISION_MODEL = "gpt-4o"  # GPT-4 Vision
EMBEDDING_MODEL = "text-embedding-3-large"

CHUNK_SIZE = 1000
CHUNK_OVERLAP = 200

# DB Config
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "ragdb")
DB_USER = os.environ.get("DB_USER", "ragadmin")
DB_PASSWORD = os.environ.get("DB_PASSWORD")

# Progress file
PROGRESS_FILE = "/tmp/translate_progress.json"


# ============================================================
# Helpers
# ============================================================

def get_s3_client():
    return boto3.client("s3", region_name=AWS_REGION)


def get_openai_client():
    if not OPENAI_API_KEY:
        raise ValueError("OPENAI_API_KEY environment variable required")
    return OpenAI(api_key=OPENAI_API_KEY)


def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def list_korean_pdfs(s3_client, limit: int = 50) -> List[str]:
    """List Korean PDF keys from S3."""
    paginator = s3_client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        for obj in page.get("Contents", []):
            if obj["Key"].lower().endswith(".pdf"):
                keys.append(obj["Key"])
                if len(keys) >= limit:
                    return keys
    return keys


def pdf_to_images(pdf_bytes: bytes, max_pages: int = 20) -> List[bytes]:
    """Convert PDF to list of PNG images (one per page)."""
    if not HAS_PYMUPDF:
        raise ImportError("PyMuPDF required for PDF to image conversion")
    
    images = []
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    
    for page_num in range(min(len(doc), max_pages)):
        page = doc[page_num]
        # Render at 150 DPI for good quality without being too large
        mat = fitz.Matrix(150/72, 150/72)
        pix = page.get_pixmap(matrix=mat)
        img_bytes = pix.tobytes("png")
        images.append(img_bytes)
    
    doc.close()
    return images


def extract_and_translate_with_vision(
    openai_client: OpenAI,
    images: List[bytes],
    filename: str
) -> str:
    """Use GPT-4 Vision to extract Korean text and translate to English."""
    
    # Process in batches of 5 pages to avoid token limits
    all_translations = []
    batch_size = 5
    
    for batch_start in range(0, len(images), batch_size):
        batch_images = images[batch_start:batch_start + batch_size]
        
        # Build message content with images
        content = [
            {
                "type": "text",
                "text": f"""You are processing pages {batch_start + 1}-{batch_start + len(batch_images)} of a Korean religious document "{filename}".

TASK:
1. Read all Korean text from these page images
2. Translate the Korean text to fluent English
3. Preserve the meaning and spiritual context accurately
4. Output ONLY the English translation, no Korean text

If a page contains only page numbers, headers, or table of contents, briefly note what it is and continue.

Begin translation:"""
            }
        ]
        
        for img_bytes in batch_images:
            img_base64 = base64.b64encode(img_bytes).decode("utf-8")
            content.append({
                "type": "image_url",
                "image_url": {
                    "url": f"data:image/png;base64,{img_base64}",
                    "detail": "high"
                }
            })
        
        try:
            response = openai_client.chat.completions.create(
                model=VISION_MODEL,
                messages=[{"role": "user", "content": content}],
                max_tokens=4096,
                temperature=0.3,
            )
            
            translation = response.choices[0].message.content
            all_translations.append(translation)
            
            print(f"    Processed pages {batch_start + 1}-{batch_start + len(batch_images)}")
            
        except Exception as e:
            print(f"    Error on pages {batch_start + 1}-{batch_start + len(batch_images)}: {e}")
            all_translations.append(f"[Error processing pages {batch_start + 1}-{batch_start + len(batch_images)}]")
        
        # Rate limiting
        time.sleep(1)
    
    return "\n\n".join(all_translations)


def chunk_text(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> List[str]:
    """Split text into overlapping chunks."""
    text = text.replace("\x00", "")  # Remove null bytes
    chunks = []
    start = 0
    
    while start < len(text):
        end = start + chunk_size
        chunk = text[start:end]
        if chunk.strip():  # Only add non-empty chunks
            chunks.append(chunk)
        start = end - overlap
        if start >= len(text):
            break
    
    return chunks


def get_embeddings(openai_client: OpenAI, texts: List[str]) -> List[List[float]]:
    """Get embeddings for a list of texts."""
    embeddings = []
    
    for i in range(0, len(texts), 20):  # Batch of 20
        batch = texts[i:i+20]
        response = openai_client.embeddings.create(
            model=EMBEDDING_MODEL,
            input=batch
        )
        for item in response.data:
            embeddings.append(item.embedding)
    
    return embeddings


def store_chunks_in_db(
    conn,
    s3_key: str,
    filename: str,
    chunks: List[str],
    embeddings: List[List[float]],
    original_language: str = "ko"
):
    """Store translated chunks in the database."""
    cursor = conn.cursor()
    
    # Create a new s3_key to distinguish from original
    translated_s3_key = f"translated/{s3_key}"
    
    values = []
    for idx, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
        embedding_str = "[" + ",".join(map(str, embedding)) + "]"
        metadata = json.dumps({
            "language": "en",
            "original_language": original_language,
            "translated": True,
            "original_s3_key": s3_key,
            "source_type": "korean_pdf_translation"
        })
        values.append((
            translated_s3_key,
            f"[EN] {filename}",  # Mark as English translation
            idx,
            chunk,
            embedding_str,
            metadata
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
    cursor.close()
    
    return len(values)


def load_progress() -> Dict[str, Any]:
    """Load progress from file."""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {"processed": [], "success": 0, "errors": 0, "total_chunks": 0}


def save_progress(progress: Dict[str, Any]):
    """Save progress to file."""
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)


# ============================================================
# Main
# ============================================================

def process_pdf(
    s3_client,
    openai_client,
    conn,
    s3_key: str,
    max_pages: int = 20
) -> Dict[str, Any]:
    """Process a single PDF: download, extract, translate, embed, store."""
    
    filename = os.path.basename(s3_key)
    print(f"\nProcessing: {filename}")
    
    result = {
        "s3_key": s3_key,
        "filename": filename,
        "success": False,
        "chunks": 0,
        "error": None
    }
    
    try:
        # 1. Download PDF from S3
        print(f"  Downloading from S3...")
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        pdf_bytes = response["Body"].read()
        
        # 2. Convert PDF to images
        print(f"  Converting to images (max {max_pages} pages)...")
        images = pdf_to_images(pdf_bytes, max_pages=max_pages)
        print(f"  Got {len(images)} page images")
        
        # 3. Extract and translate with GPT-4 Vision
        print(f"  Extracting and translating with GPT-4 Vision...")
        english_text = extract_and_translate_with_vision(openai_client, images, filename)
        
        if not english_text or len(english_text) < 100:
            result["error"] = "Translation too short or empty"
            return result
        
        print(f"  Translated text length: {len(english_text)} chars")
        
        # 4. Chunk the text
        chunks = chunk_text(english_text)
        print(f"  Created {len(chunks)} chunks")
        
        if not chunks:
            result["error"] = "No chunks created"
            return result
        
        # 5. Generate embeddings
        print(f"  Generating embeddings...")
        embeddings = get_embeddings(openai_client, chunks)
        
        # 6. Store in database
        print(f"  Storing in database...")
        stored = store_chunks_in_db(conn, s3_key, filename, chunks, embeddings)
        
        result["success"] = True
        result["chunks"] = stored
        print(f"  ✓ Stored {stored} chunks")
        
    except Exception as e:
        result["error"] = str(e)
        print(f"  ✗ Error: {e}")
    
    return result


def main():
    parser = argparse.ArgumentParser(description="Translate Korean PDFs to English and store in DB")
    parser.add_argument("--limit", type=int, default=50, help="Number of PDFs to process")
    parser.add_argument("--max-pages", type=int, default=20, help="Max pages per PDF")
    parser.add_argument("--resume", action="store_true", help="Resume from progress file")
    args = parser.parse_args()
    
    # Check dependencies
    if not HAS_PYMUPDF:
        print("Error: PyMuPDF required. Run: pip install pymupdf")
        sys.exit(1)
    
    # Check environment
    required_env = ["OPENAI_API_KEY", "DB_HOST", "DB_PASSWORD"]
    missing = [e for e in required_env if not os.environ.get(e)]
    if missing:
        print(f"Error: Missing environment variables: {', '.join(missing)}")
        sys.exit(1)
    
    print("=" * 60)
    print("Korean PDF Translation Pipeline")
    print("=" * 60)
    print(f"Target: {args.limit} PDFs, max {args.max_pages} pages each")
    print()
    
    # Initialize clients
    s3_client = get_s3_client()
    openai_client = get_openai_client()
    conn = get_db_connection()
    
    # Load progress
    progress = load_progress() if args.resume else {"processed": [], "success": 0, "errors": 0, "total_chunks": 0}
    
    # List PDFs
    print("Listing PDFs from S3...")
    all_pdfs = list_korean_pdfs(s3_client, limit=args.limit * 2)  # Get extra in case some are already processed
    print(f"Found {len(all_pdfs)} PDFs")
    
    # Filter out already processed
    to_process = [k for k in all_pdfs if k not in progress["processed"]][:args.limit]
    print(f"To process: {len(to_process)} PDFs")
    
    # Process PDFs
    for i, s3_key in enumerate(to_process):
        print(f"\n[{i+1}/{len(to_process)}] ", end="")
        
        result = process_pdf(s3_client, openai_client, conn, s3_key, max_pages=args.max_pages)
        
        progress["processed"].append(s3_key)
        if result["success"]:
            progress["success"] += 1
            progress["total_chunks"] += result["chunks"]
        else:
            progress["errors"] += 1
        
        save_progress(progress)
        
        # Rate limiting
        time.sleep(2)
    
    conn.close()
    
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Processed: {len(progress['processed'])} PDFs")
    print(f"Success: {progress['success']}")
    print(f"Errors: {progress['errors']}")
    print(f"Total chunks: {progress['total_chunks']}")


if __name__ == "__main__":
    main()
