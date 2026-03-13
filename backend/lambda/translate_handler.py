"""
Korean PDF Translation Lambda

GPT-4 Vision で韓国語 PDF を読み取り、英語に翻訳して DB に保存する。
"""
import base64
import io
import json
import os
import time
from typing import List, Dict, Any

import boto3
import psycopg2
from psycopg2.extras import execute_values
from openai import OpenAI

# PDF to image
import fitz  # PyMuPDF

# ============================================================
# Configuration
# ============================================================

S3_BUCKET = os.environ.get("S3_BUCKET", "rag-pdf-bucket-221646756615-ap-northeast-1")
VISION_MODEL = "gpt-4o"
EMBEDDING_MODEL = "text-embedding-3-large"
CHUNK_SIZE = 1000
CHUNK_OVERLAP = 200

# Clients
secrets_client = boto3.client("secretsmanager")
s3_client = boto3.client("s3")
openai_client = None


def get_openai_client():
    global openai_client
    if openai_client is None:
        secret_arn = os.environ.get("OPENAI_SECRET_ARN")
        if secret_arn:
            response = secrets_client.get_secret_value(SecretId=secret_arn)
            api_key = response["SecretString"]
        else:
            api_key = os.environ.get("OPENAI_API_KEY")
        openai_client = OpenAI(api_key=api_key)
    return openai_client


def get_db_connection():
    secret_arn = os.environ.get("DB_SECRET_ARN")
    if secret_arn:
        response = secrets_client.get_secret_value(SecretId=secret_arn)
        secret = json.loads(response["SecretString"])
        password = secret.get("password", secret.get("ragadmin"))
    else:
        password = os.environ.get("DB_PASSWORD")
    
    return psycopg2.connect(
        host=os.environ.get("DB_HOST"),
        port=os.environ.get("DB_PORT", "5432"),
        dbname=os.environ.get("DB_NAME", "ragdb"),
        user=os.environ.get("DB_USER", "ragadmin"),
        password=password,
    )


def pdf_to_images(pdf_bytes: bytes, max_pages: int = 10) -> List[bytes]:
    """Convert PDF to list of PNG images."""
    images = []
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    
    for page_num in range(min(len(doc), max_pages)):
        page = doc[page_num]
        mat = fitz.Matrix(150/72, 150/72)  # 150 DPI
        pix = page.get_pixmap(matrix=mat)
        img_bytes = pix.tobytes("png")
        images.append(img_bytes)
    
    doc.close()
    return images


def extract_and_translate(openai: OpenAI, images: List[bytes], filename: str) -> str:
    """Use GPT-4 Vision to extract Korean text and translate to English."""
    
    all_translations = []
    batch_size = 3  # Process 3 pages at a time to stay under token limits
    
    for batch_start in range(0, len(images), batch_size):
        batch_images = images[batch_start:batch_start + batch_size]
        
        content = [
            {
                "type": "text",
                "text": f"""Read the Korean text from these page images of "{filename}" and translate to fluent English.

RULES:
1. Output ONLY the English translation
2. Preserve meaning and spiritual context
3. Skip page numbers and headers
4. If text is unclear, make your best interpretation

Begin:"""
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
            response = openai.chat.completions.create(
                model=VISION_MODEL,
                messages=[{"role": "user", "content": content}],
                max_tokens=4096,
                temperature=0.3,
            )
            translation = response.choices[0].message.content
            all_translations.append(translation)
            print(f"  Translated pages {batch_start + 1}-{batch_start + len(batch_images)}")
        except Exception as e:
            print(f"  Error on pages {batch_start + 1}: {e}")
            all_translations.append(f"[Translation error on pages {batch_start + 1}-{batch_start + len(batch_images)}]")
        
        time.sleep(0.5)  # Rate limiting
    
    return "\n\n".join(all_translations)


def chunk_text(text: str) -> List[str]:
    """Split text into overlapping chunks."""
    text = text.replace("\x00", "")
    chunks = []
    start = 0
    
    while start < len(text):
        end = start + CHUNK_SIZE
        chunk = text[start:end]
        if chunk.strip():
            chunks.append(chunk)
        start = end - CHUNK_OVERLAP
        if start >= len(text):
            break
    
    return chunks


def get_embeddings(openai: OpenAI, texts: List[str]) -> List[List[float]]:
    """Get embeddings for texts."""
    embeddings = []
    for i in range(0, len(texts), 20):
        batch = texts[i:i+20]
        response = openai.embeddings.create(model=EMBEDDING_MODEL, input=batch)
        for item in response.data:
            embeddings.append(item.embedding)
    return embeddings


def store_chunks(conn, s3_key: str, filename: str, chunks: List[str], embeddings: List[List[float]]):
    """Store translated chunks in DB."""
    cursor = conn.cursor()
    
    translated_s3_key = f"translated/{s3_key}"
    
    values = []
    for idx, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
        embedding_str = "[" + ",".join(map(str, embedding)) + "]"
        metadata = json.dumps({
            "language": "en",
            "original_language": "ko",
            "translated": True,
            "original_s3_key": s3_key,
            "source_type": "korean_pdf_translation"
        })
        values.append((
            translated_s3_key,
            f"[EN] {filename}",
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


def process_single_pdf(s3_key: str, max_pages: int = 10) -> Dict[str, Any]:
    """Process a single PDF."""
    filename = os.path.basename(s3_key)
    print(f"Processing: {filename}")
    
    result = {"s3_key": s3_key, "filename": filename, "success": False, "chunks": 0, "error": None}
    
    try:
        # Download PDF
        print(f"  Downloading...")
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        pdf_bytes = response["Body"].read()
        
        # Convert to images
        print(f"  Converting to images (max {max_pages} pages)...")
        images = pdf_to_images(pdf_bytes, max_pages)
        print(f"  Got {len(images)} pages")
        
        # Translate
        print(f"  Translating with GPT-4 Vision...")
        openai = get_openai_client()
        english_text = extract_and_translate(openai, images, filename)
        print(f"  Translation: {len(english_text)} chars")
        
        if len(english_text) < 100:
            result["error"] = "Translation too short"
            return result
        
        # Chunk
        chunks = chunk_text(english_text)
        print(f"  Created {len(chunks)} chunks")
        
        # Embed
        print(f"  Generating embeddings...")
        embeddings = get_embeddings(openai, chunks)
        
        # Store
        print(f"  Storing in DB...")
        conn = get_db_connection()
        stored = store_chunks(conn, s3_key, filename, chunks, embeddings)
        conn.close()
        
        result["success"] = True
        result["chunks"] = stored
        print(f"  ✓ Stored {stored} chunks")
        
    except Exception as e:
        result["error"] = str(e)
        print(f"  ✗ Error: {e}")
    
    return result


def lambda_handler(event, context):
    """
    Lambda handler.
    
    Event:
        {
            "s3_keys": ["malsmCollection615Korean_pdfs/1.pdf", ...],
            "max_pages": 10
        }
    
    Or for batch processing:
        {
            "batch_start": 0,
            "batch_size": 10,
            "max_pages": 10
        }
    """
    try:
        s3_keys = event.get("s3_keys", [])
        max_pages = event.get("max_pages", 10)
        
        # If batch mode
        if not s3_keys and "batch_start" in event:
            batch_start = event["batch_start"]
            batch_size = event.get("batch_size", 10)
            prefix = event.get("prefix", "malsmCollection615Korean_pdfs/")
            
            # List PDFs
            paginator = s3_client.get_paginator("list_objects_v2")
            all_keys = []
            for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
                for obj in page.get("Contents", []):
                    if obj["Key"].lower().endswith(".pdf"):
                        all_keys.append(obj["Key"])
            
            # Sort and select batch
            all_keys.sort()
            s3_keys = all_keys[batch_start:batch_start + batch_size]
        
        results = []
        for s3_key in s3_keys:
            result = process_single_pdf(s3_key, max_pages)
            results.append(result)
        
        success_count = sum(1 for r in results if r["success"])
        total_chunks = sum(r["chunks"] for r in results)
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "processed": len(results),
                "success": success_count,
                "total_chunks": total_chunks,
                "results": results
            }, ensure_ascii=False)
        }
        
    except Exception as e:
        print(f"Error: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
