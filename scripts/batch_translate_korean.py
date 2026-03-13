#!/usr/bin/env python3
"""
韓国語 PDF バッチ翻訳 + メタデータ抽出パイプライン

Usage:
    python batch_translate_korean.py --start 0 --batch 100
"""

import argparse
import base64
import json
import os
import sys
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
S3_PREFIX = "malsmCollection615Korean_pdfs/"

CHUNK_SIZE = 1000
CHUNK_OVERLAP = 200
EMBEDDING_MODEL = "text-embedding-3-large"
VISION_MODEL = "gpt-4o"

MAX_PAGES_PER_PDF = 15
PROGRESS_FILE = "/tmp/batch_translate_progress.json"
TRANSLATIONS_DIR = "/tmp/korean_translations"
GLOSSARY_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "glossary.json")

# ============================================================
# Glossary for consistent translations
# ============================================================

GLOSSARY = {}

def load_glossary():
    global GLOSSARY
    try:
        with open(GLOSSARY_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            # Load Korean to English translations
            for key, value in data.items():
                if key.startswith("_"):
                    continue  # Skip comments
                if "ko_to_en" in key and isinstance(value, dict):
                    GLOSSARY.update(value)
        print(f"Loaded glossary with {len(GLOSSARY)} Korean→English terms")
    except Exception as e:
        print(f"Warning: Could not load glossary: {e}")

def get_glossary_prompt() -> str:
    """Generate glossary instructions for translation prompts."""
    if not GLOSSARY:
        return ""
    
    terms = "\n".join([f"- {k} = {v}" for k, v in list(GLOSSARY.items())[:30]])
    return f"""
IMPORTANT: Use these standard translations for Korean terms:
{terms}

Always use these established translations instead of literal translations."""

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
    
    # Load glossary for consistent translations
    load_glossary()


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

def list_all_pdfs() -> List[str]:
    """List all Korean PDFs from S3."""
    paginator = s3_client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        for obj in page.get("Contents", []):
            if obj["Key"].lower().endswith(".pdf"):
                keys.append(obj["Key"])
    return sorted(keys)


def pdf_to_images(pdf_bytes: bytes, page_nums: List[int]) -> List[bytes]:
    """Convert specific PDF pages to images."""
    images = []
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    
    for page_num in page_nums:
        if page_num < len(doc):
            page = doc[page_num]
            mat = fitz.Matrix(150/72, 150/72)
            pix = page.get_pixmap(matrix=mat)
            img_bytes = pix.tobytes("png")
            if len(img_bytes) > 15000:  # Skip mostly blank pages
                images.append(img_bytes)
    
    doc.close()
    return images


def apply_glossary(text: str) -> str:
    """Apply glossary corrections to translated text."""
    for korean, english in GLOSSARY.items():
        text = text.replace(korean, english)
    return text


def extract_metadata(images: List[bytes]) -> Dict[str, Any]:
    """Extract metadata from first pages using GPT-4 Vision."""
    if not images:
        return {}
    
    glossary_hint = get_glossary_prompt()
    
    content = [
        {
            "type": "text",
            "text": f"""Analyze these pages from a Korean religious/philosophical document.

Determine the SOURCE TYPE and extract appropriate metadata:

TYPE A - SPEECH (말씀/설교):
If this is a speech/sermon with a date and location, extract:
{{"source_type": "speech", "title": "Korean title", "title_en": "English title", "date": "YYYY-MM-DD", "location": "Location in English", "location_ko": "Korean location"}}

TYPE B - BOOK/TEXTBOOK (책/교재):
If this is from a book with chapters/sections (like 통일사상, 원리강론), extract:
{{"source_type": "book", "book_name": "Book name", "book_name_ko": "Korean name", "chapter": "Chapter X", "chapter_title": "Chapter title", "section": "Section Y", "section_title": "Section title"}}

TYPE C - ARTICLE/PAPER (논문/기사):
If this is an article or paper, extract:
{{"source_type": "article", "author": "Author name", "title": "Title", "title_ko": "Korean title", "publication": "Publication name"}}
{glossary_hint}

Return ONLY valid JSON with the appropriate structure based on source type.
If unsure, default to source_type: "speech"."""
        }
    ]
    
    for img in images[:2]:
        content.append({
            "type": "image_url",
            "image_url": {"url": f"data:image/png;base64,{base64.b64encode(img).decode()}", "detail": "high"}
        })
    
    try:
        response = openai_client.chat.completions.create(
            model=VISION_MODEL,
            messages=[{"role": "user", "content": content}],
            max_tokens=500,
        )
        result = response.choices[0].message.content
        if "```" in result:
            result = result.split("```")[1].replace("json", "").strip()
        return json.loads(result)
    except:
        return {}


def translate_pages(images: List[bytes], filename: str) -> str:
    """Translate page images to English summaries."""
    all_summaries = []
    glossary_hint = get_glossary_prompt()
    
    for i, img in enumerate(images):
        content = [
            {
                "type": "text",
                "text": f"""This is page {i+1} from a Korean spiritual/philosophical text "{filename}".

Please provide a comprehensive English summary including:
- Main topics and concepts
- Key teachings or ideas
- Important quotes (with English explanation)
{glossary_hint}

Be thorough and accurate."""
            },
            {
                "type": "image_url",
                "image_url": {"url": f"data:image/png;base64,{base64.b64encode(img).decode()}", "detail": "high"}
            }
        ]
        
        try:
            response = openai_client.chat.completions.create(
                model=VISION_MODEL,
                messages=[{"role": "user", "content": content}],
                max_tokens=2000,
            )
            summary = response.choices[0].message.content
            if summary and len(summary) > 100 and "sorry" not in summary.lower()[:50]:
                all_summaries.append(f"[Page {i+1}]\n{summary}")
        except Exception as e:
            print(f"      Page {i+1} error: {e}")
        
        time.sleep(0.5)  # Rate limiting
    
    return "\n\n---\n\n".join(all_summaries)


def build_display_name(metadata: Dict[str, Any], filename: str) -> str:
    """Build display name based on source type."""
    source_type = metadata.get("source_type", "speech")
    
    if source_type == "book":
        # 本形式: 書名 章 節 タイトル
        parts = []
        if metadata.get("book_name"):
            parts.append(metadata["book_name"])
        if metadata.get("chapter"):
            parts.append(metadata["chapter"])
        if metadata.get("chapter_title"):
            parts.append(metadata["chapter_title"])
        if metadata.get("section"):
            parts.append(metadata["section"])
        if metadata.get("section_title"):
            parts.append(metadata["section_title"])
        if parts:
            return " ".join(parts)
    
    elif source_type == "article":
        # 論文形式: 著者 - タイトル
        author = metadata.get("author")
        title = metadata.get("title") or metadata.get("title_ko")
        if author and title:
            return f"{author} - {title}"
        elif title:
            return title
    
    else:  # speech (default)
        # スピーチ形式: 日付 - タイトル (場所)
        date = metadata.get("date")
        title = metadata.get("title_en") or metadata.get("title")
        location = metadata.get("location")
        
        if date and title:
            display = f"{date} - {title}"
            if location:
                display += f" ({location})"
            return display
    
    # Fallback
    return f"[EN-KO] {filename}"


def chunk_text(text: str) -> List[str]:
    """Split text into overlapping chunks."""
    chunks = []
    start = 0
    while start < len(text):
        end = start + CHUNK_SIZE
        chunk = text[start:end].strip()
        if chunk:
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
# Database Storage (via dedicated Lambda)
# ============================================================

STORAGE_LAMBDA = "rag-storage-helper"

def store_chunks_via_lambda(chunks_data: List[Dict]) -> int:
    """Store chunks in DB using dedicated storage Lambda."""
    stored = 0
    batch_size = 30
    
    for i in range(0, len(chunks_data), batch_size):
        batch = chunks_data[i:i+batch_size]
        try:
            response = lambda_client.invoke(
                FunctionName=STORAGE_LAMBDA,
                InvocationType="RequestResponse",
                Payload=json.dumps({"action": "store", "chunks": batch})
            )
            result = json.loads(response["Payload"].read())
            if result.get("statusCode") == 200:
                body = json.loads(result.get("body", "{}"))
                stored += body.get("stored", 0)
            else:
                print(f"      Storage error: {result}")
        except Exception as e:
            print(f"      Lambda invoke error: {e}")
        time.sleep(0.3)
    
    return stored


# ============================================================
# Main Processing
# ============================================================

def process_single_pdf(s3_key: str) -> Dict[str, Any]:
    """Process a single PDF: download, extract metadata, translate, embed, store."""
    filename = os.path.basename(s3_key)
    result = {"s3_key": s3_key, "filename": filename, "success": False, "chunks": 0}
    
    try:
        # Download PDF
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        pdf_bytes = response["Body"].read()
        
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        total_pages = len(doc)
        doc.close()
        
        # Extract metadata from first pages
        meta_images = pdf_to_images(pdf_bytes, list(range(2, 10)))
        metadata = extract_metadata(meta_images)
        
        # Translate content pages
        start_page = min(8, total_pages - 1)
        content_pages = list(range(start_page, min(start_page + MAX_PAGES_PER_PDF, total_pages)))
        content_images = pdf_to_images(pdf_bytes, content_pages)
        
        if not content_images:
            result["error"] = "No content pages"
            return result
        
        print(f"    Translating {len(content_images)} pages...", flush=True)
        english_text = translate_pages(content_images, filename)
        
        if len(english_text) < 200:
            result["error"] = "Translation too short"
            return result
        
        # Build display name based on source type
        source_type = metadata.get("source_type", "speech")
        display_name = build_display_name(metadata, filename)
        
        # Chunk and embed
        chunks = chunk_text(english_text)
        print(f"    Created {len(chunks)} chunks, embedding...", flush=True)
        embeddings = get_embeddings(chunks)
        
        # Prepare chunks data with appropriate metadata
        chunks_data = []
        for idx, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
            chunk_metadata = {
                "language": "en",
                "original_language": "ko",
                "translated": True,
                "source_type": source_type,
            }
            
            # Add type-specific metadata
            if source_type == "book":
                chunk_metadata.update({
                    "book_name": metadata.get("book_name"),
                    "book_name_ko": metadata.get("book_name_ko"),
                    "chapter": metadata.get("chapter"),
                    "chapter_title": metadata.get("chapter_title"),
                    "section": metadata.get("section"),
                    "section_title": metadata.get("section_title"),
                })
            elif source_type == "article":
                chunk_metadata.update({
                    "author": metadata.get("author"),
                    "title": metadata.get("title"),
                    "title_ko": metadata.get("title_ko"),
                    "publication": metadata.get("publication"),
                })
            else:  # speech (default)
                chunk_metadata.update({
                    "speech_date": metadata.get("date"),
                    "speech_title": metadata.get("title"),
                    "speech_title_en": metadata.get("title_en"),
                    "speech_location": metadata.get("location"),
                })
            
            chunks_data.append({
                "s3_key": f"translated/{s3_key}",
                "filename": display_name,
                "chunk_index": idx,
                "chunk_text": chunk,
                "embedding": embedding,
                "metadata": chunk_metadata,
            })
        
        # Store in DB
        print(f"    Storing in DB...", flush=True)
        stored = store_chunks_via_lambda(chunks_data)
        
        result["success"] = True
        result["chunks"] = stored
        result["metadata"] = metadata
        
        # Save translation file
        os.makedirs(TRANSLATIONS_DIR, exist_ok=True)
        with open(os.path.join(TRANSLATIONS_DIR, filename.replace(".pdf", ".txt")), "w") as f:
            f.write(f"# {display_name}\n\n{english_text}")
        
    except Exception as e:
        result["error"] = str(e)
    
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=int, default=0, help="Start index")
    parser.add_argument("--batch", type=int, default=50, help="Batch size")
    parser.add_argument("--resume", action="store_true", help="Resume from progress")
    args = parser.parse_args()
    
    print("=" * 60)
    print("韓国語 PDF バッチ翻訳パイプライン")
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
    
    # List all PDFs
    all_pdfs = list_all_pdfs()
    print(f"Total PDFs: {len(all_pdfs)}")
    
    # Filter out already processed
    to_process = [k for k in all_pdfs if k not in progress["processed_pdfs"]]
    to_process = to_process[args.start:args.start + args.batch]
    print(f"To process: {len(to_process)} (starting at {args.start})")
    
    # Process PDFs
    for i, s3_key in enumerate(to_process):
        filename = os.path.basename(s3_key)
        print(f"\n[{i+1}/{len(to_process)}] {filename}", flush=True)
        
        result = process_single_pdf(s3_key)
        
        progress["processed_pdfs"].append(s3_key)
        if result["success"]:
            progress["success"] += 1
            progress["total_chunks"] += result["chunks"]
            print(f"    ✓ {result['chunks']} chunks stored", flush=True)
            if result.get("metadata", {}).get("date"):
                print(f"    📅 {result['metadata']['date']} - {result['metadata'].get('title_en', '')[:40]}", flush=True)
        else:
            progress["errors"] += 1
            print(f"    ✗ Error: {result.get('error', 'unknown')}", flush=True)
        
        save_progress(progress)
        time.sleep(2)
    
    progress["status"] = "completed"
    save_progress(progress)
    
    print("\n" + "=" * 60)
    print("完了")
    print(f"成功: {progress['success']}")
    print(f"エラー: {progress['errors']}")
    print(f"合計チャンク: {progress['total_chunks']}")


if __name__ == "__main__":
    main()
