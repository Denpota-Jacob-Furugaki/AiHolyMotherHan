import os
import sys
from pathlib import Path
from pypdf import PdfReader
from deep_translator import GoogleTranslator
import time
from tqdm import tqdm

def extract_text_from_pdf(pdf_path):
    """Extract text from a PDF file."""
    try:
        reader = PdfReader(pdf_path)
        text = ""
        for page in reader.pages:
            text += page.extract_text() + "\n"
        return text
    except Exception as e:
        print(f"Error extracting text from {pdf_path}: {e}")
        return None

def translate_text(text, max_chunk_size=3000):
    """Translate Korean text to English in chunks with retry logic."""
    if not text or not text.strip():
        return ""
    
    try:
        chunks = []
        lines = text.split('\n')
        current_chunk = ""
        
        for line in lines:
            if len(current_chunk) + len(line) + 1 < max_chunk_size:
                current_chunk += line + "\n"
            else:
                if current_chunk:
                    chunks.append(current_chunk)
                current_chunk = line + "\n"
        
        if current_chunk:
            chunks.append(current_chunk)
        
        translated_chunks = []
        
        for i, chunk in enumerate(chunks):
            max_retries = 5
            retry_delay = 2
            
            for attempt in range(max_retries):
                try:
                    translator = GoogleTranslator(source='ko', target='en')
                    result = translator.translate(chunk)
                    translated_chunks.append(result)
                    time.sleep(1.5)
                    break
                except Exception as e:
                    if attempt < max_retries - 1:
                        wait_time = retry_delay * (2 ** attempt)
                        print(f"  Retry {attempt + 1}/{max_retries} for chunk {i+1} after {wait_time}s...")
                        time.sleep(wait_time)
                    else:
                        print(f"  Failed chunk {i+1} after {max_retries} attempts, keeping original")
                        translated_chunks.append(chunk)
        
        return "\n".join(translated_chunks)
    
    except Exception as e:
        print(f"  Translation error: {e}")
        return text

def process_pdfs(source_dir, dest_dir):
    """Process all PDFs: extract text and translate."""
    source_path = Path(source_dir)
    dest_path = Path(dest_dir)
    
    dest_path.mkdir(parents=True, exist_ok=True)
    
    pdf_files = sorted(source_path.glob("*.pdf"), key=lambda x: int(x.stem) if x.stem.isdigit() else float('inf'))
    
    if not pdf_files:
        print("No PDF files found!")
        return
    
    print(f"Found {len(pdf_files)} PDF files to process.\n")
    
    processed_count = 0
    skipped_count = 0
    failed_count = 0
    
    for idx, pdf_file in enumerate(pdf_files, 1):
        output_file = dest_path / f"{pdf_file.stem}.txt"
        
        if output_file.exists():
            skipped_count += 1
            print(f"[{idx}/{len(pdf_files)}] Skipping {pdf_file.name} (already exists)")
            continue
        
        print(f"\n[{idx}/{len(pdf_files)}] Processing: {pdf_file.name}")
        
        korean_text = extract_text_from_pdf(pdf_file)
        
        if korean_text is None:
            failed_count += 1
            print(f"  ❌ Failed to extract text")
            continue
        
        if not korean_text.strip():
            output_file.write_text("", encoding='utf-8')
            processed_count += 1
            print(f"  ⚠️  No text extracted, created empty file")
            continue
        
        print(f"  📄 Extracted {len(korean_text):,} characters")
        print(f"  🔄 Translating to English...")
        
        english_text = translate_text(korean_text)
        
        output_file.write_text(english_text, encoding='utf-8')
        processed_count += 1
        print(f"  ✅ Saved to: {output_file.name}")
        
        if idx % 10 == 0:
            print(f"\n📊 Progress: {processed_count} processed, {skipped_count} skipped, {failed_count} failed")
            print(f"   Pausing for 10 seconds to avoid rate limiting...")
            time.sleep(10)
        else:
            time.sleep(2)
    
    print(f"\n\n{'='*60}")
    print(f"Processing Complete!")
    print(f"{'='*60}")
    print(f"✅ Processed: {processed_count}")
    print(f"⏭️  Skipped: {skipped_count}")
    print(f"❌ Failed: {failed_count}")
    print(f"📁 Output directory: {dest_path}")
    print(f"{'='*60}")

if __name__ == "__main__":
    source_directory = r"C:\Users\denpo\OneDrive\Coding\Ai Holy Mother Han\KR Malssum PDF"
    destination_directory = r"C:\Users\denpo\OneDrive\Coding\Ai Holy Mother Han\EN True Parents' Word Collection"
    
    print("=" * 60)
    print("PDF to English Translation Tool")
    print("=" * 60)
    print(f"Source: {source_directory}")
    print(f"Destination: {destination_directory}")
    print("=" * 60 + "\n")
    
    process_pdfs(source_directory, destination_directory)
