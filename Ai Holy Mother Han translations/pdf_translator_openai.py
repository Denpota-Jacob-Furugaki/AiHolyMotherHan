import os
import sys
from pathlib import Path
from pypdf import PdfReader
from openai import OpenAI
import time

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

def translate_text_openai(text, client, max_chunk_size=8000):
    """Translate Korean text to English using OpenAI API."""
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
            try:
                response = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {
                            "role": "system",
                            "content": "You are a professional translator. Translate the following Korean text to English. Maintain the original formatting, paragraph breaks, and structure. Only provide the translation, no explanations or additional text."
                        },
                        {
                            "role": "user",
                            "content": chunk
                        }
                    ],
                    temperature=0.3
                )
                
                translated_text = response.choices[0].message.content
                translated_chunks.append(translated_text)
                
                if (i + 1) % 5 == 0:
                    print(f"    Translated chunk {i+1}/{len(chunks)}")
                
            except Exception as e:
                print(f"  Error translating chunk {i+1}: {e}")
                translated_chunks.append(chunk)
        
        return "\n".join(translated_chunks)
    
    except Exception as e:
        print(f"  Translation error: {e}")
        return text

def process_pdfs(source_dir, dest_dir):
    """Process all PDFs: extract text and translate using OpenAI."""
    source_path = Path(source_dir)
    dest_path = Path(dest_dir)
    
    dest_path.mkdir(parents=True, exist_ok=True)
    
    pdf_files = sorted(source_path.glob("*.pdf"), key=lambda x: int(x.stem) if x.stem.isdigit() else float('inf'))
    
    if not pdf_files:
        print("No PDF files found!")
        return
    
    print(f"Found {len(pdf_files)} PDF files to process.\n")
    
    client = OpenAI()
    
    processed_count = 0
    skipped_count = 0
    failed_count = 0
    
    for idx, pdf_file in enumerate(pdf_files, 1):
        output_file = dest_path / f"{pdf_file.stem}.txt"
        
        if output_file.exists():
            skipped_count += 1
            print(f"[{idx}/{len(pdf_files)}] ⏭️  Skipping {pdf_file.name} (already exists)")
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
        print(f"  🔄 Translating to English with OpenAI...")
        
        english_text = translate_text_openai(korean_text, client)
        
        output_file.write_text(english_text, encoding='utf-8')
        processed_count += 1
        print(f"  ✅ Saved to: {output_file.name}")
        
        if idx % 10 == 0:
            print(f"\n📊 Progress: {processed_count} processed, {skipped_count} skipped, {failed_count} failed")
    
    print(f"\n\n{'='*60}")
    print(f"Processing Complete!")
    print(f"{'='*60}")
    print(f"✅ Processed: {processed_count}")
    print(f"⏭️  Skipped: {skipped_count}")
    print(f"❌ Failed: {failed_count}")
    print(f"📁 Output directory: {dest_path}")
    print(f"{'='*60}")

if __name__ == "__main__":
    api_key = os.getenv('OPENAI_API_KEY')
    
    if not api_key:
        print("=" * 60)
        print("OpenAI API Key Required")
        print("=" * 60)
        print("Please set your OpenAI API key as an environment variable:")
        print("  Windows: $env:OPENAI_API_KEY='your-api-key-here'")
        print("  Or run: setx OPENAI_API_KEY 'your-api-key-here'")
        print("=" * 60)
        sys.exit(1)
    
    source_directory = r"C:\Users\denpo\OneDrive\Coding\Ai Holy Mother Han\KR Malssum PDF"
    destination_directory = r"C:\Users\denpo\OneDrive\Coding\Ai Holy Mother Han\EN True Parents' Word Collection"
    
    print("=" * 60)
    print("PDF to English Translation Tool (OpenAI)")
    print("=" * 60)
    print(f"Source: {source_directory}")
    print(f"Destination: {destination_directory}")
    print(f"Model: gpt-4o-mini")
    print("=" * 60 + "\n")
    
    process_pdfs(source_directory, destination_directory)
