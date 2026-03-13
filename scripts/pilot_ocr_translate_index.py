import argparse
import json
import os
import pathlib
import re
import time
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import boto3
import psycopg2
from openai import OpenAI


EMBEDDING_MODEL = "text-embedding-3-large"
DEFAULT_TRANSLATION_MODEL = os.environ.get("TRANSLATION_MODEL", "gpt-4o-mini")


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _get_secret_string(secret_arn: str) -> str:
    client = boto3.client("secretsmanager")
    resp = client.get_secret_value(SecretId=secret_arn)
    return resp["SecretString"]


def _get_db_password() -> str:
    password = os.environ.get("DB_PASSWORD")
    if password:
        return password

    secret_arn = os.environ.get("DB_SECRET_ARN")
    if not secret_arn:
        raise RuntimeError("Missing required environment variable: DB_PASSWORD (or DB_SECRET_ARN)")

    secret_str = _get_secret_string(secret_arn)
    secret = json.loads(secret_str)
    return secret.get("password") or secret.get("ragadmin")


def get_db_connection():
    return psycopg2.connect(
        host=_require_env("DB_HOST"),
        port=os.environ.get("DB_PORT", "5432"),
        dbname=os.environ.get("DB_NAME", "ragdb"),
        user=os.environ.get("DB_USER", "ragadmin"),
        password=_get_db_password(),
    )


def get_openai_client() -> OpenAI:
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        secret_arn = os.environ.get("OPENAI_SECRET_ARN")
        if not secret_arn:
            raise RuntimeError("Missing required environment variable: OPENAI_API_KEY (or OPENAI_SECRET_ARN)")
        api_key = _get_secret_string(secret_arn)
    return OpenAI(api_key=api_key)


def get_embedding(client: OpenAI, text: str) -> List[float]:
    response = client.embeddings.create(model=EMBEDDING_MODEL, input=text)
    return response.data[0].embedding


def chunk_text(text: str, chunk_size: int, overlap: int) -> Iterable[str]:
    t = (text or "").strip()
    if not t:
        return
    if overlap >= chunk_size:
        raise ValueError("overlap must be smaller than chunk_size")

    start = 0
    while start < len(t):
        end = min(len(t), start + chunk_size)
        yield t[start:end]
        if end == len(t):
            break
        start = end - overlap


def ocr_pdf_pages(pdf_path: str, dpi: int, tesseract_lang: str, max_pages: Optional[int]) -> List[Tuple[int, str]]:
    try:
        from pdf2image import convert_from_path
    except Exception as e:
        raise RuntimeError("pdf2image is required. Install with: pip install pdf2image pillow") from e

    try:
        import pytesseract
    except Exception as e:
        raise RuntimeError("pytesseract is required. Install with: pip install pytesseract") from e

    images = convert_from_path(pdf_path, dpi=dpi)
    pages: List[Tuple[int, str]] = []

    for idx, image in enumerate(images, start=1):
        if max_pages is not None and idx > max_pages:
            break
        text = pytesseract.image_to_string(image, lang=tesseract_lang)
        pages.append((idx, text))

    return pages


def translate_text(client: OpenAI, text: str, target_language: str, model: str) -> str:
    prompt = (
        "Translate the following text into "
        + ("English" if target_language == "en" else "Japanese" if target_language == "ja" else target_language)
        + ". Preserve names and quoted scripture/terms as faithfully as possible. "
        "Do not add commentary. Output only the translated text.\n\n"
        + text
    )

    resp = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
    )
    return (resp.choices[0].message.content or "").strip()


def insert_document_chunk(
    conn,
    s3_key: str,
    filename: str,
    chunk_index: int,
    chunk_text: str,
    metadata: Dict,
    embedding: Sequence[float],
):
    embedding_str = "[" + ",".join(map(str, embedding)) + "]"

    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO documents (s3_key, filename, chunk_index, chunk_text, metadata, embedding)
            VALUES (%s, %s, %s, %s, %s::jsonb, %s::vector)
            """,
            (
                s3_key,
                filename,
                chunk_index,
                chunk_text,
                json.dumps(metadata, ensure_ascii=False),
                embedding_str,
            ),
        )


def document_chunk_exists(conn, s3_key: str) -> bool:
    with conn.cursor() as cursor:
        cursor.execute("SELECT 1 FROM documents WHERE s3_key = %s LIMIT 1", (s3_key,))
        return cursor.fetchone() is not None


def iter_pdfs(pdf_dir: Optional[str], pdfs: Optional[List[str]], limit: int) -> List[str]:
    selected: List[str] = []

    if pdfs:
        for p in pdfs:
            selected.append(p)
    elif pdf_dir:
        pdf_path = pathlib.Path(pdf_dir)
        if not pdf_path.exists():
            raise RuntimeError(f"pdf_dir not found: {pdf_dir}")
        for p in sorted(pdf_path.glob("**/*.pdf")):
            selected.append(str(p))
            if len(selected) >= limit:
                break

    selected = [str(pathlib.Path(p)) for p in selected]
    selected = [p for p in selected if p.lower().endswith(".pdf")]

    if not selected:
        raise RuntimeError("No PDFs selected. Use --pdf-dir or --pdf")

    return selected[:limit]


def normalize_doc_id(raw: str) -> str:
    s = raw.replace("\\", "/")
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Za-z0-9._/\-]", "_", s)
    s = s.strip("/_")
    return s or "document"


def get_doc_id_and_filename(pdf_path: str, pdf_dir: Optional[str]) -> Tuple[str, str]:
    p = pathlib.Path(pdf_path).resolve()
    if pdf_dir:
        base = pathlib.Path(pdf_dir).resolve()
        try:
            rel = p.relative_to(base)
            rel_str = str(rel)
            doc_id = normalize_doc_id(rel_str)
            return doc_id, rel_str
        except Exception:
            pass

    name = pathlib.Path(pdf_path).name
    doc_id = normalize_doc_id(name)
    return doc_id, name


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pdf-dir", default=None)
    parser.add_argument("--pdf", action="append", default=None)
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--dpi", type=int, default=300)
    parser.add_argument("--tesseract-lang", default="kor")
    parser.add_argument("--max-pages", type=int, default=None)
    parser.add_argument("--chunk-size", type=int, default=1200)
    parser.add_argument("--overlap", type=int, default=200)
    parser.add_argument("--targets", nargs="+", default=["en"], choices=["en", "ja"])
    parser.add_argument("--translation-model", default=DEFAULT_TRANSLATION_MODEL)
    parser.add_argument("--s3-prefix", default="pilot/ocr/")
    parser.add_argument("--sleep-seconds", type=float, default=0.0)
    args = parser.parse_args()

    pdf_list = iter_pdfs(args.pdf_dir, args.pdf, args.limit)

    openai_client = get_openai_client()
    conn = get_db_connection()

    try:
        inserted = 0
        for pdf_path in pdf_list:
            doc_id, filename = get_doc_id_and_filename(pdf_path, args.pdf_dir)
            pages = ocr_pdf_pages(pdf_path, dpi=args.dpi, tesseract_lang=args.tesseract_lang, max_pages=args.max_pages)

            global_chunk_index = 0

            for (page_number, page_text) in pages:
                for chunk_in_page, ko_chunk in enumerate(chunk_text(page_text, args.chunk_size, args.overlap)):
                    ko_chunk = ko_chunk.strip()
                    if not ko_chunk:
                        continue

                    for target in args.targets:
                        s3_key = f"{args.s3_prefix}{doc_id}/p{page_number:04d}/c{global_chunk_index:06d}/{target}.txt"
                        if document_chunk_exists(conn, s3_key):
                            continue

                        translated = translate_text(
                            openai_client,
                            ko_chunk,
                            target_language=target,
                            model=args.translation_model,
                        )
                        if not translated:
                            continue

                        emb = get_embedding(openai_client, translated)
                        metadata = {
                            "language": target,
                            "source_language": "ko",
                            "ocr": True,
                            "pdf_filename": filename,
                            "pdf_doc_id": doc_id,
                            "page": page_number,
                        }

                        insert_document_chunk(
                            conn,
                            s3_key=s3_key,
                            filename=filename,
                            chunk_index=global_chunk_index,
                            chunk_text=translated,
                            metadata=metadata,
                            embedding=emb,
                        )
                        inserted += 1

                        if args.sleep_seconds > 0:
                            time.sleep(args.sleep_seconds)

                    global_chunk_index += 1

            conn.commit()

        print(f"Inserted {inserted} chunks")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
