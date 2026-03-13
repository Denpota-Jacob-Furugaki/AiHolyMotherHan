import json
import os
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import boto3
import psycopg2


secrets_client = boto3.client("secretsmanager")


def _get_secret_string(secret_arn: str) -> str:
    resp = secrets_client.get_secret_value(SecretId=secret_arn)
    return resp["SecretString"]


def _get_db_password() -> str:
    password = os.environ.get("DB_PASSWORD")
    if password:
        return password

    secret_arn = os.environ.get("DB_SECRET_ARN")
    if not secret_arn:
        raise RuntimeError("Missing DB_PASSWORD (or DB_SECRET_ARN)")

    secret_str = _get_secret_string(secret_arn)
    secret = json.loads(secret_str)
    return secret.get("password") or secret.get("ragadmin")


def get_db_connection():
    return psycopg2.connect(
        host=os.environ.get("DB_HOST"),
        port=os.environ.get("DB_PORT", "5432"),
        dbname=os.environ.get("DB_NAME", "ragdb"),
        user=os.environ.get("DB_USER", "ragadmin"),
        password=_get_db_password(),
    )


def _iter_s3_jsonl(bucket: str, key: str) -> Iterable[Dict[str, Any]]:
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"]
    for line in body.iter_lines():
        if not line:
            continue
        yield json.loads(line.decode("utf-8"))


def _embedding_to_pgvector(embedding: Sequence[float]) -> str:
    return "[" + ",".join(map(str, embedding)) + "]"


def _existing_keys(conn, keys: Sequence[str]) -> set:
    if not keys:
        return set()
    with conn.cursor() as cursor:
        cursor.execute("SELECT s3_key FROM documents WHERE s3_key = ANY(%s)", (list(keys),))
        return {row[0] for row in cursor.fetchall()}


def _insert_rows(conn, rows: Sequence[Tuple[str, str, int, str, str, str]]) -> int:
    if not rows:
        return 0

    with conn.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO documents (s3_key, filename, chunk_index, chunk_text, metadata, embedding)
            VALUES (%s, %s, %s, %s, %s::jsonb, %s::vector)
            """,
            rows,
        )
    return len(rows)


def lambda_handler(event, context):
    bucket = event.get("bucket") or os.environ.get("MANIFEST_BUCKET")
    key = event.get("key") or event.get("manifest_key")

    if not bucket:
        return {"statusCode": 400, "body": json.dumps({"error": "bucket is required (event.bucket or MANIFEST_BUCKET)"})}
    if not key:
        return {"statusCode": 400, "body": json.dumps({"error": "key is required (event.key or event.manifest_key)"})}

    batch_size = int(event.get("batch_size") or os.environ.get("BATCH_SIZE", "200"))

    conn = get_db_connection()
    inserted = 0
    skipped_existing = 0
    processed = 0

    try:
        buffer: List[Dict[str, Any]] = []

        def flush():
            nonlocal inserted, skipped_existing
            if not buffer:
                return

            keys = [r.get("s3_key") for r in buffer if r.get("s3_key")]
            existing = _existing_keys(conn, keys)

            rows: List[Tuple[str, str, int, str, str, str]] = []
            for r in buffer:
                s3_key = r.get("s3_key")
                if not s3_key:
                    continue
                if s3_key in existing:
                    skipped_existing += 1
                    continue

                filename = r.get("filename") or "unknown"
                chunk_index = int(r.get("chunk_index") or 0)
                chunk_text = r.get("chunk_text") or ""
                metadata = r.get("metadata") or {}
                embedding = r.get("embedding") or []

                rows.append(
                    (
                        s3_key,
                        filename,
                        chunk_index,
                        chunk_text,
                        json.dumps(metadata, ensure_ascii=False),
                        _embedding_to_pgvector(embedding),
                    )
                )

            inserted += _insert_rows(conn, rows)
            conn.commit()
            buffer.clear()

        for record in _iter_s3_jsonl(bucket, key):
            processed += 1
            buffer.append(record)
            if len(buffer) >= batch_size:
                flush()

        flush()

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "status": "success",
                    "bucket": bucket,
                    "key": key,
                    "processed": processed,
                    "inserted": inserted,
                    "skipped_existing": skipped_existing,
                },
                ensure_ascii=False,
            ),
        }

    finally:
        conn.close()
