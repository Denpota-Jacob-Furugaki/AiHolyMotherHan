import argparse
import json
import os
from typing import Any, Dict, List, Optional, Sequence, Tuple

import boto3
import psycopg2
from psycopg2.extras import RealDictCursor
from openai import OpenAI


EMBEDDING_MODEL = "text-embedding-3-large"


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


def vector_search(
    conn,
    query_embedding: Sequence[float],
    query_language: str,
    top_k: int,
    similarity_threshold: float,
    s3_prefix: Optional[str],
) -> List[Dict[str, Any]]:
    embedding_str = "[" + ",".join(map(str, query_embedding)) + "]"

    where_clauses = ["metadata->>'language' = %s", "1 - (embedding <=> %s::vector) >= %s"]
    params: List[Any] = [query_language, embedding_str, similarity_threshold]

    if s3_prefix:
        where_clauses.append("s3_key LIKE %s")
        params.append(s3_prefix + "%")

    where_sql = " AND ".join(where_clauses)

    sql = f"""
        SELECT
            id, s3_key, filename, chunk_index, chunk_text, metadata,
            1 - (embedding <=> %s::vector) as similarity
        FROM documents
        WHERE {where_sql}
        ORDER BY embedding <=> %s::vector
        LIMIT %s
    """

    params2 = [embedding_str] + params + [embedding_str, top_k]

    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(sql, params2)
        rows = cursor.fetchall()

    results = []
    for row in rows:
        md = row.get("metadata") or {}
        results.append(
            {
                "id": row.get("id"),
                "s3_key": row.get("s3_key"),
                "filename": row.get("filename"),
                "chunk_index": row.get("chunk_index"),
                "similarity": float(row.get("similarity", 0.0)),
                "language": md.get("language"),
                "source_language": md.get("source_language"),
                "ocr": md.get("ocr"),
                "page": md.get("page"),
                "excerpt": (row.get("chunk_text") or "")[:200].replace("\n", " ") + "...",
            }
        )

    return results


def load_queries(path: str) -> List[Tuple[str, str]]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    queries: List[Tuple[str, str]] = []
    for item in data:
        q = (item.get("q") or "").strip()
        lang = (item.get("language") or "").strip()
        if q and lang:
            queries.append((lang, q))

    if not queries:
        raise RuntimeError("No queries found. Expected JSON like: [{\"language\":\"en\",\"q\":\"...\"}, ...]")

    return queries


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--queries-json", default=None)
    parser.add_argument("--query", action="append", default=None)
    parser.add_argument("--language", choices=["en", "ja"], default=None)
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--threshold", type=float, default=0.7)
    parser.add_argument("--pilot-prefix", default="pilot/ocr/")
    args = parser.parse_args()

    queries: List[Tuple[str, str]] = []
    if args.queries_json:
        queries = load_queries(args.queries_json)
    else:
        if not args.query or not args.language:
            raise RuntimeError("Provide either --queries-json, or both --language and one/more --query")
        queries = [(args.language, q) for q in args.query]

    openai_client = get_openai_client()
    conn = get_db_connection()

    try:
        hit = 0
        total = 0

        for (lang, q) in queries:
            total += 1
            emb = get_embedding(openai_client, q)
            results = vector_search(
                conn,
                query_embedding=emb,
                query_language=lang,
                top_k=args.top_k,
                similarity_threshold=args.threshold,
                s3_prefix=args.pilot_prefix,
            )

            ok = len(results) > 0
            if ok:
                hit += 1

            print("=" * 80)
            print(f"Q[{lang}]: {q}")
            print(f"Pilot hit: {ok} (top_k={args.top_k}, threshold={args.threshold}, prefix={args.pilot_prefix})")

            for i, r in enumerate(results, start=1):
                print(
                    f"  {i}. sim={r['similarity']:.3f} file={r['filename']} page={r.get('page')} "
                    f"lang={r.get('language')} src_lang={r.get('source_language')} ocr={r.get('ocr')}"
                )
                print(f"     {r['excerpt']}")

        print("=" * 80)
        print(f"Pilot retrieval hit-rate: {hit}/{total} = {hit/total:.2%}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
