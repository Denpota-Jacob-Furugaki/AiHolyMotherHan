"""
言語メタデータ更新 Lambda

AWS コンソールから「テスト」ボタンで実行できる。
ファイルパスから言語を推測し、全ドキュメントの metadata.language を更新する。
"""
import json
import os
import re

import boto3
import psycopg2
from psycopg2.extras import RealDictCursor

# ============================================================
# 言語判定ルール
# ============================================================

KOREAN_PATTERNS = [
    r"tongilgyo",
    r"통일",
    r"천성경",
    r"평화경",
    r"참부모",
    r"\.ko\.",
    r"_ko_",
    r"/ko/",
]

JAPANESE_PATTERNS = [
    r"utitokyo",
    r"統一思想",
    r"原理",
    r"\.ja\.",
    r"_ja_",
    r"/ja/",
]

ENGLISH_PATTERNS = [
    r"tparents",
    r"Moon-Books",
    r"Moon-Talks",
    r"Library",
    r"\.en\.",
    r"_en_",
    r"/en/",
]


def detect_language(s3_key: str, filename: str) -> str:
    """ファイルパスとファイル名から言語を推測"""
    combined = f"{s3_key} {filename}".lower()
    
    for pattern in KOREAN_PATTERNS:
        if re.search(pattern, combined, re.IGNORECASE):
            return "ko"
    
    for pattern in JAPANESE_PATTERNS:
        if re.search(pattern, combined, re.IGNORECASE):
            return "ja"
    
    for pattern in ENGLISH_PATTERNS:
        if re.search(pattern, combined, re.IGNORECASE):
            return "en"
    
    return "en"


# ============================================================
# DB 接続
# ============================================================

secrets_client = boto3.client("secretsmanager")


def get_db_connection():
    """DB 接続を取得"""
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


# ============================================================
# Lambda ハンドラ
# ============================================================

def lambda_handler(event, context):
    """
    言語メタデータを更新
    
    リクエスト（オプション）:
        { "dry_run": true }  # trueなら更新せず確認のみ
    
    レスポンス:
        { "status": "success", "stats": {...}, "message": "..." }
    """
    dry_run = event.get("dry_run", False)
    
    print(f"[UpdateMetadata] Starting... dry_run={dry_run}")
    
    conn = get_db_connection()
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            # 既存ドキュメントを取得
            cursor.execute("""
                SELECT id, s3_key, filename, metadata
                FROM documents
                ORDER BY id
            """)
            rows = cursor.fetchall()
            
            total = len(rows)
            print(f"[UpdateMetadata] Found {total} documents")
            
            stats = {"ko": 0, "ja": 0, "en": 0, "skipped": 0, "updated": 0}
            
            for row in rows:
                doc_id = row["id"]
                s3_key = row["s3_key"] or ""
                filename = row["filename"] or ""
                metadata = row["metadata"] or {}
                
                # 既に language がある場合はスキップ
                if metadata.get("language"):
                    stats["skipped"] += 1
                    continue
                
                # 言語を推測
                language = detect_language(s3_key, filename)
                stats[language] += 1
                stats["updated"] += 1
                
                # メタデータを更新
                metadata["language"] = language
                
                if not dry_run:
                    cursor.execute(
                        """
                        UPDATE documents
                        SET metadata = %s
                        WHERE id = %s
                        """,
                        (json.dumps(metadata), doc_id),
                    )
            
            if not dry_run:
                conn.commit()
            
            # 更新後の統計を取得
            cursor.execute("""
                SELECT 
                    COALESCE(metadata->>'language', 'unset') as language,
                    COUNT(*) as count
                FROM documents
                GROUP BY metadata->>'language'
                ORDER BY count DESC
            """)
            final_stats = {row["language"]: row["count"] for row in cursor.fetchall()}
            
            message = f"""
言語メタデータ更新{'（ドライラン）' if dry_run else ''}完了

【更新結果】
- 韓国語 (ko): {stats['ko']} 件
- 日本語 (ja): {stats['ja']} 件
- 英語 (en): {stats['en']} 件
- スキップ（既に設定済み）: {stats['skipped']} 件
- 合計更新: {stats['updated']} 件

【現在の状態】
{json.dumps(final_stats, indent=2, ensure_ascii=False)}
""".strip()
            
            print(message)
            
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "status": "success",
                    "dry_run": dry_run,
                    "stats": stats,
                    "final_stats": final_stats,
                    "message": message,
                }, ensure_ascii=False),
            }
            
    except Exception as e:
        print(f"[UpdateMetadata] Error: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "status": "error",
                "error": str(e),
            }),
        }
    finally:
        conn.close()
