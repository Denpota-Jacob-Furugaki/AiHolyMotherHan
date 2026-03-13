#!/usr/bin/env python3
"""
言語メタデータ更新スクリプト

既存の documents テーブルに language メタデータを付与する。
ファイルパス・ファイル名から言語を推測し、metadata JSONB に追加する。

使い方:
    export DB_HOST=ragstack-ragdatabase4961bfdb-ot23vvg32pbj.cx84scemy9xh.ap-northeast-1.rds.amazonaws.com
    export DB_PORT=5432
    export DB_NAME=ragdb
    export DB_USER=ragadmin
    export DB_PASSWORD=<password>
    python update_language_metadata.py
"""
import os
import json
import re
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor

# ============================================================
# 言語判定ルール
# ============================================================

# 韓国語ソース（tongilgyo, 韓国語ファイル名）
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

# 日本語ソース（utitokyo, 日本語ファイル名）
JAPANESE_PATTERNS = [
    r"utitokyo",
    r"統一思想",
    r"原理",
    r"\.ja\.",
    r"_ja_",
    r"/ja/",
]

# 英語ソース（tparents, 英語サイト）
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
    """
    ファイルパスとファイル名から言語を推測
    
    優先順位: 明示的なパターン → パス由来 → デフォルト(en)
    """
    combined = f"{s3_key} {filename}".lower()
    
    # 韓国語パターンチェック
    for pattern in KOREAN_PATTERNS:
        if re.search(pattern, combined, re.IGNORECASE):
            return "ko"
    
    # 日本語パターンチェック
    for pattern in JAPANESE_PATTERNS:
        if re.search(pattern, combined, re.IGNORECASE):
            return "ja"
    
    # 英語パターンチェック
    for pattern in ENGLISH_PATTERNS:
        if re.search(pattern, combined, re.IGNORECASE):
            return "en"
    
    # デフォルトは英語
    return "en"


# ============================================================
# DB 操作
# ============================================================

def get_db_connection():
    """DB 接続を取得"""
    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=os.environ.get("DB_PORT", "5432"),
        dbname=os.environ.get("DB_NAME", "ragdb"),
        user=os.environ.get("DB_USER", "ragadmin"),
        password=os.environ["DB_PASSWORD"],
    )


def update_language_metadata(dry_run: bool = False):
    """
    全ドキュメントの language メタデータを更新
    """
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
            
            print(f"Found {len(rows)} documents to process")
            
            stats = {"ko": 0, "ja": 0, "en": 0, "skipped": 0}
            
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
                
                # メタデータを更新
                metadata["language"] = language
                
                if dry_run:
                    print(f"[DRY RUN] id={doc_id} -> {language} ({filename[:50]}...)")
                else:
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
            
            print("\n=== Summary ===")
            print(f"Korean (ko): {stats['ko']}")
            print(f"Japanese (ja): {stats['ja']}")
            print(f"English (en): {stats['en']}")
            print(f"Skipped (already has language): {stats['skipped']}")
            
            if dry_run:
                print("\n[DRY RUN] No changes were made. Run without --dry-run to apply.")
            else:
                print("\nLanguage metadata updated successfully!")
                
    finally:
        conn.close()


# ============================================================
# メイン
# ============================================================

if __name__ == "__main__":
    import sys
    
    dry_run = "--dry-run" in sys.argv
    
    if dry_run:
        print("=== DRY RUN MODE ===\n")
    
    update_language_metadata(dry_run=dry_run)
