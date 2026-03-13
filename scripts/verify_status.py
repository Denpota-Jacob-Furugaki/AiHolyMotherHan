#!/usr/bin/env python3
"""
進捗確認スクリプト - コーディング不要で現状を確認できる

使い方:
    export DB_HOST=ragstack-ragdatabase4961bfdb-ot23vvg32pbj.cx84scemy9xh.ap-northeast-1.rds.amazonaws.com
    export DB_PORT=5432
    export DB_NAME=ragdb
    export DB_USER=ragadmin
    export DB_PASSWORD=<password>
    python verify_status.py

出力: 各チェック項目の OK / NG と、人間が読める説明
"""
import os
import sys

def print_header(title: str):
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)

def print_check(name: str, passed: bool, details: str = ""):
    status = "✅ OK" if passed else "❌ NG"
    print(f"\n{status}  {name}")
    if details:
        for line in details.strip().split("\n"):
            print(f"      {line}")

def main():
    print_header("Mini-Han 進捗確認レポート")
    
    # --------------------------------------------------
    # 1. DB 接続確認
    # --------------------------------------------------
    print_header("1. データベース接続")
    
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor
        
        conn = psycopg2.connect(
            host=os.environ.get("DB_HOST"),
            port=os.environ.get("DB_PORT", "5432"),
            dbname=os.environ.get("DB_NAME", "ragdb"),
            user=os.environ.get("DB_USER", "ragadmin"),
            password=os.environ.get("DB_PASSWORD"),
        )
        print_check("データベースに接続できる", True, "Aurora PostgreSQL に正常に接続しました")
        
    except Exception as e:
        print_check("データベースに接続できる", False, f"エラー: {str(e)}")
        print("\n⚠️ DB に接続できないため、以降のチェックをスキップします")
        return 1
    
    # --------------------------------------------------
    # 2. documents テーブルの状態
    # --------------------------------------------------
    print_header("2. documents テーブルの状態")
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            # 総件数
            cursor.execute("SELECT COUNT(*) as total FROM documents")
            total = cursor.fetchone()["total"]
            print_check(
                "ドキュメントが存在する",
                total > 0,
                f"合計 {total:,} 件のチャンクがあります"
            )
            
            # 言語メタデータの状態
            cursor.execute("""
                SELECT 
                    COALESCE(metadata->>'language', '未設定') as language,
                    COUNT(*) as count
                FROM documents
                GROUP BY metadata->>'language'
                ORDER BY count DESC
            """)
            lang_stats = cursor.fetchall()
            
            has_language = any(row["language"] in ("ja", "ko", "en") for row in lang_stats)
            unset_count = sum(row["count"] for row in lang_stats if row["language"] == "未設定")
            
            details = "言語別の件数:\n"
            for row in lang_stats:
                lang_name = {
                    "ja": "日本語 (ja)",
                    "ko": "韓国語 (ko)", 
                    "en": "英語 (en)",
                    "未設定": "⚠️ 未設定",
                }.get(row["language"], row["language"])
                details += f"  - {lang_name}: {row['count']:,} 件\n"
            
            print_check(
                "言語メタデータが設定されている",
                has_language and unset_count == 0,
                details
            )
            
            if unset_count > 0:
                print(f"\n      💡 ヒント: {unset_count:,} 件に言語が未設定です")
                print("         → update_language_metadata.py を実行してください")
            
            # サンプルデータ表示
            print_header("3. サンプルデータ（各言語から1件ずつ）")
            
            for lang in ["ja", "ko", "en"]:
                cursor.execute("""
                    SELECT filename, chunk_text
                    FROM documents
                    WHERE metadata->>'language' = %s
                    LIMIT 1
                """, (lang,))
                row = cursor.fetchone()
                
                if row:
                    lang_name = {"ja": "日本語", "ko": "韓国語", "en": "英語"}[lang]
                    excerpt = row["chunk_text"][:100].replace("\n", " ") + "..."
                    print(f"\n  【{lang_name}】")
                    print(f"      ファイル: {row['filename']}")
                    print(f"      内容: {excerpt}")
                else:
                    lang_name = {"ja": "日本語", "ko": "韓国語", "en": "英語"}[lang]
                    print(f"\n  【{lang_name}】")
                    print(f"      ⚠️ この言語のドキュメントがありません")
                    
    except Exception as e:
        print_check("テーブルの状態を確認できる", False, f"エラー: {str(e)}")
    
    finally:
        conn.close()
    
    # --------------------------------------------------
    # サマリー
    # --------------------------------------------------
    print_header("確認完了")
    print("""
次のステップ:
  1. 言語メタデータが「未設定」の場合
     → python update_language_metadata.py を実行
     
  2. 全て OK の場合
     → Lambda のデプロイに進む
""")
    
    return 0

if __name__ == "__main__":
    # 環境変数チェック
    required = ["DB_HOST", "DB_PASSWORD"]
    missing = [v for v in required if not os.environ.get(v)]
    
    if missing:
        print("❌ 環境変数が設定されていません:")
        for v in missing:
            print(f"   - {v}")
        print("\n使い方:")
        print("  export DB_HOST=...")
        print("  export DB_PASSWORD=...")
        print("  python verify_status.py")
        sys.exit(1)
    
    sys.exit(main())
