#!/usr/bin/env python3
"""
韓国語ドキュメントのメタデータを更新するスクリプト

使い方:
    python3 update_korean_metadata.py

処理完了まで定期的に実行してください（5-10分ごと）
"""

import boto3
import json
import urllib.request
import zipfile
import io
import time
import os

# AWS 認証情報
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.environ.get('AWS_DEFAULT_REGION', 'ap-northeast-1')

def main():
    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
        raise RuntimeError("Missing AWS credentials. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables.")

    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    client = session.client('lambda')
    lambda_name = 'RAGStack-DBInitBA93BBC3-I69p0LBYUaet'

    print("=== 韓国語メタデータ更新 ===\n")

    # 現在のコードを保存
    response = client.get_function(FunctionName=lambda_name)
    original_code_url = response['Code']['Location']
    with urllib.request.urlopen(original_code_url) as resp:
        original_zip = resp.read()

    # 更新コード
    update_code = '''
import json, os, boto3, psycopg2
from psycopg2.extras import RealDictCursor
secrets_client = boto3.client("secretsmanager")
def lambda_handler(event, context):
    try:
        secret_arn = os.environ.get("DB_SECRET_ARN")
        response = secrets_client.get_secret_value(SecretId=secret_arn)
        secret = json.loads(response["SecretString"])
        conn = psycopg2.connect(host=os.environ.get("DB_HOST"),port=os.environ.get("DB_PORT", "5432"),dbname=os.environ.get("DB_NAME", "ragdb"),user=os.environ.get("DB_USER", "ragadmin"),password=secret.get("password", secret.get("ragadmin")),)
        
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            # malsmCollection -> ko
            cursor.execute("""
                UPDATE documents 
                SET metadata = COALESCE(metadata, '{}'::jsonb) || '{"language": "ko"}'::jsonb
                WHERE s3_key LIKE '%malsmCollection%'
                AND (metadata->>'language' IS NULL OR metadata->>'language' != 'ko')
            """)
            malsm_updated = cursor.rowcount
            
            # tongilgyo -> ko
            cursor.execute("""
                UPDATE documents 
                SET metadata = COALESCE(metadata, '{}'::jsonb) || '{"language": "ko"}'::jsonb
                WHERE s3_key LIKE '%tongilgyo%'
                AND (metadata->>'language' IS NULL OR metadata->>'language' != 'ko')
            """)
            tongilgyo_updated = cursor.rowcount
            
            conn.commit()
            
            # 統計取得
            cursor.execute("SELECT COALESCE(metadata->>'language', 'unset') as lang, COUNT(*) as cnt FROM documents GROUP BY metadata->>'language' ORDER BY cnt DESC")
            stats = {r['lang']: r['cnt'] for r in cursor.fetchall()}
            
            # malsmCollection のチャンク数
            cursor.execute("SELECT COUNT(*) as cnt FROM documents WHERE s3_key LIKE '%malsmCollection%'")
            malsm_total = cursor.fetchone()['cnt']
            
            return {"statusCode": 200, "body": json.dumps({
                "malsm_updated": malsm_updated,
                "tongilgyo_updated": tongilgyo_updated,
                "malsm_total": malsm_total,
                "stats": stats
            }, ensure_ascii=False)}
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
'''

    # Lambda を一時的に更新
    new_zip_buffer = io.BytesIO()
    with zipfile.ZipFile(io.BytesIO(original_zip), 'r') as original:
        with zipfile.ZipFile(new_zip_buffer, 'w', zipfile.ZIP_DEFLATED) as new_zip:
            for item in original.namelist():
                if item == 'lambda_function.py':
                    new_zip.writestr(item, update_code)
                else:
                    new_zip.writestr(item, original.read(item))
    new_zip_buffer.seek(0)
    client.update_function_code(FunctionName=lambda_name, ZipFile=new_zip_buffer.read())
    time.sleep(5)

    # 実行
    response = client.invoke(FunctionName=lambda_name, InvocationType='RequestResponse', Payload='{}')
    result = json.loads(response['Payload'].read())

    if result.get('statusCode') == 200:
        body = json.loads(result.get('body', '{}'))
        print(f"malsmCollection 新規更新: {body.get('malsm_updated', 0)} 件")
        print(f"malsmCollection 合計: {body.get('malsm_total', 0)} チャンク")
        print(f"\n言語別ドキュメント数:")
        for lang, cnt in body.get('stats', {}).items():
            print(f"  {lang}: {cnt:,}")
    else:
        print(f"エラー: {result}")

    # 元に戻す
    client.update_function_code(FunctionName=lambda_name, ZipFile=original_zip)
    print("\n✅ 完了")

if __name__ == '__main__':
    main()
