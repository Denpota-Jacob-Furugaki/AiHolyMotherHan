#!/usr/bin/env python3
"""
既存翻訳の用語を修正するスクリプト

DB内の翻訳済みドキュメントで、韓国語のまま残っている用語を
正しい英語に置換する。

Usage:
    python fix_translations.py
"""

import boto3
import json
import time
import zipfile
import urllib.request
import os

# Load glossary
GLOSSARY_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "glossary.json")

def load_glossary():
    with open(GLOSSARY_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    glossary = {}
    for key, value in data.items():
        if key.startswith("_"):
            continue  # Skip comments
        if "ko_to_en" in key and isinstance(value, dict):
            glossary.update(value)
    return glossary

def main():
    print("=== 翻訳用語修正スクリプト ===\n")
    
    # Load glossary
    glossary = load_glossary()
    print(f"用語集: {len(glossary)} 項目")
    
    # AWS clients
    lambda_client = boto3.client("lambda", region_name="ap-northeast-1")
    
    # Generate SQL updates for filename field
    filename_updates = []
    for korean, english in glossary.items():
        filename_updates.append(f"""
            UPDATE documents 
            SET filename = REPLACE(filename, '{korean}', '{english}')
            WHERE filename LIKE '%{korean}%'
        """)
    
    # Generate SQL updates for metadata fields
    metadata_updates = []
    for korean, english in glossary.items():
        metadata_updates.append(f"""
            UPDATE documents 
            SET metadata = jsonb_set(
                metadata,
                '{{speech_location}}',
                to_jsonb(REPLACE(metadata->>'speech_location', '{korean}', '{english}'))
            )
            WHERE metadata->>'speech_location' LIKE '%{korean}%'
        """)
    
    # Create Lambda code to run the updates
    update_code = f'''
import json
import os
import boto3
import psycopg2

secrets_client = boto3.client("secretsmanager")

GLOSSARY = {json.dumps(glossary, ensure_ascii=False)}

def lambda_handler(event, context):
    try:
        secret_arn = os.environ.get("DB_SECRET_ARN")
        response = secrets_client.get_secret_value(SecretId=secret_arn)
        secret = json.loads(response["SecretString"])
        
        conn = psycopg2.connect(
            host=os.environ.get("DB_HOST"),
            port=os.environ.get("DB_PORT", "5432"),
            dbname=os.environ.get("DB_NAME", "ragdb"),
            user=os.environ.get("DB_USER", "ragadmin"),
            password=secret.get("password", secret.get("ragadmin")),
        )
        
        total_updated = 0
        
        with conn.cursor() as cursor:
            for korean, english in GLOSSARY.items():
                # Update filename
                cursor.execute("""
                    UPDATE documents 
                    SET filename = REPLACE(filename, %s, %s)
                    WHERE filename LIKE %s
                """, (korean, english, f"%{{korean}}%"))
                total_updated += cursor.rowcount
                
                # Update speech_location in metadata
                cursor.execute("""
                    UPDATE documents 
                    SET metadata = jsonb_set(
                        COALESCE(metadata, '{{}}'::jsonb),
                        '{{speech_location}}',
                        to_jsonb(REPLACE(COALESCE(metadata->>'speech_location', ''), %s, %s))
                    )
                    WHERE metadata->>'speech_location' LIKE %s
                """, (korean, english, f"%{{korean}}%"))
                total_updated += cursor.rowcount
            
            conn.commit()
        
        conn.close()
        return {{"statusCode": 200, "body": json.dumps({{"updated": total_updated}})}}
    except Exception as e:
        import traceback
        return {{"statusCode": 500, "body": json.dumps({{"error": str(e), "trace": traceback.format_exc()}})}}
'''
    
    print("\nLambda で DB を更新中...")
    
    # Get current Lambda code
    current = lambda_client.get_function(FunctionName="mini-han-chat")
    urllib.request.urlretrieve(current["Code"]["Location"], "/tmp/mhc-fix.zip")
    
    # Create temp Lambda
    with zipfile.ZipFile("/tmp/mhc-fix.zip", "r") as zin:
        with zipfile.ZipFile("/tmp/mhc-fix-new.zip", "w") as zout:
            for item in zin.infolist():
                if item.filename != "lambda_function.py":
                    zout.writestr(item, zin.read(item.filename))
            zout.writestr("lambda_function.py", update_code)
    
    with open("/tmp/mhc-fix-new.zip", "rb") as f:
        lambda_client.update_function_code(FunctionName="mini-han-chat", ZipFile=f.read())
    
    print("Lambda 更新待ち...")
    time.sleep(10)
    
    # Invoke
    response = lambda_client.invoke(
        FunctionName="mini-han-chat",
        InvocationType="RequestResponse",
        Payload=json.dumps({})
    )
    
    result = json.loads(response["Payload"].read())
    print(f"結果: {result}")
    
    # Restore original Lambda
    print("\nLambda を復元中...")
    with open("/tmp/mhc-fix.zip", "rb") as f:
        lambda_client.update_function_code(FunctionName="mini-han-chat", ZipFile=f.read())
    
    print("完了 ✓")


if __name__ == "__main__":
    main()
