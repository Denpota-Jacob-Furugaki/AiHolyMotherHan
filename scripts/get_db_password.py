#!/usr/bin/env python3
"""
DB パスワードを Secrets Manager から取得するスクリプト

使い方:
    # AWS認証情報を設定してから実行
    export AWS_ACCESS_KEY_ID=...
    export AWS_SECRET_ACCESS_KEY=...
    export AWS_DEFAULT_REGION=ap-northeast-1
    
    python get_db_password.py

出力: DB_PASSWORD として使える値
"""
import json

def main():
    try:
        import boto3
    except ImportError:
        print("❌ boto3 がインストールされていません")
        print("   pip install boto3 を実行してください")
        return 1
    
    secret_arn = "arn:aws:secretsmanager:ap-northeast-1:221646756615:secret:RAGStackRAGDatabaseSecret73-qib29r8jLJhR-jIwM2k"
    
    print("Secrets Manager からパスワードを取得しています...")
    
    try:
        client = boto3.client("secretsmanager", region_name="ap-northeast-1")
        response = client.get_secret_value(SecretId=secret_arn)
        secret = json.loads(response["SecretString"])
        
        # パスワードを取得（複数のキー名に対応）
        password = secret.get("password") or secret.get("ragadmin")
        
        if password:
            print("\n✅ パスワードを取得しました")
            print("\n以下のコマンドをコピーして実行してください:")
            print("-" * 50)
            print(f'export DB_PASSWORD="{password}"')
            print("-" * 50)
            return 0
        else:
            print("❌ シークレットにパスワードが見つかりません")
            print(f"   シークレットの内容: {list(secret.keys())}")
            return 1
            
    except Exception as e:
        print(f"❌ エラー: {str(e)}")
        print("\nAWS認証情報が正しく設定されているか確認してください:")
        print("  export AWS_ACCESS_KEY_ID=...")
        print("  export AWS_SECRET_ACCESS_KEY=...")
        print("  export AWS_DEFAULT_REGION=ap-northeast-1")
        return 1

if __name__ == "__main__":
    exit(main())
