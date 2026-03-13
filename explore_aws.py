#!/usr/bin/env python3
"""
AWS 探索スクリプト - 与えた IAM 情報を環境変数に設定して実行する。

使い方（Tokyo リージョン）:
  export AWS_ACCESS_KEY_ID=あなたのAccessKey
  export AWS_SECRET_ACCESS_KEY=あなたのSecretKey
  export AWS_DEFAULT_REGION=ap-northeast-1
  python3 explore_aws.py

事前に boto3 が必要:  pip install boto3

出力: STS・S3・Lambda・RDS・API Gateway・Secrets Manager の一覧（読み取りのみ）。
"""
import os
import json
from datetime import datetime

def json_serial(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(repr(obj) + " is not JSON serializable")

def main():
    try:
        import boto3
    except ImportError:
        print("boto3 が入っていません: pip install boto3")
        return 1

    region = os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-1")
    if not os.environ.get("AWS_ACCESS_KEY_ID") or not os.environ.get("AWS_SECRET_ACCESS_KEY"):
        print("環境変数 AWS_ACCESS_KEY_ID と AWS_SECRET_ACCESS_KEY を設定してください")
        return 1

    session = boto3.Session(region_name=region)
    out = []

    # --- STS: 呼び出し元の識別 ---
    try:
        sts = session.client("sts")
        identity = sts.get_caller_identity()
        out.append("## 呼び出し元 (STS get_caller_identity)")
        out.append(json.dumps(identity, indent=2, default=json_serial))
        out.append("")
    except Exception as e:
        out.append("## STS エラー: " + str(e))
        out.append("")

    # --- S3: バケット一覧 ---
    s3 = session.client("s3")
    try:
        buckets = s3.list_buckets().get("Buckets", [])
        out.append("## S3 バケット一覧")
        for b in buckets:
            out.append("  - " + b["Name"] + " (作成: " + str(b.get("CreationDate", "")) + ")")
        out.append("")
    except Exception as e:
        out.append("## S3 list_buckets エラー: " + str(e))
        out.append("")

    paginator = s3.get_paginator("list_objects_v2")

    # --- S3: RAG PDF バケットの中身（プレフィックス一覧） ---
    rag_bucket = "rag-pdf-bucket-221646756615-ap-northeast-1"
    try:
        prefixes = set()
        for page in paginator.paginate(Bucket=rag_bucket, Delimiter="/"):
            for p in page.get("CommonPrefixes", []):
                prefixes.add(p["Prefix"])
        out.append("## S3 " + rag_bucket + " のトッププレフィックス (Delimiter=/)")
        for p in sorted(prefixes):
            out.append("  - " + p)
        if not prefixes:
            # プレフィックスが無ければオブジェクトを最大 20 件
            objs = s3.list_objects_v2(Bucket=rag_bucket, MaxKeys=20).get("Contents", [])
            out.append("  (CommonPrefixes なし。先頭オブジェクト例)")
            for o in objs[:20]:
                out.append("  - " + o.get("Key", "") + "  " + str(o.get("Size", 0)) + " bytes")
        out.append("")
    except Exception as e:
        out.append("## S3 " + rag_bucket + " エラー: " + str(e))
        out.append("")

    # --- S3: フロントバケットの中身 ---
    front_bucket = "rag-frontend-221646756615-ap-northeast-1"
    try:
        prefixes = set()
        for page in paginator.paginate(Bucket=front_bucket, Delimiter="/"):
            for p in page.get("CommonPrefixes", []):
                prefixes.add(p["Prefix"])
        out.append("## S3 " + front_bucket + " のトッププレフィックス")
        for p in sorted(prefixes):
            out.append("  - " + p)
        objs = s3.list_objects_v2(Bucket=front_bucket, MaxKeys=50).get("Contents", [])
        if objs and not prefixes:
            out.append("  (先頭オブジェクト例)")
            for o in objs[:30]:
                out.append("  - " + o.get("Key", "") + "  " + str(o.get("Size", 0)) + " bytes")
        out.append("")
    except Exception as e:
        out.append("## S3 " + front_bucket + " エラー: " + str(e))
        out.append("")

    # --- Lambda: 関数一覧 ---
    try:
        lam = session.client("lambda")
        funcs = lam.list_functions().get("Functions", [])
        out.append("## Lambda 関数一覧")
        for f in funcs:
            out.append("  - " + f.get("FunctionName", "") + "  Runtime=" + str(f.get("Runtime", "")) + "  Handler=" + str(f.get("Handler", "")))
        out.append("")
    except Exception as e:
        out.append("## Lambda list_functions エラー: " + str(e))
        out.append("")

    # --- RDS: DB インスタンス（Aurora 含む） ---
    try:
        rds = session.client("rds")
        instances = rds.describe_db_instances().get("DBInstances", [])
        out.append("## RDS / Aurora インスタンス")
        for db in instances:
            out.append("  - " + db.get("DBInstanceIdentifier", "") + "  Engine=" + str(db.get("Engine", "")) + "  Endpoint=" + str(db.get("Endpoint", {}).get("Address", "")))
        out.append("")
    except Exception as e:
        out.append("## RDS describe_db_instances エラー: " + str(e))
        out.append("")

    # --- API Gateway: REST API 一覧 ---
    try:
        apigw = session.client("apigateway")
        apis = apigw.get_rest_apis().get("items", [])
        out.append("## API Gateway REST APIs")
        for api in apis:
            out.append("  - " + api.get("name", "") + "  id=" + api.get("id", ""))
        out.append("")
    except Exception as e:
        out.append("## API Gateway get_rest_apis エラー: " + str(e))
        out.append("")

    # --- API Gateway v2: HTTP APIs ---
    try:
        apigw2 = session.client("apigatewayv2")
        apis2 = apigw2.get_apis().get("Items", [])
        out.append("## API Gateway HTTP APIs (v2)")
        for api in apis2:
            out.append("  - " + api.get("Name", "") + "  ApiId=" + api.get("ApiId", "") + "  ApiEndpoint=" + str(api.get("ApiEndpoint", "")))
        out.append("")
    except Exception as e:
        out.append("## API Gateway v2 get_apis エラー: " + str(e))
        out.append("")

    # --- Secrets Manager: シークレット名一覧（値は出さない） ---
    try:
        sm = session.client("secretsmanager")
        secrets = sm.list_secrets().get("SecretList", [])
        out.append("## Secrets Manager シークレット名（ARN のみ）")
        for s in secrets:
            out.append("  - " + s.get("Name", "") + "  ARN=" + s.get("ARN", "")[:80] + "...")
        out.append("")
    except Exception as e:
        out.append("## Secrets Manager list_secrets エラー: " + str(e))
        out.append("")

    print("\n".join(out))
    return 0

if __name__ == "__main__":
    exit(main())
