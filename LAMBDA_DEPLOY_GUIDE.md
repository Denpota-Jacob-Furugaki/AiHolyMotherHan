# Lambda デプロイ手順（AWS コンソールから）

このガイドでは、AWS コンソールで操作するだけで Lambda をデプロイできます。
コマンドラインは使いません。

---

## Step 1: Lambda 関数の作成（言語メタデータ更新用）

### 1-1. AWS コンソールにログイン

1. https://console.aws.amazon.com/ にアクセス
2. ログイン
3. 右上のリージョンが「アジアパシフィック (東京)」になっていることを確認

### 1-2. Lambda を開く

1. 上部の検索バーに「Lambda」と入力
2. 「Lambda」をクリック

### 1-3. 関数を作成

1. 「関数の作成」ボタンをクリック
2. 以下を設定:
   - **関数名**: `mini-han-update-metadata`
   - **ランタイム**: Python 3.11
   - **アーキテクチャ**: x86_64
3. 「詳細設定」を開く
4. 「VPC を有効にする」にチェック
5. VPC の設定:
   - **VPC**: RDS と同じ VPC を選択
   - **サブネット**: プライベートサブネットを選択（複数可）
   - **セキュリティグループ**: RDS へのアクセスを許可しているものを選択
6. 「関数の作成」をクリック

### 1-4. コードをアップロード

1. 関数が作成されたら、「コード」タブを開く
2. `lambda_function.py` の内容を全て削除
3. 以下のファイルの内容をコピー＆ペースト:
   - ファイル: `backend/lambda/update_metadata_handler.py`
4. 「Deploy」ボタンをクリック

### 1-5. レイヤーを追加（psycopg2 用）

Lambda には psycopg2 が入っていないので、レイヤーを追加します:

1. 「コード」タブの下の方にある「レイヤー」セクション
2. 「レイヤーの追加」をクリック
3. 「ARN を指定」を選択
4. 以下の ARN を貼り付け（東京リージョン用）:
   ```
   arn:aws:lambda:ap-northeast-1:898466741470:layer:psycopg2-py311:1
   ```
   ※ これは公開されている psycopg2 レイヤーです
5. 「追加」をクリック

### 1-6. 環境変数を設定

1. 「設定」タブをクリック
2. 左メニューの「環境変数」をクリック
3. 「編集」をクリック
4. 以下の環境変数を追加:

| キー | 値 |
|-----|-----|
| `DB_HOST` | `ragstack-ragdatabase4961bfdb-ot23vvg32pbj.cx84scemy9xh.ap-northeast-1.rds.amazonaws.com` |
| `DB_PORT` | `5432` |
| `DB_NAME` | `ragdb` |
| `DB_USER` | `ragadmin` |
| `DB_SECRET_ARN` | `arn:aws:secretsmanager:ap-northeast-1:221646756615:secret:RAGStackRAGDatabaseSecret73-qib29r8jLJhR-jIwM2k` |

5. 「保存」をクリック

### 1-7. IAM ロールに権限を追加

1. 「設定」タブ → 「アクセス許可」
2. 「ロール名」のリンクをクリック（IAM コンソールが開く）
3. 「ポリシーをアタッチ」をクリック
4. 以下を検索してアタッチ:
   - `SecretsManagerReadWrite`
   - `AmazonVPCFullAccess`
5. または、以下のインラインポリシーを追加:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "secretsmanager:GetSecretValue",
      "Resource": "arn:aws:secretsmanager:ap-northeast-1:221646756615:secret:RAGStackRAGDatabaseSecret73-*"
    }
  ]
}
```

### 1-8. タイムアウトを延長

1. 「設定」タブ → 「一般設定」
2. 「編集」をクリック
3. **タイムアウト**: 5 分に設定
4. **メモリ**: 256 MB に設定
5. 「保存」をクリック

---

## Step 2: 関数をテスト実行

### 2-1. ドライラン（確認のみ）

1. 「テスト」タブをクリック
2. テストイベント名: `DryRun`
3. イベント JSON:
   ```json
   { "dry_run": true }
   ```
4. 「テスト」ボタンをクリック
5. 結果を確認:
   - `"status": "success"` と表示されれば OK
   - 各言語の件数を確認

### 2-2. 本番実行

1. 新しいテストイベントを作成
2. テストイベント名: `UpdateForReal`
3. イベント JSON:
   ```json
   { "dry_run": false }
   ```
4. 「テスト」ボタンをクリック
5. 結果を確認:
   - `"status": "success"` と表示されれば完了
   - `stats` に更新件数が表示される

---

## Step 3: 結果の確認

テスト実行後、「実行結果」に以下のような JSON が表示されます:

```json
{
  "statusCode": 200,
  "body": "{\"status\": \"success\", \"stats\": {\"ko\": 1234, \"ja\": 567, \"en\": 890, ...}}"
}
```

この結果をコピーして私（AI）に共有してください。
次のステップ（チャット API のデプロイ）に進みます。

---

## トラブルシューティング

### 「Task timed out」と出る

- タイムアウトを 5 分以上に延長してください

### 「Connection refused」と出る

- VPC 設定を確認してください
- セキュリティグループで PostgreSQL (5432) が許可されているか確認

### 「Access Denied」と出る

- IAM ロールに Secrets Manager へのアクセス権限があるか確認
