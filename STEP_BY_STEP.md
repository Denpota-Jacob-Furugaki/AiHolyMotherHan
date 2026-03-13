# Mini-Han 構築手順書（コーディング不要）

このガイドでは、コマンドをコピー＆ペーストするだけで作業を進められます。
各ステップには「確認ポイント」があり、正しく完了したかを確認できます。

---

## 事前準備

### 必要なもの
- ターミナル（Mac の場合は「ターミナル」アプリ）
- Python 3（Mac には標準でインストール済み）
- AWS CLI 認証情報（Access Key / Secret Key）

### Python パッケージのインストール

ターミナルを開いて、以下をコピー＆ペーストして実行:

```bash
pip3 install boto3 psycopg2-binary openai
```

**確認ポイント**: エラーなく完了すれば OK

---

## Step 1: 言語メタデータの更新

### 1-1. プロジェクトフォルダに移動

```bash
cd "/Users/denpotafurugaki/Documents/AI Coding/AIHolyMotherHan/scripts"
```

### 1-2. AWS 認証情報を設定

```bash
export AWS_ACCESS_KEY_ID=あなたのAccessKey
export AWS_SECRET_ACCESS_KEY=あなたのSecretKey
export AWS_DEFAULT_REGION=ap-northeast-1
```

⚠️ **注意**: `あなたのAccessKey` と `あなたのSecretKey` は実際の値に置き換えてください

### 1-3. DB パスワードを取得

```bash
python3 get_db_password.py
```

**確認ポイント**: 
- `✅ パスワードを取得しました` と表示される
- `export DB_PASSWORD="..."` の形式で出力される

表示されたコマンドをコピーして実行してください。

### 1-4. DB 接続情報を設定

```bash
export DB_HOST=ragstack-ragdatabase4961bfdb-ot23vvg32pbj.cx84scemy9xh.ap-northeast-1.rds.amazonaws.com
export DB_PORT=5432
export DB_NAME=ragdb
export DB_USER=ragadmin
```

### 1-5. 現状を確認（ドライラン前）

```bash
python3 verify_status.py
```

**確認ポイント**:
- `✅ OK データベースに接続できる` と表示される
- 言語別の件数が表示される（最初は「未設定」が多いはず）

### 1-6. 言語メタデータ更新（ドライラン）

まず、実際に変更せずに確認だけ行います:

```bash
python3 update_language_metadata.py --dry-run
```

**確認ポイント**:
- `[DRY RUN]` と表示される
- 各ドキュメントがどの言語に判定されるか表示される
- 最後に Summary が表示される

### 1-7. 言語メタデータ更新（本番実行）

問題なければ本番実行:

```bash
python3 update_language_metadata.py
```

**確認ポイント**:
- `Language metadata updated successfully!` と表示される
- Summary に Korean/Japanese/English の件数が表示される

### 1-8. 更新後の確認

```bash
python3 verify_status.py
```

**確認ポイント**:
- `✅ OK 言語メタデータが設定されている` と表示される
- 「未設定」が 0 件になっている
- 各言語のサンプルデータが表示される

---

## Step 1 完了の判定基準

以下が全て満たされていれば Step 1 完了:

| チェック項目 | 確認方法 |
|------------|----------|
| DB に接続できる | verify_status.py で `✅ OK` |
| 言語メタデータが設定済み | 「未設定」が 0 件 |
| 日本語データがある | サンプルに日本語が表示される |
| 韓国語データがある | サンプルに韓国語が表示される |
| 英語データがある | サンプルに英語が表示される |

---

## トラブルシューティング

### 「command not found: python3」と出る

Python がインストールされていません。以下を実行:
```bash
brew install python3
```

### 「ModuleNotFoundError: No module named 'boto3'」と出る

パッケージが入っていません。以下を実行:
```bash
pip3 install boto3 psycopg2-binary
```

### 「Connection refused」や「timeout」と出る

DB への接続がブロックされています。以下を確認:
- VPN を使っている場合は切断してみる
- AWS のセキュリティグループで接続元 IP が許可されているか確認

### 「Access Denied」と出る

AWS 認証情報が正しくありません。以下を確認:
- Access Key / Secret Key が正しいか
- 新しいキーを発行していれば、古いキーを使っていないか

---

## 次のステップ

Step 1 が完了したら、以下を私（AI）に伝えてください:

「Step 1 完了。verify_status.py の結果は〇〇でした」

結果を見て、Step 2（Lambda デプロイ）の手順を案内します。
