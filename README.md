# AI Holy Mother Han - Mini-Han

3ヶ国語対応（英語・日本語・韓国語）の RAG チャットボット。True Parents と統一思想についての質問に、提供されたデータに基づいて回答する。

## クイックスタート

**コーディング不要の手順書**: [STEP_BY_STEP.md](STEP_BY_STEP.md) を参照

各ステップには確認スクリプトがあり、正しく完了したかを確認できます。

## アーキテクチャ

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│    Frontend     │────▶│   API Gateway   │────▶│   Lambda Chat   │
│  (S3 + CF)      │     │  /chat POST     │     │   Handler       │
└─────────────────┘     └─────────────────┘     └────────┬────────┘
                                                         │
                        ┌────────────────────────────────┴────────┐
                        │                                         │
                        ▼                                         ▼
               ┌─────────────────┐                    ┌─────────────────┐
               │ Aurora pgvector │                    │    OpenAI API   │
               │   (documents)   │                    │  - embedding    │
               └─────────────────┘                    │  - chat         │
                                                      └─────────────────┘
```

## 既存 AWS リソース

| リソース | 値 |
|----------|-----|
| S3 (RAG データ) | `rag-pdf-bucket-221646756615-ap-northeast-1` |
| S3 (フロント) | `rag-frontend-221646756615-ap-northeast-1` |
| RDS | `ragstack-ragdatabase4961bfdb-ot23vvg32pbj.cx84scemy9xh.ap-northeast-1.rds.amazonaws.com` |
| DB 名 | `ragdb` |
| DB ユーザ | `ragadmin` |
| DB Secret ARN | `arn:aws:secretsmanager:ap-northeast-1:221646756615:secret:RAGStackRAGDatabaseSecret73-qib29r8jLJhR-jlwM2k` |
| OpenAI Secret ARN | `arn:aws:secretsmanager:ap-northeast-1:221646756615:secret:rag/openai-api-key-4Mrj6W` |
| API Gateway | RAG Query API (`1o0165j9ig`) |

## ディレクトリ構成

```
.
├── .cursor/rules/          # Cursor ルール
│   ├── development-philosophy.mdc
│   └── implementation-discipline.mdc
├── backend/
│   ├── lambda/
│   │   ├── chat_handler.py     # チャット Lambda
│   │   └── requirements.txt
│   └── prompts/
├── frontend/
│   ├── src/
│   │   ├── App.tsx
│   │   ├── App.css
│   │   ├── main.tsx
│   │   └── index.css
│   ├── index.html
│   ├── package.json
│   └── vite.config.ts
├── scripts/
│   └── update_language_metadata.py   # 言語メタデータ更新
├── tongilgyo_archive/      # RAG ソース (Korean)
├── utitokyo_archive/       # RAG ソース (Japanese)
├── malsm_pdfs/             # RAG ソース (Korean PDFs)
├── tparents_pdfs/          # RAG ソース (English PDFs)
└── README.md
```

## セットアップ

### 1. 言語メタデータの更新

既存の `documents` テーブルに言語タグを付与する:

```bash
cd scripts

# 環境変数を設定
export DB_HOST=ragstack-ragdatabase4961bfdb-ot23vvg32pbj.cx84scemy9xh.ap-northeast-1.rds.amazonaws.com
export DB_PORT=5432
export DB_NAME=ragdb
export DB_USER=ragadmin
export DB_PASSWORD=<Secrets Manager から取得>

# ドライラン（変更を確認）
python update_language_metadata.py --dry-run

# 実行
python update_language_metadata.py
```

### 2. Lambda のデプロイ

```bash
cd backend/lambda

# 依存関係をインストール
pip install -r requirements.txt -t .

# ZIP 作成
zip -r chat_handler.zip .

# Lambda にアップロード（AWS CLI または コンソール）
aws lambda update-function-code \
  --function-name <your-chat-lambda-name> \
  --zip-file fileb://chat_handler.zip
```

Lambda の環境変数:
- `DB_HOST`: RDS エンドポイント
- `DB_PORT`: 5432
- `DB_NAME`: ragdb
- `DB_USER`: ragadmin
- `DB_SECRET_ARN`: DB シークレット ARN
- `OPENAI_SECRET_ARN`: OpenAI シークレット ARN
- `CHAT_MODEL`: gpt-4o（デフォルト）

### 3. フロントエンドのビルドとデプロイ

```bash
cd frontend

# 依存関係をインストール
npm install

# ビルド
npm run build

# S3 にデプロイ
aws s3 sync dist/ s3://rag-frontend-221646756615-ap-northeast-1/ --delete
```

### 4. API Gateway の設定

1. 「RAG Query API」を開く
2. `/chat` リソースを作成
3. POST メソッドを追加し、Lambda 統合を設定
4. CORS を有効化
5. デプロイ

## API 仕様

### POST /chat

リクエスト:
```json
{
  "message": "What is the Divine Principle?",
  "language": "en"
}
```

`language`: `"en"` | `"ja"` | `"ko"`

レスポンス:
```json
{
  "reply": "The Divine Principle is...",
  "sources": [
    {
      "index": 1,
      "filename": "divine_principle.pdf",
      "s3_key": "tparents/divine_principle.pdf",
      "language": "en",
      "similarity": 0.85,
      "excerpt": "The Divine Principle explains..."
    }
  ],
  "language": "en"
}
```

## 3ヶ国語対応の仕組み

1. **データ側**: 各チャンクに `metadata.language`（ja/ko/en）を付与
2. **検索**: 言語フィルタなしで全言語を検索（意味的類似度で取得）
3. **応答**: システムプロンプトで「必ずクエリ言語で応答」「他言語のソースは翻訳・要約して引用」を指示

## 開発ルール

- `.cursor/rules/development-philosophy.mdc`: 設計優先の方針
- `.cursor/rules/implementation-discipline.mdc`: 実装規律

詳細はファイルを参照。
