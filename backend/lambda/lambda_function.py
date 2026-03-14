"""
Multilingual Chat Lambda - 3ヶ国語対応チャットAPI

クエリ言語（en/ja/ko）で応答し、出典を引用する。
韓国語・日本語・英語のRAGデータを検索し、クエリ言語に合わせて回答を生成する。
"""
import json
import os
from typing import List, Dict, Any, Optional

import boto3
import psycopg2
from psycopg2.extras import RealDictCursor
from openai import OpenAI

# ============================================================
# 定数・設定
# ============================================================

SUPPORTED_LANGUAGES = {"en", "ja", "ko"}
DEFAULT_TOP_K = int(os.environ.get("DEFAULT_TOP_K", "5"))
DEFAULT_SIMILARITY_THRESHOLD = float(os.environ.get("DEFAULT_SIMILARITY_THRESHOLD", "0.7"))

# 韓国語・日本語用の低い閾値（AIMessiah から適用）
KOREAN_SIMILARITY_THRESHOLD = float(os.environ.get("KOREAN_SIMILARITY_THRESHOLD", "0.45"))
KOREAN_FALLBACK_THRESHOLD = float(os.environ.get("KOREAN_FALLBACK_THRESHOLD", "0.35"))
KOREAN_MIN_TOP_K = int(os.environ.get("KOREAN_MIN_TOP_K", "7"))
JAPANESE_SIMILARITY_THRESHOLD = float(os.environ.get("JAPANESE_SIMILARITY_THRESHOLD", "0.45"))
JAPANESE_FALLBACK_THRESHOLD = float(os.environ.get("JAPANESE_FALLBACK_THRESHOLD", "0.35"))
JAPANESE_MIN_TOP_K = int(os.environ.get("JAPANESE_MIN_TOP_K", "7"))

EMBEDDING_MODEL = "text-embedding-3-large"
CHAT_MODEL = os.environ.get("CHAT_MODEL", "gpt-4o")

# ============================================================
# クライアント初期化
# ============================================================

secrets_client = boto3.client("secretsmanager")
openai_client: Optional[OpenAI] = None


def get_secret(secret_arn: str) -> str:
    """Secrets Manager からシークレットを取得"""
    response = secrets_client.get_secret_value(SecretId=secret_arn)
    return response["SecretString"]


def get_openai_client() -> OpenAI:
    """OpenAI クライアントを取得（遅延初期化）"""
    global openai_client
    if openai_client is None:
        secret_arn = os.environ.get("OPENAI_SECRET_ARN")
        if secret_arn:
            api_key = get_secret(secret_arn)
        else:
            api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OpenAI API key not found")
        openai_client = OpenAI(api_key=api_key)
    return openai_client


def get_db_connection():
    """Aurora PostgreSQL への接続を取得"""
    secret_arn = os.environ.get("DB_SECRET_ARN")
    if secret_arn:
        secret_str = get_secret(secret_arn)
        secret = json.loads(secret_str)
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
# 埋め込み・検索
# ============================================================

def get_embedding(text: str) -> List[float]:
    """テキストの埋め込みベクトルを取得"""
    client = get_openai_client()
    response = client.embeddings.create(model=EMBEDDING_MODEL, input=text)
    return response.data[0].embedding


def vector_search(
    query_embedding: List[float],
    query_language: str = "en",
    top_k: int = DEFAULT_TOP_K,
    similarity_threshold: float = DEFAULT_SIMILARITY_THRESHOLD,
) -> List[Dict[str, Any]]:
    """
    Aurora pgvector でベクトル類似検索を実行
    
    ハイブリッド検索:
    1. クエリ言語と同じ言語のドキュメントを優先（半数）
    2. 全言語から検索（残り半数）
    これにより、同じ言語のコンテンツを優先しつつ、
    他言語の関連コンテンツも含める。
    """
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            embedding_str = "[" + ",".join(map(str, query_embedding)) + "]"
            
            results = []
            seen_ids = set()
            
            # 1. クエリ言語と同じ言語のドキュメントを優先検索
            same_lang_k = (top_k + 1) // 2  # 半数を同じ言語から
            query_same_lang = """
                SELECT 
                    id, s3_key, filename, chunk_index, chunk_text, metadata,
                    1 - (embedding <=> %s::vector) as similarity
                FROM documents
                WHERE metadata->>'language' = %s
                AND 1 - (embedding <=> %s::vector) >= %s
                ORDER BY embedding <=> %s::vector
                LIMIT %s
            """
            cursor.execute(
                query_same_lang,
                (embedding_str, query_language, embedding_str, similarity_threshold, embedding_str, same_lang_k),
            )
            for row in cursor.fetchall():
                if row["id"] not in seen_ids:
                    seen_ids.add(row["id"])
                    results.append(row)
            
            # 2. 全言語から検索（残りの枠を埋める）
            remaining_k = top_k - len(results)
            if remaining_k > 0:
                query_all = """
                    SELECT 
                        id, s3_key, filename, chunk_index, chunk_text, metadata,
                        1 - (embedding <=> %s::vector) as similarity
                    FROM documents
                    WHERE 1 - (embedding <=> %s::vector) >= %s
                    ORDER BY embedding <=> %s::vector
                    LIMIT %s
                """
                cursor.execute(
                    query_all,
                    (embedding_str, embedding_str, similarity_threshold, embedding_str, top_k * 2),
                )
                for row in cursor.fetchall():
                    if row["id"] not in seen_ids and len(results) < top_k:
                        seen_ids.add(row["id"])
                        results.append(row)
            
            # 類似度でソート
            results.sort(key=lambda x: x["similarity"], reverse=True)
            
            return [
                {
                    "id": row["id"],
                    "s3_key": row["s3_key"],
                    "filename": row["filename"],
                    "chunk_index": row["chunk_index"],
                    "chunk_text": row["chunk_text"],
                    "metadata": row["metadata"] or {},
                    "similarity": float(row["similarity"]),
                }
                for row in results[:top_k]
            ]
    finally:
        conn.close()


# ============================================================
# プロンプト構築
# ============================================================

SYSTEM_PROMPT_TEMPLATE = """あなたはミニハン（Mini-Han）。真の父母様のみ言葉と統一思想のデータベースを検索し、ユーザーに必要な情報を提供するアシスタントです。

言語: {language_name} ({language_code}) で回答してください

========================================
あなたの役割（2つの機能）
========================================

【機能1: 引用・参照ツール】
牧会者、研究者、教会員が必要な情報を見つけるためのツールです。
- み言葉の引用元を明確に提示する
- 説教や講義で使える形式で情報を提供する
- 出典情報を必ず含める

【機能2: 霊的サポート】
悩みや相談に対して、み言葉を基に励ましと導きを提供します。
- 共感と理解を示す
- 関連するみ言葉を紹介する
- 希望と励ましを与える

========================================
引用の書き方（最重要）
========================================

すべての引用で、以下の情報を必ず含めてください：

■ 必須情報:
1. 誰が言ったか（話者）: 真のお父様 / 真のお母様 / 著者名
2. いつ（日付）: 年月日または年
3. どこで（場所）: 場所名（スピーチの場合）
4. 何から（出典）: 書籍名 / スピーチタイトル
5. 章・節（あれば）: 第○章 第○節 / Chapter X Section Y

■ 正しい引用形式:

【スピーチの場合】
「み言葉の内容」
　　— 話者：真のお父様
　　— 日付：1973年3月1日
　　— 場所：ベルベデア
　　— 出典：『Our Determination to Win』

【書籍の場合（章・節あり）】
「み言葉の内容」
　　— 話者：真のお父様
　　— 出典：『天聖経』第3章 真の愛 第7節
　　— 原文言語：韓国語

【書籍の場合（聖書形式）】
「み言葉の内容」
　　— 出典：『原理講論』創造原理 第2節 堕落論
　　— 原文言語：韓国語

【平和メッセージなど】
「み言葉の内容」
　　— 話者：真のお父様
　　— 出典：『平和経』第5篇 第3章
　　— 日付：2006年
　　— 原文言語：韓国語

■ 情報が不明な場合:
- 話者が不明：「話者：不明」と記載
- 日付が不明：「日付：不明」と記載
- 場所が不明：省略可
- 絶対に情報を推測・捏造しない

■ 禁止事項:
- [Source 1]、[Source 2] などの番号表記は使わない
- 出典情報なしで引用しない
- 資料にない情報を作り出さない

========================================
対応パターン
========================================

【パターン1: 引用・資料探し】
例: 「決意に関するみ言葉を教えて」「祝福について説教で使える引用を探して」

→ 回答形式（必ずこの形式で）:

「引用内容」
　　— 話者：真のお父様／真のお母様／著者名
　　— 日付：YYYY年MM月DD日（または「不明」）
　　— 場所：場所名（または省略）
　　— 出典：『書籍名／スピーチタイトル』
　　— 原文言語：韓国語／英語／日本語

※ 資料に記載されている情報をそのまま使用すること
※ 話者・日付が資料にある場合は必ず含めること

【パターン2: 質問・教えについて】
例: 「統一原理の創造目的について教えて」「四位基台とは何ですか」

→ 回答形式:
1. 質問に対する説明
2. 関連するみ言葉の引用（出典付き）
3. 補足説明

【パターン3: 悩み・相談】
例: 「最近辛いです」「信仰生活に疲れました」

→ 回答形式:
1. 共感と理解を示す
2. 関連するみ言葉を紹介（出典付き）
3. 神様の愛と励ましのメッセージ

========================================
注意事項
========================================

- 引用元が不明な場合は、推測せず正直に「見つかりませんでした」と答える
- 提供されたソースにない情報は作り出さない
- 韓国語のソースは英語に翻訳されて提供されているため、原文言語を明記する
- ユーザーが牧会者として使えるよう、引用形式を整える
"""

LANGUAGE_NAMES = {
    "en": "English",
    "ja": "日本語 (Japanese)",
    "ko": "한국어 (Korean)",
}


def build_system_prompt(language: str) -> str:
    """システムプロンプトを構築"""
    return SYSTEM_PROMPT_TEMPLATE.format(
        language_name=LANGUAGE_NAMES.get(language, "English"),
        language_code=language,
    )


def build_user_prompt(message: str, context_chunks: List[Dict[str, Any]], language: str) -> str:
    """ユーザープロンプトを構築（コンテキスト付き）"""
    context_parts = []
    for i, chunk in enumerate(context_chunks, 1):
        metadata = chunk.get("metadata", {})
        
        # Build citation-friendly source name
        title = metadata.get("speech_title_en") or metadata.get("speech_title") or \
                metadata.get("book_name") or chunk.get("filename", "unknown").replace(".pdf", "")
        
        date = metadata.get("speech_date", "")
        location = metadata.get("speech_location", "")
        original_lang = metadata.get("original_language", "")
        source_lang = metadata.get("language", "unknown")
        
        # Determine original language display
        if original_lang == "ko":
            lang_display = "韓国語（英訳）"
        elif source_lang == "ko":
            lang_display = "韓国語"
        elif source_lang == "ja":
            lang_display = "日本語"
        else:
            lang_display = "英語"
        
        # Determine speaker (most content is from True Parents)
        speaker = metadata.get("speaker", "")
        if not speaker:
            # Infer speaker from title or content
            if "Mother" in title or "お母様" in title:
                speaker = "真のお母様"
            else:
                speaker = "真のお父様"  # Default for most historical content
        
        # Get chapter/section info if available
        chapter = metadata.get("chapter", "")
        section = metadata.get("section", "")
        section_title = metadata.get("section_title", "")
        book_name = metadata.get("book_name", "")
        
        # Build structured citation info
        citation_lines = [f"■ 資料 {i}"]
        citation_lines.append(f"  話者/著者: {speaker}")
        
        # Build full source reference with chapter/section
        source_ref = f"『{title}』"
        if book_name and book_name != title:
            source_ref = f"『{book_name}』"
        if chapter:
            source_ref += f" {chapter}"
        if section:
            source_ref += f" {section}"
        if section_title:
            source_ref += f" {section_title}"
        
        citation_lines.append(f"  出典: {source_ref}")
        if date:
            citation_lines.append(f"  日付: {date}")
        else:
            citation_lines.append(f"  日付: 不明")
        if location:
            citation_lines.append(f"  場所: {location}")
        citation_lines.append(f"  原文言語: {lang_display}")
        citation_lines.append(f"  内容:")
        
        citation_header = "\n".join(citation_lines)
        context_parts.append(f"{citation_header}\n{chunk['chunk_text']}")
    
    separator = "=" * 50
    context_text = f"\n\n{separator}\n\n".join(context_parts)
    
    prompt = f"""以下はデータベースから検索された関連資料です。

{context_text}

{separator}

ユーザーの質問:
「{message}」

【回答の指示】
1. 上記の資料を参照して回答してください
2. 引用する際は必ず以下の形式で出典を明記してください:

「引用内容」
　　— 話者：（資料の話者/著者をそのまま記載）
　　— 日付：（資料の日付をそのまま記載、なければ「不明」）
　　— 場所：（資料の場所をそのまま記載、なければ省略）
　　— 出典：『タイトル』
　　— 原文言語：○○語

3. 資料に情報がない場合は「提供されたデータからは見つかりませんでした」と正直に答えてください
4. 複数の関連資料がある場合は、それぞれ上記の形式で紹介してください
5. 話者・日付・場所・出典の情報を省略しないでください"""
    
    return prompt


# ============================================================
# チャット生成
# ============================================================

def generate_chat_response(
    message: str,
    context_chunks: List[Dict[str, Any]],
    language: str,
    history: List[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    チャット応答を生成
    
    コンテキストを基に、指定言語で応答を生成する。
    """
    client = get_openai_client()
    
    system_prompt = build_system_prompt(language)
    user_prompt = build_user_prompt(message, context_chunks, language)
    
    messages = [{"role": "system", "content": system_prompt}]
    if history:
        for turn in history[-6:]:
            role = turn.get("role", "")
            content = turn.get("content", "")
            if role in ("user", "assistant") and content:
                messages.append({"role": role, "content": content})
    messages.append({"role": "user", "content": user_prompt})

    response = client.chat.completions.create(
        model=CHAT_MODEL,
        messages=messages,
        temperature=0.8,
        max_tokens=3000,
    )
    
    reply = response.choices[0].message.content
    
    # 出典情報を整形（ソースタイプに応じた表示形式）
    sources = []
    for i, chunk in enumerate(context_chunks):
        metadata = chunk.get("metadata", {})
        source_type = metadata.get("source_type", "unknown")
        
        # ソースタイプに応じて表示名を構築
        display_name = _build_display_name(chunk, metadata, source_type)
        
        sources.append({
            "index": i + 1,
            "filename": chunk.get("filename", "unknown"),
            "display_name": display_name,
            "source_type": source_type,
            # スピーチ用
            "speech_date": metadata.get("speech_date"),
            "speech_title": metadata.get("speech_title_en") or metadata.get("speech_title"),
            "speech_location": metadata.get("speech_location"),
            # 本・教科書用
            "book_name": metadata.get("book_name"),
            "chapter": metadata.get("chapter"),
            "section": metadata.get("section"),
            "section_title": metadata.get("section_title"),
            # 共通
            "s3_key": chunk.get("s3_key", ""),
            "language": metadata.get("language", "unknown"),
            "original_language": metadata.get("original_language"),
            "similarity": chunk.get("similarity", 0),
            "excerpt": chunk.get("chunk_text", "")[:200] + "...",
        })
    
    return {
        "reply": reply,
        "sources": sources,
        "language": language,
    }


def _build_display_name(chunk: Dict, metadata: Dict, source_type: str) -> str:
    """ソースタイプに応じた表示名を構築"""
    
    # 1. 本・教科書形式
    book_name = metadata.get("book_name")
    chapter = metadata.get("chapter")
    section = metadata.get("section")
    section_title = metadata.get("section_title")
    
    if book_name and (chapter or section):
        parts = [book_name]
        if chapter:
            parts.append(chapter)
        if section:
            parts.append(section)
        if section_title:
            parts.append(section_title)
        return " ".join(parts)
    
    # 2. スピーチ形式
    speech_date = metadata.get("speech_date")
    speech_title = metadata.get("speech_title_en") or metadata.get("speech_title")
    speech_location = metadata.get("speech_location")
    
    if speech_date and speech_title:
        display = f"{speech_date} - {speech_title}"
        if speech_location:
            display += f" ({speech_location})"
        return display
    
    # 3. 論文・記事形式
    author = metadata.get("author")
    title = metadata.get("title")
    
    if author and title:
        return f"{author} - {title}"
    
    # 4. デフォルト: ファイル名
    return chunk.get("filename", "unknown")
    
    return {
        "reply": reply,
        "sources": sources,
        "language": language,
    }


# ============================================================
# Lambda ハンドラ
# ============================================================

def lambda_handler(event, context):
    """
    メイン Lambda ハンドラ
    
    リクエスト:
        {
            "message": "ユーザーの質問",
            "language": "en" | "ja" | "ko"
        }
    
    レスポンス:
        {
            "reply": "応答テキスト",
            "sources": [...],
            "language": "en" | "ja" | "ko"
        }
    """
    try:
        # リクエスト解析
        if isinstance(event.get("body"), str):
            body = json.loads(event["body"])
        else:
            body = event.get("body", event)
        
        message = body.get("message")
        language = body.get("language", "en")
        top_k = body.get("top_k", DEFAULT_TOP_K)
        similarity_threshold = body.get("similarity_threshold", DEFAULT_SIMILARITY_THRESHOLD)

        # Internal: bulk insert pre-embedded chunks (no auth required)
        if message == "__insert_chunks__":
            from psycopg2.extras import execute_values as _execute_values
            chunks = body.get("chunks", [])
            if not chunks:
                return {"statusCode": 400, "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}, "body": json.dumps({"error": "no chunks"})}
            if chunks[0].get("embedding"):
                embeddings = [c["embedding"] for c in chunks]
            else:
                client = get_openai_client()
                texts = [c.get("chunk_text") or c.get("text", "") for c in chunks]
                resp = client.embeddings.create(model=EMBEDDING_MODEL, input=texts)
                embeddings = [item.embedding for item in sorted(resp.data, key=lambda x: x.index)]
            conn = get_db_connection()
            cur = conn.cursor()
            inserted = 0
            errors = 0
            try:
                rows = []
                for chunk, emb in zip(chunks, embeddings):
                    chunk_text = chunk.get("chunk_text") or chunk.get("text", "")
                    emb_str = "[" + ",".join(map(str, emb)) + "]"
                    s3_key = chunk.get("s3_key") or chunk.get("source_url") or chunk.get("filename", "")
                    rows.append((s3_key, chunk.get("filename", ""), chunk.get("chunk_index", 0),
                                 chunk_text, json.dumps(chunk.get("metadata", {})), emb_str))
                _execute_values(cur, """
                    INSERT INTO documents (s3_key, filename, chunk_index, chunk_text, metadata, embedding)
                    VALUES %s
                    ON CONFLICT (s3_key, chunk_index) DO UPDATE
                      SET chunk_text = EXCLUDED.chunk_text,
                          metadata   = EXCLUDED.metadata,
                          embedding  = EXCLUDED.embedding
                """, rows, template="(%s, %s, %s, %s, %s, %s::vector)", page_size=len(rows))
                inserted = len(rows)
                conn.commit()
            except Exception as e:
                errors = len(chunks)
                print(f"[Insert] batch error: {e}")
                try: conn.rollback()
                except: pass
            finally:
                cur.close()
                conn.close()
            print(f"[Insert] inserted={inserted} errors={errors}")
            return {"statusCode": 200, "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}, "body": json.dumps({"inserted": inserted, "errors": errors})}

        # バリデーション
        if not message:
            return _error_response(400, "message is required")
        
        if language not in SUPPORTED_LANGUAGES:
            return _error_response(
                400,
                f"language must be one of: {', '.join(SUPPORTED_LANGUAGES)}",
            )
        
        print(f"[Chat] message={message[:50]}... language={language}")
        
        # 言語別の閾値を適用（AIMessiah ロジック）
        language_settings = {
            "ko": {
                "min_top_k": KOREAN_MIN_TOP_K,
                "threshold": KOREAN_SIMILARITY_THRESHOLD,
                "fallback": KOREAN_FALLBACK_THRESHOLD,
            },
            "ja": {
                "min_top_k": JAPANESE_MIN_TOP_K,
                "threshold": JAPANESE_SIMILARITY_THRESHOLD,
                "fallback": JAPANESE_FALLBACK_THRESHOLD,
            },
        }
        
        settings = language_settings.get(language)
        effective_top_k = top_k
        effective_threshold = similarity_threshold
        fallback_threshold = None
        
        if settings:
            effective_top_k = max(top_k, settings["min_top_k"])
            effective_threshold = min(similarity_threshold, settings["threshold"])
            fallback_threshold = settings["fallback"]
        
        print(f"[Chat] Using threshold={effective_threshold}, top_k={effective_top_k}")
        
        # 埋め込み取得
        query_embedding = get_embedding(message)
        
        # ベクトル検索（クエリ言語を優先）
        context_chunks = vector_search(query_embedding, language, effective_top_k, effective_threshold)
        
        # フォールバック: 結果が少ない場合、より低い閾値で再検索
        if fallback_threshold and len(context_chunks) < 3 and effective_threshold > fallback_threshold:
            print(f"[Chat] Falling back to lower threshold: {fallback_threshold}")
            context_chunks = vector_search(query_embedding, language, effective_top_k, fallback_threshold)
        
        print(f"[Chat] Found {len(context_chunks)} relevant chunks")
        
        # チャット応答生成 (conversation history support)
        history = body.get("history", [])
        result = generate_chat_response(message, context_chunks, language, history)
        
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "POST, OPTIONS",
            },
            "body": json.dumps(result, ensure_ascii=False),
        }
        
    except Exception as e:
        print(f"[Chat] Error: {str(e)}")
        return _error_response(500, str(e))


def _error_response(status_code: int, message: str) -> Dict[str, Any]:
    """エラーレスポンスを生成"""
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps({"error": message}),
    }
