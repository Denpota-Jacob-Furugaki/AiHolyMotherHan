"""
Mini-Han API server — Freemium model

Users get 10 free prompts (tracked client-side), then see a subscription prompt.
Paying users receive a subscription token that unlocks unlimited access.

The server holds all API keys — users never need to configure anything.

Environment variables required:
    OPENAI_API_KEY    — for embeddings (text-embedding-3-large)
    GROQ_API_KEY      — for chat generation (free via Groq)
    DB_HOST           — Aurora PostgreSQL host
    DB_PASSWORD       — Aurora PostgreSQL password

    # Optional
    CHAT_MODEL        — override default Groq model
    VALID_TOKENS      — comma-separated list of valid subscription tokens
                        e.g. "tok_abc123,tok_def456"
    OLLAMA_BASE_URL   — if using Ollama instead of Groq (default: http://localhost:11434)

Usage:
    pip install -r requirements-local.txt
    uvicorn local_server:app --reload --port 8000
"""

import json
import os
import urllib.request
import urllib.error
from typing import Any, Dict, List, Optional

import pg8000
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ============================================================
# Config — loaded from env, never from user requests
# ============================================================

EMBEDDING_MODEL = "text-embedding-3-large"
OPENAI_API_BASE = "https://api.openai.com/v1"
GROQ_API_BASE = "https://api.groq.com/openai/v1"
OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")

CHAT_PROVIDER = os.environ.get("CHAT_PROVIDER", "groq")  # "groq" | "openai" | "ollama"
CHAT_MODEL = os.environ.get("CHAT_MODEL", "llama-3.3-70b-versatile")

DEFAULT_TOP_K = int(os.environ.get("DEFAULT_TOP_K", "3"))
DEFAULT_SIMILARITY_THRESHOLD = float(os.environ.get("DEFAULT_SIMILARITY_THRESHOLD", "0.45"))
FALLBACK_THRESHOLD = float(os.environ.get("FALLBACK_THRESHOLD", "0.35"))

FREE_PROMPT_LIMIT = int(os.environ.get("FREE_PROMPT_LIMIT", "10"))
SUPPORTED_LANGUAGES = {"en", "ja", "ko"}

# Subscription tokens — comma-separated in env var
# e.g. VALID_TOKENS="tok_abc123,tok_def456"
_raw_tokens = os.environ.get("VALID_TOKENS", "")
VALID_TOKENS: set = {t.strip() for t in _raw_tokens.split(",") if t.strip()}

# ============================================================
# FastAPI app
# ============================================================

app = FastAPI(title="Mini-Han API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class ChatRequest(BaseModel):
    message: str
    language: str = "en"
    subscription_token: Optional[str] = None  # provided by paying users


class TokenCheckRequest(BaseModel):
    token: str


# ============================================================
# HTTP helper
# ============================================================

def _api_request(base_url: str, api_key: str, endpoint: str, payload: dict) -> dict:
    url = f"{base_url}/{endpoint}"
    data = json.dumps(payload).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (compatible; mini-han/1.0)",
    }
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"API error {e.code}: {e.read().decode()}")


# ============================================================
# Embeddings — server's OpenAI key
# ============================================================

def get_embedding(text: str) -> List[float]:
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("Server OPENAI_API_KEY is not configured")
    response = _api_request(OPENAI_API_BASE, api_key, "embeddings", {
        "model": EMBEDDING_MODEL,
        "input": text,
    })
    return response["data"][0]["embedding"]


# ============================================================
# Chat generation — server's Groq / OpenAI / Ollama key
# ============================================================

SYSTEM_PROMPT_TEMPLATE = """You are Mini-Han, a spiritual guide who teaches ONLY from the words and teachings of Reverend Sun Myung Moon and his wife Hak Ja Han Moon (True Parents). Respond in {language_name}.

CRITICAL RULES — never break these:
1. ONLY use the reference passages provided below. Do NOT use your general knowledge.
2. If no relevant passage is provided, say: "I couldn't find a direct teaching on this in the provided texts. Please try rephrasing or ask about a related topic."
3. NEVER give a neutral, relativistic, or secular answer. Always ground your reply in True Parents' teachings.
4. NEVER say things like "some people believe" or "it's a personal decision" — give the clear teaching from the texts.
5. Always cite the source (speech title and date) when quoting.
6. Be warm and approachable, but never compromise the content of the teaching.
7. For greetings and small talk, respond naturally. For all theological or ethical questions, rules 1–6 apply strictly.
"""

LANGUAGE_NAMES = {"en": "English", "ja": "日本語 (Japanese)", "ko": "한국어 (Korean)"}


def generate_reply(system: str, user_prompt: str) -> str:
    provider = CHAT_PROVIDER.lower()

    if provider == "groq":
        api_key = os.environ.get("GROQ_API_KEY")
        if not api_key:
            raise RuntimeError("Server GROQ_API_KEY is not configured")
        response = _api_request(GROQ_API_BASE, api_key, "chat/completions", {
            "model": CHAT_MODEL,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.7,
            "max_tokens": 600,
        })
        return response["choices"][0]["message"]["content"]

    elif provider == "openai":
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError("Server OPENAI_API_KEY is not configured")
        response = _api_request(OPENAI_API_BASE, api_key, "chat/completions", {
            "model": CHAT_MODEL or "gpt-4o-mini",
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.7,
            "max_tokens": 600,
        })
        return response["choices"][0]["message"]["content"]

    elif provider == "ollama":
        url = f"{OLLAMA_BASE_URL}/api/chat"
        payload = {
            "model": CHAT_MODEL or "gemma3:4b",
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user_prompt},
            ],
            "stream": False,
            "options": {"temperature": 0.7, "num_predict": 600},
        }
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url, data=data, headers={"Content-Type": "application/json"}, method="POST"
        )
        with urllib.request.urlopen(req, timeout=120) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            return result["message"]["content"]

    else:
        raise RuntimeError(f"Unknown CHAT_PROVIDER '{provider}'")


# ============================================================
# Aurora pgvector search
# ============================================================

def _get_db_connection():
    conn = pg8000.connect(
        host=os.environ.get("DB_HOST", "ragstack-ragdatabase4961bfdb-ot23vvg32pbj.cx84scemy9xh.ap-northeast-1.rds.amazonaws.com"),
        port=int(os.environ.get("DB_PORT", "5432")),
        database=os.environ.get("DB_NAME", "ragdb"),
        user=os.environ.get("DB_USER", "ragadmin"),
        password=os.environ.get("DB_PASSWORD"),
        timeout=30,
    )
    cursor = conn.cursor()
    cursor.execute("SET statement_timeout = '25s'")
    cursor.close()
    return conn


def vector_search(
    query_embedding: List[float],
    language: str,
    top_k: int,
    threshold: float,
) -> List[Dict[str, Any]]:
    conn = _get_db_connection()
    try:
        cursor = conn.cursor()
        emb_str = "[" + ",".join(map(str, query_embedding)) + "]"
        columns = ["id", "s3_key", "filename", "chunk_index", "chunk_text", "metadata", "similarity"]
        cursor.execute(
            """
            SELECT id, s3_key, filename, chunk_index, chunk_text, metadata,
                   1 - (embedding <=> %s::vector) AS similarity
            FROM documents
            WHERE metadata->>'language' = %s
              AND 1 - (embedding <=> %s::vector) >= %s
            ORDER BY embedding <=> %s::vector
            LIMIT %s
            """,
            (emb_str, language, emb_str, threshold, emb_str, top_k),
        )
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        cursor.close()
        rows.sort(key=lambda x: x["similarity"], reverse=True)
        return [
            {
                "id": r["id"],
                "s3_key": r["s3_key"],
                "filename": r["filename"],
                "chunk_index": r["chunk_index"],
                "chunk_text": r["chunk_text"],
                "metadata": r["metadata"] or {},
                "similarity": float(r["similarity"]),
            }
            for r in rows[:top_k]
        ]
    finally:
        conn.close()


# ============================================================
# Prompt helpers
# ============================================================

THEOLOGICAL_TERMS = {
    "True Parents": "True Parents 真の父母様 참부모님",
    "Divine Principle": "Divine Principle 原理講論 원리강론",
    "Cheon Il Guk": "Cheon Il Guk 天一国 천일국",
    "True Love": "True Love 真の愛 참사랑",
    "真の父母様": "真の父母様 True Parents 참부모님 真の父母",
    "原理講論": "原理講論 Divine Principle 원리강론 統一原理",
    "창조원리": "창조원리 創造原理 Principle of Creation",
    "참부모님": "참부모님 真の父母様 True Parents",
}


def _expand_query(query: str) -> str:
    for term, expansion in THEOLOGICAL_TERMS.items():
        if term in query:
            return f"{query} {expansion}"
    return query


def _build_user_prompt(message: str, chunks: List[Dict[str, Any]]) -> str:
    parts = []
    for i, chunk in enumerate(chunks, 1):
        meta = chunk.get("metadata", {})
        title = (
            meta.get("speech_title_en")
            or meta.get("speech_title")
            or meta.get("book_name")
            or chunk.get("filename", "unknown").replace(".pdf", "")
        )
        date = meta.get("speech_date", "")
        text = chunk["chunk_text"][:1000]
        source_line = f"Source: '{title}'"
        if date:
            source_line += f" ({date})"
        parts.append(f"[Passage {i}]\n{source_line}\n{text}")
    context = ("\n\n---\n\n").join(parts) if parts else "NO PASSAGES FOUND"
    return (
        f"User question: {message}\n\n"
        f"=== REFERENCE PASSAGES FROM TRUE PARENTS' TEACHINGS ===\n\n{context}\n\n"
        "=== END OF PASSAGES ===\n\n"
        "Answer the question using ONLY the passages above. Quote directly where possible and cite the source. "
        "If the passages are not relevant, say so clearly."
    )


# ============================================================
# Endpoints
# ============================================================

@app.post("/chat")
async def chat(req: ChatRequest):
    if req.language not in SUPPORTED_LANGUAGES:
        raise HTTPException(400, f"language must be one of: {SUPPORTED_LANGUAGES}")

    # Validate subscription token if provided
    is_subscriber = bool(req.subscription_token and req.subscription_token in VALID_TOKENS)
    print(f"[Chat] lang={req.language} subscriber={is_subscriber} msg={req.message[:50]}")

    # 1. Embed query (server's OpenAI key)
    expanded = _expand_query(req.message)
    try:
        embedding = get_embedding(expanded)
    except RuntimeError as e:
        raise HTTPException(502, f"Embedding error: {e}")

    # 2. RAG retrieval from Aurora
    chunks: List[Dict[str, Any]] = []
    try:
        # First: search in requested language
        chunks = vector_search(embedding, req.language, DEFAULT_TOP_K, DEFAULT_SIMILARITY_THRESHOLD)
        if len(chunks) < 2:
            chunks = vector_search(embedding, req.language, DEFAULT_TOP_K, FALLBACK_THRESHOLD)
        # Fallback: cross-language search (documents may be in ja/ko regardless of query language)
        if len(chunks) < 2:
            for lang in ["en", "ja", "ko"]:
                if lang == req.language:
                    continue
                extra = vector_search(embedding, lang, DEFAULT_TOP_K, FALLBACK_THRESHOLD)
                chunks.extend(extra)
                if len(chunks) >= 2:
                    break
        # Deduplicate and keep top results by similarity
        seen_ids = set()
        unique_chunks = []
        for c in sorted(chunks, key=lambda x: x["similarity"], reverse=True):
            if c["id"] not in seen_ids:
                seen_ids.add(c["id"])
                unique_chunks.append(c)
        chunks = unique_chunks[:DEFAULT_TOP_K]
        print(f"[Chat] Retrieved {len(chunks)} chunks (langs searched: all)")
    except Exception as e:
        print(f"[Chat] DB unavailable, continuing without RAG: {e}")

    # 3. Build prompts
    system = SYSTEM_PROMPT_TEMPLATE.format(
        language_name=LANGUAGE_NAMES.get(req.language, "English")
    )
    user_prompt = _build_user_prompt(req.message, chunks)

    # 4. Generate reply (server's chat provider)
    try:
        reply = generate_reply(system, user_prompt)
    except RuntimeError as e:
        raise HTTPException(502, f"Chat error: {e}")

    # 5. Format sources
    sources = [
        {
            "index": i + 1,
            "filename": c.get("filename", "unknown"),
            "s3_key": c.get("s3_key", ""),
            "language": c.get("metadata", {}).get("language", "unknown"),
            "similarity": c.get("similarity", 0),
            "excerpt": c.get("chunk_text", "")[:200] + "...",
        }
        for i, c in enumerate(chunks)
    ]

    return {"reply": reply, "sources": sources, "language": req.language}


@app.post("/validate-token")
async def validate_token(req: TokenCheckRequest):
    """Paying users submit their subscription token here to verify it's valid."""
    if not req.token:
        raise HTTPException(400, "token is required")
    valid = req.token in VALID_TOKENS
    return {"valid": valid}


@app.get("/health")
def health():
    return {
        "status": "ok",
        "free_prompt_limit": FREE_PROMPT_LIMIT,
        "chat_provider": CHAT_PROVIDER,
        "chat_model": CHAT_MODEL,
    }
