"""
Multilingual Chat Lambda - 3ヶ国語対応チャットAPI

クエリ言語（en/ja/ko）で応答し、出典を引用する。
韓国語・日本語・英語のRAGデータを検索し、クエリ言語に合わせて回答を生成する。
"""
import json
import os
import urllib.request
import urllib.error
from typing import List, Dict, Any, Optional

import boto3
import pg8000
import pg8000.native

# DynamoDB token table (set by env var)
_TOKEN_TABLE = os.environ.get("TOKEN_TABLE", "mini-han-tokens")
_dynamo = None

def _get_dynamo_table():
    global _dynamo
    if _dynamo is None:
        dynamo = boto3.resource("dynamodb", region_name=os.environ.get("AWS_REGION", "ap-northeast-1"))
        _dynamo = dynamo.Table(_TOKEN_TABLE)
    return _dynamo


DAILY_QUERY_LIMIT = int(os.environ.get("DAILY_QUERY_LIMIT", "10"))
ADMIN_EMAILS = {"denpotafurugaki@gmail.com"}


def verify_google_token(id_token: str) -> Optional[dict]:
    """Verify Google ID token; return {sub, email} or None."""
    try:
        url = f"https://oauth2.googleapis.com/tokeninfo?id_token={id_token}"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        sub = data.get("sub")
        if not sub:
            return None
        return {"sub": sub, "email": data.get("email", "")}
    except Exception as e:
        print(f"[Google] token verify failed: {e}")
        return None


def check_and_increment_daily_limit(google_sub: str, email: str = "") -> dict:
    """Check daily query limit. Returns {allowed, count, limit, unlimited}."""
    if email in ADMIN_EMAILS:
        return {"allowed": True, "count": 0, "limit": DAILY_QUERY_LIMIT, "unlimited": True}
    import datetime
    # JST = UTC+9
    now_jst = datetime.datetime.utcnow() + datetime.timedelta(hours=9)
    today = now_jst.strftime("%Y-%m-%d")
    key = f"google:{google_sub}"
    table = _get_dynamo_table()
    try:
        resp = table.get_item(Key={"token": key})
        item = resp.get("Item")
        # Check unlimited flag (set when coupon is redeemed)
        if item and item.get("unlimited"):
            return {"allowed": True, "count": 0, "limit": DAILY_QUERY_LIMIT, "unlimited": True}
        if not item or item.get("query_date") != today:
            # New day or first visit — start fresh
            table.put_item(Item={
                "token": key,
                "google_sub": google_sub,
                "query_date": today,
                "query_count": 1,
                "active": True,
            })
            return {"allowed": True, "count": 1, "limit": DAILY_QUERY_LIMIT, "unlimited": False}
        count = int(item.get("query_count", 0))
        if count >= DAILY_QUERY_LIMIT:
            return {"allowed": False, "count": count, "limit": DAILY_QUERY_LIMIT, "unlimited": False}
        new_count = count + 1
        table.update_item(
            Key={"token": key},
            UpdateExpression="SET query_count = :c",
            ExpressionAttributeValues={":c": new_count},
        )
        return {"allowed": True, "count": new_count, "limit": DAILY_QUERY_LIMIT, "unlimited": False}
    except Exception as e:
        print(f"[Google] daily limit check failed: {e}")
        return {"allowed": True, "count": 0, "limit": DAILY_QUERY_LIMIT, "unlimited": False}


def redeem_coupon(google_sub: str, coupon_code: str) -> dict:
    """Redeem a coupon code for unlimited access. Returns {success, message}."""
    table = _get_dynamo_table()
    coupon_key = f"coupon:{coupon_code.strip().upper()}"
    try:
        resp = table.get_item(Key={"token": coupon_key})
        item = resp.get("Item")
        if not item or not item.get("active", False):
            return {"success": False, "message": "invalid_coupon"}
        # Deactivate coupon (single-use)
        table.update_item(
            Key={"token": coupon_key},
            UpdateExpression="SET active = :f, redeemed_by = :s",
            ExpressionAttributeValues={":f": False, ":s": google_sub},
        )
        # Mark user as unlimited
        user_key = f"google:{google_sub}"
        table.update_item(
            Key={"token": user_key},
            UpdateExpression="SET unlimited = :t, active = :t",
            ExpressionAttributeValues={":t": True},
        )
        return {"success": True, "message": "ok"}
    except Exception as e:
        print(f"[Coupon] redemption failed: {e}")
        return {"success": False, "message": "error"}


def get_token_info(token: str) -> dict:
    """Returns {valid, unlimited, credits, email} for a token."""
    if not token:
        return {"valid": False, "unlimited": False, "credits": 0}
    # 1. Check DynamoDB
    try:
        resp = _get_dynamo_table().get_item(Key={"token": token})
        item = resp.get("Item")
        if item and item.get("active", False):
            credits = item.get("credits", -1)
            unlimited = (credits == -1)
            if not unlimited and credits <= 0:
                return {"valid": False, "unlimited": False, "credits": 0, "email": item.get("email", "")}
            return {"valid": True, "unlimited": unlimited, "credits": int(credits), "email": item.get("email", "")}
    except Exception as e:
        print(f"[Token] DynamoDB lookup failed, falling back to env: {e}")
    # 2. Fallback: comma-separated VALID_TOKENS env var (treated as unlimited)
    valid = {t.strip() for t in os.environ.get("VALID_TOKENS", "").split(",") if t.strip()}
    if token in valid:
        return {"valid": True, "unlimited": True, "credits": -1}
    return {"valid": False, "unlimited": False, "credits": 0}


def is_valid_token(token: str) -> bool:
    return get_token_info(token)["valid"]


def deduct_credit(token: str) -> int:
    """Decrement credits by 1. Returns new balance, or -1 for unlimited."""
    try:
        resp = _get_dynamo_table().get_item(Key={"token": token})
        item = resp.get("Item")
        if not item:
            return 0
        credits = item.get("credits", -1)
        if credits == -1:
            return -1  # unlimited
        if credits <= 0:
            return 0
        new_credits = credits - 1
        _get_dynamo_table().update_item(
            Key={"token": token},
            UpdateExpression="SET credits = :c, active = :a",
            ExpressionAttributeValues={
                ":c": new_credits,
                ":a": new_credits > 0,
            },
        )
        return new_credits
    except Exception as e:
        print(f"[Token] deduct_credit failed: {e}")
        return -1

# ============================================================
# 定数・設定
# ============================================================

SUPPORTED_LANGUAGES = {"en", "ja", "ko"}
DEFAULT_TOP_K = int(os.environ.get("DEFAULT_TOP_K", "3"))  # Reduced for faster responses
DEFAULT_SIMILARITY_THRESHOLD = float(os.environ.get("DEFAULT_SIMILARITY_THRESHOLD", "0.7"))

# 韓国語・日本語用の低い閾値（AIMessiah から適用）
KOREAN_SIMILARITY_THRESHOLD = float(os.environ.get("KOREAN_SIMILARITY_THRESHOLD", "0.45"))
KOREAN_FALLBACK_THRESHOLD = float(os.environ.get("KOREAN_FALLBACK_THRESHOLD", "0.35"))
KOREAN_MIN_TOP_K = int(os.environ.get("KOREAN_MIN_TOP_K", "3"))  # Reduced for faster responses
JAPANESE_SIMILARITY_THRESHOLD = float(os.environ.get("JAPANESE_SIMILARITY_THRESHOLD", "0.45"))
JAPANESE_FALLBACK_THRESHOLD = float(os.environ.get("JAPANESE_FALLBACK_THRESHOLD", "0.35"))
JAPANESE_MIN_TOP_K = int(os.environ.get("JAPANESE_MIN_TOP_K", "3"))  # Reduced for faster responses

EMBEDDING_MODEL = "text-embedding-3-large"
CHAT_MODEL = os.environ.get("CHAT_MODEL", "gpt-4o")

# ============================================================
# クライアント初期化
# ============================================================

secrets_client = boto3.client("secretsmanager")
_openai_api_key: Optional[str] = None

OPENAI_API_BASE = "https://api.openai.com/v1"


def get_secret(secret_arn: str) -> str:
    """Secrets Manager からシークレットを取得"""
    response = secrets_client.get_secret_value(SecretId=secret_arn)
    return response["SecretString"]


def get_openai_api_key() -> str:
    """OpenAI API キーを取得（遅延初期化）"""
    global _openai_api_key
    if _openai_api_key is None:
        secret_arn = os.environ.get("OPENAI_SECRET_ARN")
        if secret_arn:
            _openai_api_key = get_secret(secret_arn)
        else:
            _openai_api_key = os.environ.get("OPENAI_API_KEY")
        if not _openai_api_key:
            raise ValueError("OpenAI API key not found")
    return _openai_api_key


def openai_request(endpoint: str, payload: dict) -> dict:
    """OpenAI API に HTTP リクエストを送信"""
    api_key = get_openai_api_key()
    url = f"{OPENAI_API_BASE}/{endpoint}"
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    
    try:
        with urllib.request.urlopen(req, timeout=60) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8")
        raise RuntimeError(f"OpenAI API error: {e.code} - {error_body}")


def get_db_connection():
    """Aurora PostgreSQL への接続を取得"""
    secret_arn = os.environ.get("DB_SECRET_ARN")
    if secret_arn:
        secret_str = get_secret(secret_arn)
        secret = json.loads(secret_str)
        password = secret.get("password", secret.get("ragadmin"))
    else:
        password = os.environ.get("DB_PASSWORD")

    conn = pg8000.connect(
        host=os.environ.get("DB_HOST"),
        port=int(os.environ.get("DB_PORT", "5432")),
        database=os.environ.get("DB_NAME", "ragdb"),
        user=os.environ.get("DB_USER", "ragadmin"),
        password=password,
        timeout=30,  # Connection timeout
    )
    # Set statement timeout to avoid long-running queries
    cursor = conn.cursor()
    cursor.execute("SET statement_timeout = '25s'")
    cursor.close()
    return conn


# ============================================================
# 用語展開（専門用語の多言語マッピング）
# ============================================================

# 重要な神学用語のマッピング（検索精度向上用）
# ────────────────────────────────────────────────────────────────
# カバー範囲:
#   A) 口語的な悩み・苦しみ表現        B) 家族・夫婦・結婚の悩み
#   C) 信仰・神様への疑問              D) 生きる意味・虚しさ
#   E) 許せない・怒り・憎しみ          F) 先祖・霊的問題
#   G) よく使う口語の質問パターン      H) 神学専門用語（多言語）
#   I) 英語パターン                    J) 韓国語パターン
# ────────────────────────────────────────────────────────────────
THEOLOGICAL_TERMS = {

    # ── A) 個人の悩み・苦しみ・生きづらさ ────────────────────────
    "生きるのが辛": "神様の心情 心情の苦しみ 蕩減復帰 原罪 창조목적 탕감복귀 하나님의 심정 하나님의 사랑 suffering indemnity God's love",
    "生きているのが辛": "神様の心情 心情の苦しみ 蕩減復帰 原罪 탕감복귀 하나님의 심정 창조목적 suffering indemnity",
    "生きてるのが辛": "神様の心情 心情の苦しみ 蕩減復帰 原罪 탕감복귀 하나님의 심정 suffering indemnity",
    "死にたい": "命の価値 創造目的 神様の愛 하나님의 사랑 창조목적 생명의 가치 인간의 가치 God's love value of life",
    "消えたい": "命の価値 創造目的 神様の愛 하나님의 사랑 창조목적 God's love",
    "いなくなりたい": "命の価値 創造目的 心情 하나님의 사랑 창조목적 God's love heart",
    "孤独": "神様の心情の孤独 神様の悲しみ 心情 창조목적 하나님의 외로움 하나님의 심정 loneliness God's heart shimjung",
    "ひとりぼっち": "神様の孤独 神様の心情 心情 하나님의 심정 하나님의 외로움 loneliness God's heart",
    "さみしい": "神様の心情 心情 하나님의 심정 심정 loneliness God's heart",
    "寂しい": "神様の心情 心情 하나님의 심정 하나님의 외로움 loneliness God's heart shimjung",
    "自分が嫌い": "創造目的 神様に愛された子女 子女の価値 창조목적 자녀의 가치 하나님의 사랑 God's love value",
    "自己嫌悪": "創造目的 神様の愛 子女の価値 창조목적 자녀의 가치 하나님의 사랑",
    "自分はダメ": "創造目的 子女の価値 神様の愛 창조목적 자녀의 가치 하나님의 사랑 God's love",
    "自分を好きになれ": "創造目的 子女の価値 神様の愛 하나님의 사랑 창조목적 God's love",
    "自信がない": "創造目的 子女の価値 神様の愛 하나님의 사랑 창조목적",
    "自分には価値がない": "創造目的 子女の価値 神様の愛 하나님의 사랑 창조목적 God's love",
    "悩んでいる": "蕩減復帰 心情 神様の心情 탕감복귀 하나님의 심정 indemnity restoration",
    "悩みがある": "蕩減復帰 心情 神様の心情 탕감복귀 하나님의 심정 indemnity",
    "つらい": "蕩減復帰 心情の苦しみ 神様の悲しみ 탕감복귀 하나님의 심정 심정 indemnity suffering",
    "苦しい": "蕩減復帰 心情の苦しみ 原罪 탕감복귀 원죄 하나님의 심정 indemnity suffering",
    "悲しい": "神様の悲しみ 心情 蕩減復帰 하나님의 슬픔 하나님의 심정 탕감복귀 God's sorrow heart",
    "不安": "蕩減復帰 神様の摂理 信仰 탕감복귀 하나님의 섭리 믿음 faith providence",
    "怖い": "蕩減復帰 信仰 神様の愛 탕감복귀 믿음 하나님의 사랑 faith God's love",
    "うつ": "蕩減復帰 神様の心情 心情 탕감복귀 하나님의 심정 심정 indemnity God's heart",
    "落ち込んでいる": "蕩減復帰 神様の悲しみ 心情 탕감복귀 하나님의 슬픔 심정",
    "やる気が出ない": "蕩減復帰 創造目的 為に生きる 탕감복귀 창조목적 위하여 사는 삶",
    "疲れた": "蕩減復帰 心情の苦しみ 神様の慰め 탕감복귀 하나님의 심정 위로 comfort",

    # ── B) 家族・夫婦・結婚・人間関係 ────────────────────────────
    "結婚できない": "祝福 祝福結婚 真の家庭 理想家庭 축복 참가정 이상가정 blessing marriage",
    "結婚したい": "祝福 祝福結婚 真の家庭 이상가정 축복 참가정 blessing ideal family",
    "結婚相手": "祝福 真の家庭 理想家庭 축복 참가정 이상가정 blessing family",
    "結婚について": "祝福 祝福結婚 真の家庭 창조원리 축복 참가정 blessing",
    "祝福結婚": "祝福 祝福結婚 真の家庭 혈통전환 축복결혼 참가정 血統転換 blessing ceremony",
    "夫婦仲": "真の家庭 授受作用 真の愛 참가정 수수작용 부부관계 참사랑 true love family",
    "夫婦関係": "真の家庭 授受作用 真の愛 참가정 수수작용 부부관계 참사랑",
    "夫と仲が悪い": "真の家庭 授受作用 心情 참가정 부부관계 수수작용 true love",
    "妻と仲が悪い": "真の家庭 授受作用 心情 참가정 부부관계 수수작용 true love",
    "離婚": "真の家庭 祝福 真の愛 참가정 축복 참사랑 ideal family true love",
    "親との関係": "孝情 父母 真の父母 효정 부모 참부모님 filial heart parents",
    "親と仲が悪い": "孝情 父母への愛 心情 효정 부모 하나님의 심정 filial heart",
    "親が嫌い": "孝情 父母 心情 효정 부모님 하나님의 심정 filial heart parents",
    "子育て": "真の家庭 子女教育 孝情 참가정 자녀교육 효정 true family children",
    "子供の教育": "真の家庭 子女教育 参加原理 참가정 자녀교육 true family",
    "子供がいうことを聞かない": "真の家庭 孝情 子女 참가정 효정 자녀 true family",
    "友達ができない": "心情 真の愛 為に生きる 심정 참사랑 위하여 사는 삶 true love",
    "人間関係がうまくいかない": "心情 真の愛 授受作用 為に生きる 심정 참사랑 수수작용",
    "人が怖い": "心情 真の愛 授受作用 심정 참사랑 수수작용 God's love",
    "いじめ": "蕩減復帰 原罪 心情 탕감복귀 원죄 심정 indemnity Fall",

    # ── C) 神様・信仰への疑問 ─────────────────────────────────────
    "神様は自分を見えなく": "神様の悲しみ 堕落論 神様の心情 神様の疎外 하나님의 슬픔 하나님의 심정 타락 원죄 God sorrow Fall separation hidden invisible",
    "神様はいるの": "神様の実存 神様の心情 창조원리 하나님의 존재 하나님의 심정 God exists Divine Principle",
    "神様はいる": "神様の実存 創造原理 神様の心情 하나님의 존재 하나님의 심정 창조원리 God exists",
    "神様って本当": "神様の実存 創造原理 하나님의 존재 창조원리 하나님의 심정 God exists Divine Principle",
    "神様を信じられない": "神様の実存 神様の心情 信仰 창조원리 하나님의 심정 하나님의 존재 faith",
    "神様は見てる": "神様の摂理 神様の愛 하나님의 섭리 하나님의 사랑 섭리 providence God's love",
    "神様は私のこと": "神様の愛 神様の心情 창조목적 하나님의 사랑 하나님의 심정 God's love",
    "神様に祈っても": "神様の心情 祈り 信仰 하나님의 심정 기도 믿음 prayer faith",
    "神様はなぜ": "神様の心情 神様の悲しみ 堕落論 하나님의 심정 하나님의 슬픔 하나님의 섭리",
    "なんで神様": "神様の心情 神様の悲しみ 堕落論 摂理 하나님의 심정 하나님의 슬픔 하나님의 섭리",
    "なぜ神様": "神様の心情 神様の悲しみ 堕落論 섭리 하나님의 심정 하나님의 슬픔",
    "なぜ神は": "神様の心情 神様の悲しみ 堕落論 하나님의 심정 하나님의 섭리",
    "天国って": "霊界 天国 地上天国 영계 천국 지상천국 spirit world heaven",
    "天国はどこ": "霊界 天国 영계 천국 spirit world heaven",
    "死んだらどこ": "霊界 天国 先祖 영계 천국 조상 spirit world",
    "死後": "霊界 天国 先祖解析 영계 천국 조상 spirit world ancestors",
    "地獄って": "霊界 地獄 蕩減 영계 지옥 탕감 spirit world hell indemnity",
    "悪魔って": "堕落論 サタン 原罪 타락론 사탄 원죄 Fall Satan",
    "サタンって": "堕落論 サタン 原罪 霊界 타락론 사탄 원죄 영계 Satan Fall",
    "悪霊": "霊界 先祖 堕落 영계 조상 악령 spirit world Fall",
    "信仰が薄れ": "神様の摂理 信仰 心情 하나님의 섭리 믿음 심정 faith providence",
    "信仰をやめたい": "神様の摂理 信仰 心情 하나님의 섭리 믿음 심정 섭리 faith",
    "信じられない": "神様の実存 創造原理 信仰 하나님의 존재 창조원리 믿음 faith",
    "摂理って": "神様の摂理 復帰摂理 섭리 복귀섭리 하나님의 섭리 providence restoration",
    "原罪って": "原罪 堕落論 血統転換 원죄 타락론 혈통전환 original sin Fall",
    "原罪は": "原罪 堕落論 血統転換 원죄 타락론 혈통전환 original sin",

    # ── D) 生きる意味・虚しさ・目的 ──────────────────────────────
    "何のために生きる": "創造目的 心情 神様の愛 창조목적 하나님의 사랑 하나님의 심정 purpose of life God's love",
    "なんのために生きる": "創造目的 心情 神様の愛 창조목적 하나님의 사랑 purpose of life",
    "生きる意味": "創造目的 心情 神様の愛 창조목적 하나님의 사랑 삶의 의미 purpose meaning",
    "生きがいがない": "創造目的 為に生きる 心情 창조목적 위하여 사는 삶 purpose of life",
    "意味がない": "創造目的 神様の愛 心情 창조목적 하나님의 사랑 삶의 의미 meaning purpose",
    "虚しい": "創造目的 真の愛 心情 창조목적 참사랑 심정 emptiness purpose true love",
    "空虚": "創造目的 真の愛 心情 창조목적 참사랑 심정 emptiness true love",
    "どうして生まれた": "創造目的 創造原理 神様の愛 창조목적 창조원리 하나님의 사랑 purpose of creation",
    "なぜ生まれた": "創造目的 創造原理 神様の愛 창조목적 창조원리 하나님의 사랑",
    "なぜ生きる": "創造目的 心情 神様の愛 창조목적 하나님의 사랑 purpose of life",
    "幸せになれない": "真の愛 真の家庭 創造目的 참사랑 참가정 창조목적 true love ideal family",
    "幸せとは": "真の愛 真の家庭 心情 참사랑 참가정 심정 true love happiness",

    # ── E) 許せない・怒り・憎しみ ────────────────────────────────
    "許せない": "蕩減復帰 赦し 心情 탕감복귀 용서 심정 indemnity forgiveness",
    "許せない人がいる": "蕩減復帰 赦し 心情 탕감복귀 용서 심정 forgiveness",
    "憎い": "蕩減復帰 原罪 赦し 탕감복귀 원죄 용서 indemnity hatred forgiveness",
    "憎しみ": "蕩減復帰 原罪 赦し 탕감복귀 원죄 용서 indemnity hatred",
    "怒りが抑えられない": "蕩減復帰 原罪 心情 탕감복귀 원죄 심정 indemnity anger",
    "怒り": "蕩減復帰 原罪 心情 탕감복귀 원죄 심정 indemnity",
    "復讐したい": "蕩減復帰 赦し 原罪 탕감복귀 용서 원죄 indemnity forgiveness",
    "なぜ悪": "堕落論 原罪 蕩減復帰 타락론 원죄 탕감복귀 Fall evil",
    "なんで苦しみ": "蕩減復帰 原罪 堕落論 神様の心情 탕감복귀 원죄 타락론 하나님의 심정",
    "なぜ苦しむ": "蕩減復帰 原罪 堕落論 탕감복귀 원죄 타락론 indemnity Fall",
    "なぜ悲しいことが": "蕩減復帰 神様の悲しみ 원죄 탕감복귀 하나님의 슬픔 indemnity",

    # ── F) 先祖・霊的問題・因縁 ──────────────────────────────────
    "先祖の因縁": "先祖解析 霊界 原罪 血統 조상해원 영계 원죄 혈통 ancestors spirit world",
    "先祖のせい": "先祖解析 霊界 蕩減復帰 조상해원 영계 탕감복귀 ancestors indemnity",
    "先祖に守られ": "霊界 先祖 섭리 영계 조상 spirit world ancestors",
    "因縁": "先祖解析 霊界 蕩減復帰 原罪 조상해원 영계 탕감복귀 원죄 ancestors",
    "霊的": "霊界 先祖 摂理 영계 조상 섭리 spirit world providence",
    "スピリチュアル": "霊界 摂理 神様の愛 영계 섭리 하나님의 사랑 spirit world",
    "霊界って": "霊界 天国 先祖 영계 천국 조상 spirit world heaven",
    "霊が": "霊界 先祖 堕落 영계 조상 타락 spirit world",
    "前世": "霊界 先祖 蕩減復帰 영계 조상 탕감복귀 spirit world ancestors",

    # ── G) 口語的な質問パターン ──────────────────────────────────
    "神様の愛": "神様の愛 真の愛 心情 하나님의 사랑 참사랑 심정 God's love True Love",
    "神様の気持ち": "神様の心情 神様の悲しみ 하나님의 심정 하나님의 슬픔 God's heart shimjung",
    "神様の心": "神様の心情 神様の悲しみ 하나님의 심정 하나님의 슬픔 God's heart shimjung",
    "神様はどんな": "神様の実存 神様の心情 創造原理 하나님의 심정 하나님의 존재 창조원리 God",
    "なぜ人間": "創造原理 創造目的 堕落論 인간창조 창조원리 타락론",
    "人間はなぜ": "創造原理 創造目的 堕落論 창조원리 타락론",
    "人間の目的": "創造目的 心情 神様の愛 창조목적 하나님의 사랑 purpose",
    "なんで生きる": "創造目的 心情 神様の愛 창조목적 하나님의 사랑 purpose of life",
    "どうすれば幸せ": "真の愛 真の家庭 心情 창조목적 참사랑 참가정 심정 true love happiness",
    "どうしたら幸せ": "真の愛 真の家庭 心情 참사랑 참가정 창조목적 true love happiness",
    "どう生きればいい": "創造目的 為に生きる 真の愛 창조목적 위하여 사는 삶 참사랑 purpose of life",
    "どうやって信じる": "信仰 神様の心情 摂理 믿음 하나님의 심정 섭리 faith",
    "信仰ってなんで": "神様の摂理 信仰 心情 하나님의 섭리 믿음 심정 faith providence",
    "なんで信仰": "神様の摂理 信仰 心情 하나님의 섭리 믿음 faith",
    "ってどういう意味": "原理講論 天聖経 平和経 원리강론 천성경 평화경 Divine Principle",
    "について教えて": "原理講論 天聖経 平和経 원리강론 천성경 Divine Principle",
    "お父様って": "真の父母様 文鮮明先生 참아버님 문선명 True Father 独生子",
    "お母様って": "独生女 真の父母様 韓鶴子様 독생녀 참어머님 한학자 True Mother",
    "真のお母様": "独生女 真の父母様 韓鶴子様 독생녀 참어머님 한학자 Only Begotten Daughter",
    "蕩減って": "蕩減復帰 탕감복귀 蕩減 indemnity restoration",
    "蕩減はなぜ": "蕩減復帰 原罪 堕落論 탕감복귀 원죄 타락론 indemnity",
    "蕩減の意味": "蕩減復帰 原罪 탕감복귀 원죄 indemnity restoration",
    "祝福って": "祝福 祝福結婚 真の家庭 혈통전換 축복 참가정 blessing ceremony",
    "どうやって祝福": "祝福 祝福結婚 真の家庭 축복 참가정 blessing marriage",

    # ── H) 神学専門用語（日本語・多言語） ───────────────────────
    "復帰摂理時代": "復帰摂理時代 Age of Restoration Providence 복귀섭리시대 復帰摂理",
    "天一国時代": "天一国時代 Cheon Il Guk Era 천일국시대 天一国",
    "蕩減復帰": "蕩減復帰 Restoration through Indemnity 탕감복귀 蕩減",
    "創造原理": "創造原理 Principle of Creation 창조원리",
    "堕落論": "堕落論 The Fall 타락론 堕落",
    "四位基台": "四位基台 Four Position Foundation 사위기대",
    "三対象目的": "三対象目的 Three Object Purpose 삼대상목적",
    "授受作用": "授受作用 Give and Take Action 수수작용",
    "心情": "心情 Heart Shimjung 심정",
    "真の愛": "真の愛 True Love 참사랑",
    "祝福": "祝福 Blessing 축복 祝福式 Blessing Ceremony",
    "血統転換": "血統転換 Change of Blood Lineage 혈통전환",
    "為に生きる": "為に生きる Living for Others 위하여 사는 삶",
    "再臨主": "再臨主 Lord of the Second Advent Messiah 재림주 メシヤ",
    "独生女": "独生女 Only Begotten Daughter 독생녀",
    "独生子": "独生子 Only Begotten Son 독생자",
    "真の父母様": "真の父母様 True Parents 참부모님",
    "真の父母": "真の父母様 True Parents 참부모님",
    "原理講論": "原理講論 Divine Principle 원리강론 統一原理",
    "天聖経": "天聖経 Cheon Seong Gyeong 천성경",
    "平和経": "平和経 Pyeong Hwa Gyeong 평화경",
    "孝情": "孝情 Hyojeong Filial Heart 효정",
    "二性性相": "二性性相 Dual Characteristics 이성성상",
    "霊界": "霊界 Spirit World 영계",
    "基元節": "基元節 Foundation Day 기원절",
    "メシヤ": "メシヤ 再臨主 独生子 독생자 재림주 메시아 Messiah Lord Second Advent",
    "韓鶴子": "独生女 真の父母様 韓鶴子様 독생녀 참어머님 한학자",
    "文鮮明": "真の父母様 文鮮明先生 참아버님 문선명 True Father",
    "真の家庭": "真の家庭 理想家庭 家庭理想 참가정 이상가정 ideal family true family",
    "子女": "子女 子女の価値 孝情 자녀 효정 자녀교육 children",
    "統一教会": "統一教会 真の父母様 原理講論 통일교 참부모님 원리강론 Unification Church",
    "家庭連合": "家庭連合 真の父母様 真の家庭 가정연합 참부모님 참가정 Family Federation",
    "摂理": "神様の摂理 復帰摂理 섭리 하나님의 섭리 복귀섭리 providence restoration",

    # ── I) 英語口語パターン（悩み・疑問・感情 全般） ──────────────
    # Personal struggles
    "i want to die": "命の価値 창조목적 생명의 가치 하나님의 사랑 God's love value of life 創造目的",
    "don't want to live": "命の価値 창조목적 생명의 가치 하나님의 사랑 God's love value of life",
    "want to disappear": "命の価値 創造目的 하나님의 사랑 창조목적 God's love purpose",
    "feel so lonely": "God's heart shimjung 하나님의 심정 하나님의 외로움 神様の孤独 심정 loneliness",
    "i'm lonely": "God's heart shimjung 하나님의 심정 하나님의 외로움 神様の孤独 loneliness",
    "feel alone": "God's heart shimjung 하나님의 심정 하나님의 외로움 loneliness 心情",
    "i hate myself": "창조목적 자녀의 가치 하나님의 사랑 God's love value 創造目的 子女の価値",
    "worthless": "창조목적 자녀의 가치 하나님의 사랑 God's love value 創造目的",
    "not good enough": "창조목적 자녀의 가치 하나님의 사랑 God's love 創造目的",
    "i'm a failure": "창조목적 자녀의 가치 하나님의 사랑 탕감복귀 God's love indemnity",
    "no confidence": "창조목적 자녀의 가치 하나님의 사랑 God's love 創造目的",
    "i'm depressed": "蕩減復帰 탕감복귀 하나님의 심정 God's heart 심정 indemnity suffering",
    "feel hopeless": "蕩減復帰 창조목적 하나님의 사랑 탕감복귀 God's love purpose",
    "so tired": "蕩減復帰 탕감복귀 하나님의 심정 God's heart indemnity comfort",
    "i'm anxious": "蕩減復帰 神様の摂理 信仰 탕감복귀 하나님의 섭리 믿음 faith providence",
    "feel empty": "創造目的 真の愛 心情 창조목적 참사랑 심정 emptiness purpose true love",
    "no purpose": "創造目的 心情 神様の愛 창조목적 하나님의 사랑 삶의 의미 purpose",
    "what's the point": "創造目的 心情 神様の愛 창조목적 하나님의 사랑 purpose of life",
    "pointless": "創造目的 心情 神様の愛 창조목적 하나님의 사랑 삶의 의미",
    # Relationships / family
    "can't get married": "祝福 축복 참가정 이상가정 blessing marriage True Family 真の家庭",
    "marriage problems": "真の家庭 참가정 수수작용 부부관계 참사랑 授受作用 true love ideal family",
    "want a divorce": "真の家庭 참가정 축복 참사랑 true love ideal family 授受作用",
    "husband and wife": "真の家庭 참가정 수수작용 부부관계 참사랑 true love",
    "relationship with parents": "孝情 효정 부모 하나님의 심정 filial heart parents 心情",
    "hate my parents": "孝情 효정 부모님 하나님의 심정 filial heart 心情",
    "raising children": "真の家庭 참가정 자녀교육 孝情 효정 true family children",
    "kids won't listen": "真の家庭 참가정 자녀교육 孝情 효정 true family",
    "no friends": "心情 真の愛 為に生きる 심정 참사랑 위하여 사는 삶 true love",
    "can't trust": "心情 真の愛 授受作用 탕감복귀 심정 참사랑 trust forgiveness",
    "bullied": "蕩減復帰 原罪 心情 탕감복귀 원죄 심정 indemnity Fall",
    # Anger / forgiveness
    "can't forgive": "蕩減復帰 赦し 心情 탕감복귀 용서 심정 indemnity forgiveness",
    "so angry": "蕩減復帰 原罪 心情 탕감복귀 원죄 심정 indemnity anger",
    "i hate": "蕩減復帰 原罪 赦し 탕감복귀 원죄 용서 indemnity hatred forgiveness",
    "want revenge": "蕩減復帰 赦し 原罪 탕감복귀 용서 원죄 indemnity forgiveness",
    "why do bad things": "蕩減復帰 原罪 堕落論 탕감복귀 원죄 타락론 indemnity Fall",
    "why suffering": "蕩減復帰 原罪 堕落論 탕감복귀 원죄 타락론 indemnity Fall suffering",
    "why do good people suffer": "蕩減復帰 원죄 하나님의 심정 탕감복귀 神様の悲しみ God's sorrow indemnity",
    # Faith / God questions
    "does God exist": "God exists 神様の実存 하나님의 존재 창조원리 Divine Principle",
    "is God real": "God exists 神様の実存 하나님의 존재 창조원리 Divine Principle",
    "God doesn't care": "God's love heart shimjung 하나님의 사랑 하나님의 심정 神様の愛 섭리",
    "God isn't listening": "God's heart prayer shimjung 하나님의 심정 기도 믿음 섭리 faith",
    "can't believe in God": "God exists faith 하나님의 존재 창조원리 믿음 Divine Principle",
    "losing my faith": "神様の摂理 信仰 하나님의 섭리 믿음 심정 faith providence",
    "why God": "God sorrow heart shimjung Fall 하나님의 슬픔 신음 타락 神様の悲しみ 堕落論",
    "why did God": "God sorrow heart shimjung Fall 하나님의 슬픔 타락 神様の悲しみ 堕落論",
    "why is God": "God sorrow heart shimjung 하나님의 심정 섭리 神様の心情 神様の悲しみ",
    "why do we suffer": "蕩減復帰 原罪 堕落論 탕감복귀 원죄 타락론 indemnity Fall suffering",
    "what happens when we die": "霊界 천국 영계 Spirit World heaven 先祖 조상",
    "after death": "霊界 천국 영계 Spirit World heaven 先祖 조상",
    "is there heaven": "霊界 天国 영계 천국 Spirit World heaven",
    "is there hell": "霊界 地獄 영계 지옥 spirit world hell indemnity",
    "evil spirits": "霊界 先祖 堕落 영계 조상 악령 spirit world Fall Satan",
    "spiritual problems": "霊界 先祖解析 蕩減復帰 영계 조상해원 탕감복귀 spirit world",
    "ancestors": "先祖解析 霊界 조상해원 영계 ancestors spirit world",
    "ancestral problems": "先祖解析 霊界 조상해원 영계 원죄 ancestors spirit world indemnity",
    "how to be happy": "真の愛 真の家庭 心情 참사랑 참가정 창조목적 true love happiness purpose",
    "what is happiness": "真の愛 真の家庭 心情 創造目的 참사랑 참가정 심정 true love",
    "purpose of life": "創造目的 心情 神様の愛 창조목적 하나님의 사랑 삶의 의미 Divine Principle",
    "meaning of life": "創造目的 心情 神様の愛 창조목적 하나님의 사랑 Divine Principle purpose",
    "why was i born": "創造目的 創造原理 神様の愛 창조목적 창조원리 하나님의 사랑 purpose",
    "why am i here": "創造目的 心情 神様の愛 창조목적 하나님의 사랑 purpose of life",
    # Technical terms
    "true love": "真の愛 心情 真の家庭 참사랑 심정 참가정 True Love Divine Principle",
    "Principle of Creation": "Principle of Creation 創造原理 창조원리",
    "Four Position Foundation": "Four Position Foundation 四位基台 사위기대",
    "Restoration through Indemnity": "Restoration through Indemnity 蕩減復帰 탕감복귀",
    "True Love": "True Love 真の愛 참사랑",
    "True Parents": "True Parents 真の父母様 참부모님",
    "Divine Principle": "Divine Principle 原理講論 원리강론",
    "Cheon Il Guk": "Cheon Il Guk 天一国 천일국",
    "God's heart": "God's heart shimjung 心情 하나님의 심정 神様の心情",
    "God's love": "God's love 神様の愛 하나님의 사랑 심정 True Love",
    "original sin": "Original Sin 原罪 堕落論 원죄 타락론 Fall",
    "fallen nature": "堕落性本性 堕落論 타락성 타락론 Fall",
    "spirit world": "霊界 天国 영계 천국 Spirit World heaven",
    "indemnity": "蕩減復帰 蕩減 탕감복귀 탕감 indemnity restoration",
    "blessing ceremony": "祝福 祝福結婚 真の家庭 축복 참가정 혈통전환 Blessing",
    "only begotten daughter": "独生女 真の父母様 韓鶴子様 독생녀 참어머님 Only Begotten Daughter",

    # ── J) 韓国語口語パターン（悩み・疑問・感情 全般） ────────────
    # 개인 고통 / Personal struggles
    "죽고 싶다": "창조목적 생명의 가치 하나님의 사랑 자녀의 가치 God's love value of life 命の価値",
    "살기 싫다": "창조목적 생명의 가치 하나님의 사랑 자녀의 가치 God's love purpose",
    "사라지고 싶다": "창조목적 생명의 가치 하나님의 사랑 God's love 命の価値",
    "너무 힘들어": "탕감복귀 하나님의 심정 심정 蕩減復帰 神様の心情 indemnity suffering",
    "힘들어요": "탕감복귀 하나님의 심정 심정 蕩減復帰 indemnity suffering",
    "너무 외로워": "하나님의 외로움 하나님의 심정 창조목적 심정 神様の孤独 loneliness",
    "외로워요": "하나님의 외로움 하나님의 심정 창조목적 심정 loneliness",
    "혼자야": "하나님의 외로움 하나님의 심정 창조목적 심정 神様の孤독 loneliness",
    "나 자신이 싫어": "창조목적 자녀의 가치 하나님의 사랑 God's love value 創造目的",
    "자신감이 없어": "창조목적 자녀의 가치 하나님의 사랑 God's love 創造目的",
    "나는 왜 이럴까": "창조목적 자녀의 가치 탕감복귀 하나님의 사랑 God's love indemnity",
    "우울해": "탕감복귀 하나님의 심정 심정 蕩減復帰 神様の心情 indemnity",
    "무기력해": "창조목적 탕감복귀 위하여 사는 삶 蕩減復帰 創造目的 purpose",
    "불안해": "탕감복귀 하나님의 섭리 믿음 蕩減復帰 神様の摂理 faith providence",
    "허무해": "창조목적 참사랑 심정 하나님의 사랑 삶의 의미 emptiness purpose true love",
    "의미가 없어": "창조목적 하나님의 사랑 삶의 의미 蕩減復帰 God's love purpose",
    "살아야 하는 이유": "창조목적 하나님의 사랑 삶의 의미 하나님의 심정 purpose of life God's love",
    "왜 태어났을까": "창조목적 창조원리 하나님의 사랑 삶의 의미 purpose of creation God's love",
    "왜 태어났는지": "창조목적 창조원리 하나님의 사랑 purpose of creation",
    "행복해지고 싶어": "참사랑 참가정 창조목적 심정 true love happiness purpose",
    "어떻게 하면 행복": "참사랑 참가정 창조목적 심정 true love happiness purpose",
    "지쳐요": "탕감복귀 하나님의 심정 심정 蕩減復帰 indemnity God's heart",
    # 가족·결혼·인간관계 / Family / Relationships
    "결혼이 안 돼": "축복 참가정 이상가정 혈통전환 blessing marriage True Family 祝福",
    "결혼 못 해": "축복 참가정 이상가정 blessing marriage True Family",
    "결혼하고 싶어": "축복 참가정 이상가정 blessing marriage ideal family",
    "부부 사이가": "참가정 수수작용 부부관계 참사랑 true love 真の家庭 授受作用",
    "이혼하고 싶어": "참가정 축복 참사랑 true love ideal family 真の家庭",
    "부모님과 사이가": "효정 부모님 하나님의 심정 filial heart parents 孝情",
    "부모님이 싫어": "효정 부모님 하나님의 심정 심정 filial heart parents",
    "자녀 교육": "참가정 자녀교육 효정 true family children 孝情 真の家庭",
    "아이가 말을 안 들어": "참가정 자녀교육 효정 true family children",
    "인간 관계": "심정 참사랑 수수작용 위하여 사는 삶 心情 授受作用 true love",
    "친구가 없어": "심정 참사랑 위하여 사는 삶 心情 true love loneliness",
    "왕따": "탕감복귀 원죄 심정 蕩減復帰 原罪 indemnity Fall",
    # 분노·용서 / Anger / Forgiveness
    "용서가 안 돼": "탕감복귀 용서 심정 蕩減復帰 心情 indemnity forgiveness",
    "용서 못 해": "탕감복귀 용서 심정 원죄 蕩減復帰 indemnity forgiveness",
    "너무 억울해": "탕감복귀 원죄 심정 蕩減復帰 原罪 indemnity",
    "화가 나": "탕감복귀 원죄 심정 蕩減復帰 原罪 indemnity anger",
    "미워": "탕감복귀 원죄 용서 蕩減復帰 原罪 indemnity hatred",
    "왜 착한 사람이 고생해": "탕감복귀 원죄 하나님의 슬픔 탕감복귀 神様の悲しみ indemnity God's sorrow",
    # 신앙 / Faith questions
    "하나님은 왜": "하나님의 슬픔 하나님의 심정 타락론 섭리 神様の悲しみ 堕落論",
    "왜 하나님": "하나님의 슬픔 하나님의 심정 타락론 원죄 神様の悲しみ 堕落論",
    "하나님이 있어": "하나님의 존재 창조원리 하나님의 심정 God exists Divine Principle 神様の実存",
    "하나님이 계세요": "하나님의 존재 창조원리 하나님의 심정 God exists Divine Principle",
    "하나님을 믿을 수가 없어": "하나님의 존재 창조원리 믿음 faith Divine Principle",
    "신앙이 흔들려": "하나님의 섭리 믿음 심정 섭리 神様の摂理 faith providence",
    "믿음이 없어져": "하나님의 섭리 믿음 심정 섭리 faith providence",
    "기도해도 안 들어줘": "하나님의 심정 기도 믿음 섭리 God's heart prayer faith",
    "하나님은 나를 보고 있어": "하나님의 섭리 하나님의 사랑 섭리 God's love providence",
    "천국이 있어": "영계 천국 지상천국 spirit world heaven 霊界",
    "죽으면 어디로 가": "영계 천국 조상 spirit world heaven ancestors 霊界",
    "귀신이": "영계 조상 악령 타락 spirit world Fall 霊界",
    "악령": "영계 조상해원 타락 사탄 spirit world Fall Satan 霊界",
    "조상 때문에": "조상해원 영계 탕감복귀 원죄 ancestors spirit world indemnity",
    "인연 때문에": "조상해원 영계 탕감복귀 원죄 선조 ancestors indemnity",
    "섭리가 뭐야": "하나님의 섭리 복귀섭리 섭리 providence restoration 神様の摂理",
    "왜 고통": "탕감복귀 원죄 타락론 하나님의 심정 蕩減復帰 原罪 suffering",
    # 기술 용어 / Technical terms
    "창조원리": "창조원리 創造原理 Principle of Creation",
    "사위기대": "사위기대 四位基台 Four Position Foundation",
    "탕감복귀": "탕감복귀 蕩減復帰 Restoration through Indemnity",
    "참사랑": "참사랑 真の愛 True Love",
    "참부모님": "참부모님 真の父母様 True Parents",
    "하나님의 심정": "하나님의 심정 神様の心情 God's heart shimjung",
    "하나님의 사랑": "하나님의 사랑 神様の愛 God's love 참사랑",
    "삶의 의미": "삶의 의미 창조목적 하나님의 사랑 生きる意味 purpose of life",
    "왜 살아야": "삶의 의미 창조목적 하나님의 사랑 生きる意味 purpose",
    "외롭다": "하나님의 외로움 하나님의 심정 창조목적 심정 神様の孤독 loneliness",
    "힘들다": "탕감복귀 하나님의 심정 심정 蕩減復帰 神様の心情 indemnity",
}


def expand_query_with_terms(query: str) -> str:
    """
    クエリ内の専門用語・質問パターンを多言語で展開し、検索精度を向上させる
    """
    additions = []
    for term, expansion in THEOLOGICAL_TERMS.items():
        if term in query:
            additions.append(expansion)
    if additions:
        return query + " " + " ".join(additions)
    return query


def rewrite_query_for_search(query: str, openai_key: str) -> str:
    """
    GPTを使って口語的な質問を神学的検索クエリに変換する。
    TERMSマッチがなかった場合の補完として使用。
    """
    try:
        import urllib.request as _req
        import json as _json
        system_prompt = """You convert colloquial user questions (in Japanese, Korean, OR English) into rich multi-language theological search queries for a Unification Church / Divine Principle RAG database containing:
- 천성경 (Cheon Seong Gyeong), 평화경 (Pyeong Hwa Gyeong), 원리강론 (Divine Principle)
- True Parents' speeches in Japanese, Korean, English

Output ONLY a search query — no explanation. Include the most relevant terms across Korean + Japanese + English.

Key mappings (apply regardless of input language):
- Suffering/hardship/つらい/힘들다 → 蕩減復帰 탕감복귀 心情 심정 하나님의 심정 indemnity God's heart
- Meaning/purpose/生きる意味/살아야 하는 이유 → 創造目的 창조목적 하나님의 사랑 삶의 의미 purpose God's love
- Want to die/死にたい/죽고 싶다 → 命の価値 창조목적 생명의 가치 하나님의 사랑 God's love value of life
- Lonely/孤独/외롭다 → 神様の心情 하나님의 심정 하나님의 외로움 God's heart shimjung loneliness
- Self-hate/自分が嫌い/나 자신이 싫어 → 創造目的 자녀의 가치 하나님의 사랑 God's love value 창조목적
- Marriage/結婚/결혼 → 祝福 축복 참가정 이상가정 blessing True Family 真の家庭
- Couple/夫婦/부부 → 真の家庭 참가정 수수작용 부부관계 참사랑 授受作用 true love
- Parents relation/親/부모님 → 孝情 효정 부모 하나님의 심정 filial heart parents
- Children/子育て/자녀 → 真の家庭 참가정 자녀교육 孝情 true family
- Can't forgive/許せない/용서가 안 돼 → 蕩減復帰 탕감복귀 용서 심정 forgiveness indemnity
- Ancestors/先祖/조상 → 先祖解析 조상해원 霊界 영계 ancestors spirit world
- Does God exist/神様はいる/하나님이 있어 → 神様の実存 하나님의 존재 창조원리 God exists Divine Principle
- After death/天国/천국 → 霊界 영계 천국 Spirit World heaven
- Losing faith/信仰/신앙이 흔들려 → 神様の摂理 하나님의 섭리 믿음 faith 心情 shimjung
- Why God/なぜ神様/하나님은 왜 → 神様の心情 하나님의 심정 하나님의 슬픔 神様の悲しみ God's heart sorrow
- Happiness/幸せ/행복 → 真の愛 참사랑 참가정 창조목적 心情 true love happiness

Examples (Japanese):
Q: 仕事がうまくいかなくてしんどい → 蕩減復帰 心情 탕감복귀 하나님의 심정 為に生きる 위하여 사는 삶 indemnity God's heart
Q: 友達に裏切られて信じられない → 心情 真の愛 授受作用 탕감복귀 용서 심정 forgiveness indemnity
Q: 生まれてきた意味がわからない → 創造目的 心情 神様の愛 창조목적 하나님의 사랑 삶의 의미 purpose of life
Q: 先祖の因縁で苦しんでいる → 先祖解析 霊界 조상해원 영계 탕감복귀 원죄 ancestors indemnity

Examples (Korean):
Q: 너무 외로워서 힘들어요 → 하나님의 심정 하나님의 외로움 탕감복귀 심정 God's heart loneliness 神様の孤独 indemnity
Q: 결혼이 안 돼서 고민이에요 → 축복 참가정 이상가정 혈통전환 blessing marriage True Family 祝福
Q: 하나님이 정말 있는지 모르겠어요 → 하나님의 존재 창조원리 하나님의 심정 God exists Divine Principle 神様の実存
Q: 부모님과 관계가 너무 나빠요 → 효정 부모님 하나님의 심정 孝情 filial heart parents 心情
Q: 신앙이 점점 흔들려요 → 하나님의 섭리 믿음 심정 섭리 神様の摂理 faith providence shimjung

Examples (English):
Q: I feel so empty and don't know why I'm alive → 창조목적 하나님의 사랑 삶의 의미 God's love purpose 創造目的 심정
Q: I can't forgive my father no matter how hard I try → 탕감복귀 용서 심정 蕩減復帰 孝情 forgiveness indemnity filial heart
Q: I'm so lonely and feel like no one cares → 하나님의 심정 하나님의 외로움 God's heart shimjung 神様の心情 loneliness
Q: Why does God let innocent people suffer → 탕감복귀 원죄 하나님의 슬픔 蕩減復帰 원죄 God's sorrow indemnity Fall
Q: I'm losing my faith, I don't feel God anymore → 하나님의 섭리 믿음 하나님의 심정 神様の摂理 faith God's heart providence
Q: My marriage is falling apart → 참가정 수수작용 참사랑 真の家庭 授受作用 부부관계 true love blessing"""

        payload = {
            "model": "gpt-4o-mini",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Q: {query} →"},
            ],
            "max_tokens": 120,
            "temperature": 0,
        }
        req = _req.Request(
            "https://api.openai.com/v1/chat/completions",
            data=_json.dumps(payload).encode(),
            headers={"Authorization": f"Bearer {openai_key}", "Content-Type": "application/json"},
        )
        with _req.urlopen(req, timeout=8) as resp:
            result = _json.loads(resp.read())
        rewritten = result["choices"][0]["message"]["content"].strip()
        print(f"[QueryRewrite] {query!r} → {rewritten!r}")
        return f"{query} {rewritten}"
    except Exception as e:
        print(f"[QueryRewrite] failed: {e}")
        return query


# ============================================================
# 埋め込み・検索
# ============================================================

def get_embedding(text: str) -> List[float]:
    """テキストの埋め込みベクトルを取得"""
    response = openai_request("embeddings", {
        "model": EMBEDDING_MODEL,
        "input": text,
    })
    return response["data"][0]["embedding"]


def get_embeddings_batch(texts: List[str]) -> List[List[float]]:
    """複数テキストを一度のAPIコールで埋め込み取得（最大2048件）"""
    response = openai_request("embeddings", {
        "model": EMBEDDING_MODEL,
        "input": texts,
    })
    items = sorted(response["data"], key=lambda x: x["index"])
    return [item["embedding"] for item in items]


def row_to_dict(row, columns):
    """Convert a tuple row to a dictionary using column names."""
    return {col: val for col, val in zip(columns, row)}


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
    import time as _time
    _db_start = _time.time()
    conn = get_db_connection()
    print(f"[DB] Connection established in {_time.time() - _db_start:.1f}s")
    try:
        cursor = conn.cursor()
        embedding_str = "[" + ",".join(map(str, query_embedding)) + "]"
        
        results = []
        seen_ids = set()
        columns = ["id", "s3_key", "filename", "chunk_index", "chunk_text", "metadata", "similarity"]
        
        # 1. Search in requested language first
        same_lang_k = (top_k + 1) // 2
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
        _q1_start = _time.time()
        cursor.execute(
            query_same_lang,
            (embedding_str, query_language, embedding_str, similarity_threshold, embedding_str, same_lang_k),
        )
        print(f"[DB] Query 1 (same lang) executed in {_time.time() - _q1_start:.1f}s")
        for row in cursor.fetchall():
            row_dict = row_to_dict(row, columns)
            if row_dict["id"] not in seen_ids:
                seen_ids.add(row_dict["id"])
                results.append(row_dict)

        # 2. Cross-language fallback — search all languages if not enough results
        if len(results) < 2:
            cross_lang_query = """
                SELECT 
                    id, s3_key, filename, chunk_index, chunk_text, metadata,
                    1 - (embedding <=> %s::vector) as similarity
                FROM documents
                WHERE 1 - (embedding <=> %s::vector) >= %s
                ORDER BY embedding <=> %s::vector
                LIMIT %s
            """
            _q2_start = _time.time()
            cursor.execute(
                cross_lang_query,
                (embedding_str, embedding_str, similarity_threshold, embedding_str, top_k),
            )
            print(f"[DB] Query 2 (cross-lang fallback) executed in {_time.time() - _q2_start:.1f}s")
            for row in cursor.fetchall():
                row_dict = row_to_dict(row, columns)
                if row_dict["id"] not in seen_ids:
                    seen_ids.add(row_dict["id"])
                    results.append(row_dict)
        
        cursor.close()
        
        # 類似度でソート
        results.sort(key=lambda x: x["similarity"], reverse=True)
        
        return [
            {
                "id": row["id"],
                "s3_key": row["s3_key"],
                "filename": row["filename"],
                "chunk_index": row["chunk_index"],
                "chunk_text": row["chunk_text"],
                "metadata": row["metadata"] if row["metadata"] else {},
                "similarity": float(row["similarity"]),
            }
            for row in results[:top_k]
        ]
    finally:
        conn.close()


# ============================================================
# プロンプト構築
# ============================================================

SYSTEM_PROMPT_TEMPLATE = """You are Mini-Han, a spiritual guide who teaches ONLY from the words and teachings of Reverend Sun Myung Moon and his wife Hak Ja Han Moon (True Parents). Respond in {language_name}.

CRITICAL RULES — never break these:
1. ONLY use the reference passages provided below the question. Do NOT use your general knowledge.
2. If no relevant passage is provided, say: "I couldn't find a direct teaching on this in the provided texts. Please try rephrasing or ask about a related topic."
3. NEVER give a neutral, relativistic, or secular answer. Always ground your reply in True Parents' teachings.
4. NEVER say things like "some people believe" or "it's a personal decision" — give the clear teaching from the texts.
5. Always cite the source (speech title and date) when quoting.
6. Be warm and approachable, but never compromise the content of the teaching.
7. For greetings and small talk, respond naturally. For all theological or ethical questions, rules 1–6 apply strictly.
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
    parts = []
    for i, chunk in enumerate(context_chunks, 1):
        metadata = chunk.get("metadata", {})
        title = (
            metadata.get("speech_title_en")
            or metadata.get("speech_title")
            or metadata.get("book_name")
            or chunk.get("filename", "unknown").replace(".pdf", "")
        )
        date = metadata.get("speech_date", "")
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
# チャット生成
# ============================================================

def generate_chat_response(
    message: str,
    context_chunks: List[Dict[str, Any]],
    language: str,
) -> Dict[str, Any]:
    """
    チャット応答を生成
    
    コンテキストを基に、指定言語で応答を生成する。
    """
    system_prompt = build_system_prompt(language)
    user_prompt = build_user_prompt(message, context_chunks, language)
    
    response = openai_request("chat/completions", {
        "model": CHAT_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.7,
        "max_tokens": 600,
    })
    
    reply = response["choices"][0]["message"]["content"]
    
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
    # CORS preflight
    if event.get("httpMethod") == "OPTIONS":
        return {"statusCode": 200, "headers": {"Access-Control-Allow-Origin": "*", "Access-Control-Allow-Headers": "Content-Type", "Access-Control-Allow-Methods": "POST, OPTIONS"}, "body": ""}

    # Route: /validate-token
    path = event.get("path", "") or event.get("resource", "")
    if "/validate-token" in path:
        try:
            raw = event.get("body", "{}")
            body = json.loads(raw) if isinstance(raw, str) else (raw or {})
            token = body.get("token", "")
            info = get_token_info(token)
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
                "body": json.dumps({
                    "valid": info["valid"],
                    "credits": info.get("credits", 0),
                    "unlimited": info.get("unlimited", False),
                }),
            }
        except Exception as e:
            return {"statusCode": 500, "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}, "body": json.dumps({"error": str(e)})}

    # Route: /redeem-coupon
    if "/redeem-coupon" in path:
        try:
            raw = event.get("body", "{}")
            body = json.loads(raw) if isinstance(raw, str) else (raw or {})
            google_id_token = body.get("google_id_token", "")
            coupon_code = body.get("coupon_code", "")
            if not google_id_token or not coupon_code:
                return {"statusCode": 400, "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}, "body": json.dumps({"success": False, "message": "missing_fields"})}
            google_info = verify_google_token(google_id_token)
            if not google_info:
                return {"statusCode": 401, "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}, "body": json.dumps({"success": False, "message": "invalid_token"})}
            result = redeem_coupon(google_info["sub"], coupon_code)
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
                "body": json.dumps(result),
            }
        except Exception as e:
            return {"statusCode": 500, "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}, "body": json.dumps({"success": False, "message": str(e)})}

    try:
        # リクエスト解析
        if isinstance(event.get("body"), str):
            body = json.loads(event["body"])
        else:
            body = event.get("body", event)
        
        message = body.get("message")
        language = body.get("language", "en")
        google_id_token = body.get("google_id_token", "")
        top_k = body.get("top_k", DEFAULT_TOP_K)
        similarity_threshold = body.get("similarity_threshold", DEFAULT_SIMILARITY_THRESHOLD)

        # Google auth + daily limit check
        if not google_id_token:
            return _error_response(401, "Google sign-in required")

        google_info = verify_google_token(google_id_token)
        if not google_info:
            return _error_response(401, "Invalid Google token")

        google_sub = google_info["sub"]
        google_email = google_info.get("email", "")
        limit_result = check_and_increment_daily_limit(google_sub, google_email)
        print(f"[Chat] google_sub={google_sub[:8]}... count={limit_result['count']}/{limit_result['limit']}")
        if not limit_result["allowed"]:
            return {
                "statusCode": 429,
                "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
                "body": json.dumps({
                    "error": "daily_limit_reached",
                    "count": limit_result["count"],
                    "limit": limit_result["limit"],
                }),
            }
        
        # DB audit (internal use)
        if message == "__audit__":
            conn = get_db_connection()
            cur = conn.cursor()
            audit = {}
            cur.execute("SELECT COUNT(*) FROM documents")
            audit["total_chunks"] = cur.fetchone()[0]
            cur.execute("SELECT metadata->>'language', COUNT(*) FROM documents GROUP BY 1 ORDER BY 2 DESC")
            audit["by_language"] = {r[0]: r[1] for r in cur.fetchall()}
            cur.execute("SELECT metadata->>'source_type', COUNT(*) FROM documents GROUP BY 1 ORDER BY 2 DESC")
            audit["by_source_type"] = {r[0]: r[1] for r in cur.fetchall()}
            cur.execute("SELECT DISTINCT filename FROM documents ORDER BY filename")
            audit["indexed_files"] = [r[0] for r in cur.fetchall()]
            cur.close()
            conn.close()
            return {"statusCode": 200, "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}, "body": json.dumps(audit, ensure_ascii=False)}

        # Bulk insert pre-translated chunks (internal use)
        # Body: {"message": "__insert_chunks__", "chunks": [{filename, s3_key, chunk_index, chunk_text, metadata}, ...]}
        if message == "__insert_chunks__":
            chunks = body.get("chunks", [])
            if not chunks:
                return {"statusCode": 400, "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}, "body": json.dumps({"error": "no chunks"})}
            openai_key = get_openai_api_key()
            conn = get_db_connection()
            cur = conn.cursor()
            inserted = 0
            errors = 0
            try:
                # If chunks already have pre-computed embeddings, skip OpenAI call
                if chunks[0].get("embedding"):
                    embeddings = [c["embedding"] for c in chunks]
                else:
                    texts = [c.get("chunk_text") or c.get("text", "") for c in chunks]
                    embeddings = get_embeddings_batch(texts)
                for chunk, emb in zip(chunks, embeddings):
                    try:
                        chunk_text = chunk.get("chunk_text") or chunk.get("text", "")
                        emb_str = "[" + ",".join(map(str, emb)) + "]"
                        s3_key = chunk.get("s3_key") or chunk.get("source_url") or chunk.get("filename", "")
                        cur.execute("""
                            INSERT INTO documents (s3_key, filename, chunk_index, chunk_text, metadata, embedding)
                            VALUES (%s, %s, %s, %s, %s, %s::vector)
                            ON CONFLICT (s3_key, chunk_index) DO UPDATE
                              SET chunk_text = EXCLUDED.chunk_text,
                                  metadata   = EXCLUDED.metadata,
                                  embedding  = EXCLUDED.embedding
                        """, (
                            s3_key,
                            chunk.get("filename", ""),
                            chunk.get("chunk_index", 0),
                            chunk_text,
                            json.dumps(chunk.get("metadata", {})),
                            emb_str,
                        ))
                        inserted += 1
                    except Exception as e:
                        errors += 1
                        print(f"[Insert] row error: {e}")
                conn.commit()
            except Exception as e:
                errors += len(chunks)
                print(f"[Insert] batch error: {e}")
            finally:
                cur.close()
                conn.close()
            print(f"[Insert] inserted={inserted} errors={errors}")
            return {"statusCode": 200, "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}, "body": json.dumps({"inserted": inserted, "errors": errors})}

        # Similarity probe (internal use) — message format: "__probe__:<query text>"
        if message.startswith("__probe__:"):
            probe_query = message[len("__probe__:"):]
            emb = get_embedding(probe_query)
            emb_str = "[" + ",".join(map(str, emb)) + "]"
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                SELECT filename, metadata->>'language', metadata->>'source_type',
                       1 - (embedding <=> %s::vector) as sim,
                       LEFT(chunk_text, 120)
                FROM documents
                ORDER BY embedding <=> %s::vector
                LIMIT 10
            """, (emb_str, emb_str))
            rows = cur.fetchall()
            cur.close()
            conn.close()
            results = [{"filename": r[0], "lang": r[1], "source_type": r[2], "similarity": round(float(r[3]), 4), "excerpt": r[4]} for r in rows]
            return {"statusCode": 200, "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}, "body": json.dumps(results, ensure_ascii=False)}

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
        
        import time as _time
        _t0 = _time.time()
        
        # 専門用語を含むクエリを展開（検索精度向上）
        expanded_query = expand_query_with_terms(message)
        if expanded_query != message:
            print(f"[Chat] Query expanded with theological terms")
        else:
            # フォールバック: GPTで口語的質問を神学的クエリに変換
            openai_key_for_rewrite = get_openai_api_key()
            expanded_query = rewrite_query_for_search(message, openai_key_for_rewrite)
        
        # 埋め込み取得（展開されたクエリを使用）
        query_embedding = get_embedding(expanded_query)
        print(f"[Timing] Embedding: {_time.time() - _t0:.1f}s")
        
        _t1 = _time.time()
        # ベクトル検索（クエリ言語を優先）
        context_chunks = vector_search(query_embedding, language, effective_top_k, effective_threshold)
        
        # フォールバック: 結果が少ない場合、より低い閾値で再検索
        if fallback_threshold and len(context_chunks) < 3 and effective_threshold > fallback_threshold:
            print(f"[Chat] Falling back to lower threshold: {fallback_threshold}")
            context_chunks = vector_search(query_embedding, language, effective_top_k, fallback_threshold)
        print(f"[Timing] DB Search: {_time.time() - _t1:.1f}s")
        
        print(f"[Chat] Found {len(context_chunks)} relevant chunks")
        
        _t2 = _time.time()
        # チャット応答生成
        result = generate_chat_response(message, context_chunks, language)
        print(f"[Timing] Chat: {_time.time() - _t2:.1f}s")

        result["queries_used"] = limit_result["count"]
        result["queries_remaining"] = max(0, limit_result["limit"] - limit_result["count"])
        result["daily_limit"] = limit_result["limit"]
        result["unlimited"] = limit_result.get("unlimited", False)

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
