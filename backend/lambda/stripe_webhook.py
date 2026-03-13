"""
Stripe Webhook Lambda
Handles subscription payments and cancellations for Mini-Han freemium model.

Events handled:
  checkout.session.completed  → generate token, store in DynamoDB, email subscriber
  customer.subscription.deleted → revoke token in DynamoDB
"""
import hashlib
import hmac
import json
import os
import secrets
import time
from typing import Optional

import boto3

# ============================================================
# Config
# ============================================================

DYNAMO_TABLE = os.environ.get("TOKEN_TABLE", "mini-han-tokens")
SENDER_EMAIL = os.environ.get("SENDER_EMAIL", "")          # SES verified address
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "")  # whsec_...

dynamo = boto3.resource("dynamodb", region_name=os.environ.get("AWS_REGION", "ap-northeast-1"))
table = dynamo.Table(DYNAMO_TABLE)
ses = boto3.client("ses", region_name=os.environ.get("AWS_REGION", "ap-northeast-1"))
secrets_client = boto3.client("secretsmanager", region_name=os.environ.get("AWS_REGION", "ap-northeast-1"))


# ============================================================
# Stripe signature verification (no stripe library needed)
# ============================================================

def _verify_stripe_signature(payload: bytes, sig_header: str, secret: str) -> bool:
    """Verify Stripe webhook signature using HMAC-SHA256."""
    try:
        parts = {k: v for k, v in (p.split("=", 1) for p in sig_header.split(","))}
        timestamp = parts.get("t", "")
        v1_sig = parts.get("v1", "")
        signed_payload = f"{timestamp}.{payload.decode('utf-8')}".encode("utf-8")
        expected = hmac.new(secret.encode("utf-8"), signed_payload, hashlib.sha256).hexdigest()
        # Reject if timestamp is more than 5 minutes old
        if abs(time.time() - int(timestamp)) > 300:
            return False
        return hmac.compare_digest(expected, v1_sig)
    except Exception:
        return False


# ============================================================
# Token helpers
# ============================================================

def generate_token() -> str:
    return secrets.token_urlsafe(24)


# Credit pack sizes keyed by amount_total in cents (USD)
# $1.00 = 200 credits, $2.50 = 600 credits, $5.00 = 1500 credits
CREDIT_PACKS = {
    100:  200,   # $1.00
    250:  600,   # $2.50
    500: 1500,   # $5.00
}
DEFAULT_CREDITS = 200  # fallback if amount not matched


def credits_for_amount(amount_cents: int) -> int:
    return CREDIT_PACKS.get(amount_cents, DEFAULT_CREDITS)


def store_token(token: str, email: str, subscription_id: str, customer_id: str, credits: int = -1) -> None:
    item = {
        "token": token,
        "email": email,
        "subscription_id": subscription_id,
        "customer_id": customer_id,
        "active": True,
        "created_at": int(time.time()),
        "credits": credits,  # -1 = unlimited (legacy subscription), >0 = credit pack
    }
    table.put_item(Item=item)


def revoke_token_by_subscription(subscription_id: str) -> Optional[str]:
    """Mark token inactive by subscription_id. Returns the email if found."""
    resp = table.scan(
        FilterExpression="subscription_id = :sid",
        ExpressionAttributeValues={":sid": subscription_id},
    )
    for item in resp.get("Items", []):
        table.update_item(
            Key={"token": item["token"]},
            UpdateExpression="SET active = :f",
            ExpressionAttributeValues={":f": False},
        )
        return item.get("email")
    return None


def get_stripe_webhook_secret() -> str:
    """Get webhook secret from env or Secrets Manager."""
    if STRIPE_WEBHOOK_SECRET:
        return STRIPE_WEBHOOK_SECRET
    secret_arn = os.environ.get("STRIPE_WEBHOOK_SECRET_ARN", "")
    if secret_arn:
        resp = secrets_client.get_secret_value(SecretId=secret_arn)
        return resp["SecretString"]
    return ""


# ============================================================
# Email helpers
# ============================================================

def send_token_email(to_email: str, token: str, credits: int = -1) -> None:
    if not SENDER_EMAIL:
        print(f"[Webhook] No SENDER_EMAIL configured — token for {to_email}: {token} credits={credits}")
        return
    if credits == -1:
        credits_line = "<p>Your token provides <strong>unlimited access</strong>.</p>"
    else:
        credits_line = f"<p>Your token includes <strong>{credits} credits</strong> (1 credit = 1 question).</p>"
    ses.send_email(
        Source=SENDER_EMAIL,
        Destination={"ToAddresses": [to_email]},
        Message={
            "Subject": {"Data": "Your AI Holy Mother Han access token"},
            "Body": {
                "Html": {
                    "Data": f"""
<p>Thank you for your purchase of <strong>AI Holy Mother Han</strong>!</p>
<p>Your personal access token is:</p>
<p style="font-size:1.4em;font-weight:bold;letter-spacing:2px;background:#f4f4f4;padding:12px 20px;border-radius:6px;display:inline-block">{token}</p>
{credits_line}
<p>To activate your credits:</p>
<ol>
  <li>Visit <a href="https://aiholymotherhan.com">aiholymotherhan.com</a></li>
  <li>When the prompt appears, click <em>Already have a token?</em></li>
  <li>Paste your token and click Activate</li>
</ol>
<p>Your credit balance is shown in the chat header. When you run low, simply purchase more credits.</p>
<p>Keep this token safe — it works on any device.</p>
<p>Questions? Reply to this email.</p>
"""
                }
            },
        },
    )
    print(f"[Webhook] Token email sent to {to_email} credits={credits}")


def send_cancellation_email(to_email: str) -> None:
    if not SENDER_EMAIL:
        print(f"[Webhook] No SENDER_EMAIL — cancellation for {to_email}")
        return
    ses.send_email(
        Source=SENDER_EMAIL,
        Destination={"ToAddresses": [to_email]},
        Message={
            "Subject": {"Data": "Your Mini-Han subscription has been cancelled"},
            "Body": {
                "Html": {
                    "Data": """
<p>Your <strong>Mini-Han</strong> subscription has been cancelled.</p>
<p>Your access token has been deactivated. You can still use Mini-Han
with the free 10-prompt limit.</p>
<p>We hope to see you again!</p>
"""
                }
            },
        },
    )


# ============================================================
# Lambda handler
# ============================================================

def lambda_handler(event, context):
    # Parse raw body and headers
    body_raw = event.get("body", "")
    if event.get("isBase64Encoded"):
        import base64
        body_bytes = base64.b64decode(body_raw)
    else:
        body_bytes = body_raw.encode("utf-8") if isinstance(body_raw, str) else body_raw

    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
    sig_header = headers.get("stripe-signature", "")

    # Verify signature
    webhook_secret = get_stripe_webhook_secret()
    if webhook_secret and not _verify_stripe_signature(body_bytes, sig_header, webhook_secret):
        print("[Webhook] Signature verification failed")
        return _resp(400, {"error": "Invalid signature"})

    try:
        stripe_event = json.loads(body_bytes)
    except Exception:
        return _resp(400, {"error": "Invalid JSON"})

    event_type = stripe_event.get("type", "")
    data = stripe_event.get("data", {}).get("object", {})
    print(f"[Webhook] Event: {event_type}")

    # ── Payment succeeded → issue token ──────────────────────
    if event_type == "checkout.session.completed":
        email = data.get("customer_details", {}).get("email") or data.get("customer_email", "")
        subscription_id = data.get("subscription", "")
        customer_id = data.get("customer", "")
        amount_cents = data.get("amount_total", 0)  # in smallest currency unit
        mode = data.get("mode", "payment")  # "payment" = one-time, "subscription" = recurring

        if not email:
            print("[Webhook] No email in checkout session")
            return _resp(200, {"status": "no_email"})

        # One-time credit pack → fixed credits; subscription → unlimited (-1)
        if mode == "subscription":
            credits = -1
        else:
            credits = credits_for_amount(amount_cents)

        token = generate_token()
        store_token(token, email, subscription_id or "", customer_id or "", credits=credits)
        send_token_email(email, token, credits=credits)
        print(f"[Webhook] Token issued for {email} mode={mode} credits={credits}")

    # ── Subscription cancelled → revoke token ────────────────
    elif event_type in ("customer.subscription.deleted", "customer.subscription.updated"):
        subscription_id = data.get("id", "")
        status = data.get("status", "")

        if event_type == "customer.subscription.updated" and status not in ("canceled", "unpaid", "past_due"):
            return _resp(200, {"status": "ignored"})

        email = revoke_token_by_subscription(subscription_id)
        if email:
            send_cancellation_email(email)
            print(f"[Webhook] Token revoked for subscription={subscription_id} email={email}")
        else:
            print(f"[Webhook] No token found for subscription={subscription_id}")

    return _resp(200, {"received": True})


def _resp(status: int, body: dict) -> dict:
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(body),
    }
