"""
stale_deal_scanner.py
─────────────────────
Daily Lambda (EventBridge, 8 AM ET).

REPORT ONLY. The daily scan never changes a deal in Pipeline and never emails a
client; its only output is Chad's report email. (Auto-obsolete and client removal
notices were removed; client reminders will be a separate future feature.)

Flow per active deal (Confirm / Firm / Inquiry — Hold deals are skipped entirely):
  clock start = later of Pipeline updated_at and Chad's last Reset click
  1. DUE       — age >= refresh days → "Due for review" in Chad's report (oldest
                 first) with a Reset link
  2. HEADS-UP  — due within 7 days → listed in Chad's report with a Reset link

State: s3://full-pipeline-cache/stale-scanner-state.json (reset_at / obsoleted_at).
  Leftover noticed_at / updated_at_at_notice keys are stripped on every run.
  If state cannot be saved, the run stops before sending anything.

Stage defaults (days):  Confirm=1, Firm=60, Inquiry=60 (Hold=90 used only by the Reset page)
Refresh=0 → skip forever | Refresh=N → N days | blank → stage default

Invocations:
  EventBridge (no special keys)      → daily scan (report only)
  {"diagnose": true}                 → legacy diagnostic (unchanged)
  {"restore": "dry"}                 → list the Sep 23 2026 obsoleted deals, change nothing
  {"restore": "execute"}             → move them to Inquiry, Refresh = 90
  {"backfill_last_confirmed": "dry"}     → list active deals with blank Last Confirmed, change nothing
  {"backfill_last_confirmed": "execute"} → write Last Confirmed = snapshot updated_at date (one-time)
  Function URL GET/POST ?action=reset → Chad's Reset button
  Function URL ?action=review&key=ADMIN_KEY → Obsolete review page (tick deals to restore;
                                       review_apply / review_undo are its POSTs)

Env vars: FORM_LAMBDA_URL, HMAC_SECRET, DRY_RUN, TEST_EMAIL_OVERRIDE, ADMIN_KEY (review page),
          SCANNER_URL (this Lambda's Function URL; Reset links omitted if blank)
"""

import json
import logging
import urllib.request
import urllib.error
import urllib.parse
import boto3
import hmac
import hashlib
import base64
import html
import os
import time
from datetime import datetime, timezone, timedelta

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Config ────────────────────────────────────────────────────────────────────
CACHE_BUCKET        = "full-pipeline-cache"
STATE_KEY           = "stale-scanner-state.json"
SES_SENDER          = "agent@agent.graciagroup.com"
CLIENT_FROM         = "Chad Gracia <cgracia@graciagroup.com>"
REPLY_TO            = "cgracia@rainmakersecurities.com"
CHAD_EMAIL          = "cgracia@rainmakersecurities.com"
BEDROCK_MODEL       = "us.anthropic.claude-sonnet-4-6"
OBSOLETE_STAGE_ID   = 2348038
INQUIRY_STAGE_ID    = 2109142
FORM_LAMBDA_URL     = os.environ.get("FORM_LAMBDA_URL", "")
HMAC_SECRET         = os.environ.get("HMAC_SECRET", "change-me-in-env")
DRY_RUN             = os.environ.get("DRY_RUN", "false").lower() == "true"
TEST_EMAIL_OVERRIDE = os.environ.get("TEST_EMAIL_OVERRIDE", "").strip()
SCANNER_URL         = os.environ.get("SCANNER_URL", "").strip()
ADMIN_KEY           = os.environ.get("ADMIN_KEY", "").strip()   # review page; blank → page refuses all access

GRACE_DAYS          = 3
HEADSUP_DAYS        = 7
MAX_NOTICES_PER_RUN = 25

# Refresh field
REFRESH_FIELD      = "custom_label_3994687"

# Stage ID → name + default days
STAGE_DEFAULTS = {
    2388323: ("Confirm",  1),
    111800:  ("Firm",     60),
    2109142: ("Inquiry",  60),
    2094373: ("Hold",     90),
}

# Deal type custom field
DEAL_TYPE_FIELD  = "custom_label_1958"
SELL_TYPE_ID     = 5011675
BUY_TYPE_ID      = 5077819

# Price fields
GROSS_FIELD      = "custom_label_3064339"
NET_FIELD        = "custom_label_3064369"
MIN_SIZE_FIELD   = "custom_label_3065488"
MAX_SIZE_FIELD   = "custom_label_3064645"

# One-time restore of the deals obsoleted by the Sep 23 2026 run
RESTORE_DATE = "2026/09/23"
RESTORE_NAMES = {
    "Atoms: $1M Buy", "Anduril: Sell", "Zipline: $10M Buy", "Kraken: Sell",
    "Hadrian: $34M Buy", "OpenAI: $3M Buy", "Polymarket: $2.3M Sell", "ICON: $5M Sell",
    "Prometheus: $5M Buy", "Anthropic: $50M Buy", "Anthropic: Buy", "Stripe: $125M Sell",
    "Supabase: $3M Buy", "Bytedance: $10M Sell", "Mistral AI: $15M Sell",
    "Anthropic: $2.4M Buy", "Saronic: $5M Buy", "Tenstorrent: $7M Buy",
    "Starcloud: $10M Buy", "Waymo: $10M Buy", "Cognition AI: $10M Buy",
    "Mistral AI: $10M Buy", "Figure AI: $10M Sell", "Starcloud: $500K Buy",
    "Neuralink: $5M Buy", "Rippling: $15M Buy", "Anthropic: $3M Buy", "Plaid: $400K Buy",
    "Fivetran: $500K Buy", "Canva: $1M Buy", "Anysphere (Cursor): $1.5M Buy",
    "Isomorphic Labs: $5M Buy", "Xsight: $250K Buy", "Project Prometheus: $250K Buy",
    "Kalshi: $5M Buy", "Project Prometheus: $1M Buy", "Apollo.io: $1.8M Sell",
    "Notion: $7M Sell", "Remote: $1M Sell", "Chainalysis: $100K Sell", "SpotOn: $700K Sell",
    "OurCrowd: $1.7M Sell", "Aleo: $10M Sell", "Flexport: $5M Sell", "GrubMarket: $2M Buy",
    "Flexport: $450K Sell",
}
RESTORE_EXPECTED = 50

# One-time backfill of Last Confirmed (LAST_CONFIRMED_FIELD, defined with the review page below)
BACKFILL_STAGES = {
    2109142: "Inquiry", 111800: "Firm", 2094373: "Hold", 2381534: "Matched",
    2388323: "Confirm", 2517909: "LOI Signed", 2381535: "SPA Signed",
    2533998: "Transfer Notice", 2426790: "ROFR-ed",
}
# updated_at dates from the Sep 23 2026 bulk restore are not client confirmations: leave blank.
BACKFILL_SKIP_DATE = "2026-09-23"
BACKFILL_PACE_SECONDS = 0.3
BACKFILL_MAX_RETRIES = 3
BACKFILL_MAX_CONSECUTIVE_FAILURES = 5


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_jwt():
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket="pipeline-token", Key="pipeline-jwt.json")
    return json.loads(obj['Body'].read())['jwt']


def diagnose_stale_deals():
    """One-shot diagnostic. Classifies why each stale deal can't be processed.
    Sends Chad a summary email. Sends NO emails to contacts. Modifies NO deals."""
    jwt = get_jwt()
    deals_data = load_json_from_s3(CACHE_BUCKET, "deals.json")
    all_deals = deals_data.get("deals", [])
    now = datetime.now(timezone.utc)

    stale = []
    for deal in all_deals:
        cf = deal.get("custom_fields", {})
        stage_id = (deal.get("deal_stage") or {}).get("id")
        if stage_id not in STAGE_DEFAULTS:
            continue
        _name, stage_default = STAGE_DEFAULTS[stage_id]
        refresh_raw = parse_cf_value(cf, REFRESH_FIELD)
        if refresh_raw is not None and refresh_raw != "":
            try:
                refresh_days = int(float(str(refresh_raw)))
            except (ValueError, TypeError):
                refresh_days = stage_default
        else:
            refresh_days = stage_default
        if refresh_days == 0:
            continue
        age = days_since(deal.get("updated_at") or deal.get("updated", ""))
        if age < refresh_days:
            continue
        stale.append((age, deal))
    stale.sort(key=lambda x: x[0], reverse=True)

    buckets = {"no_contact_id": [], "api_error": [], "no_email": [], "would_process": []}
    for age, deal in stale:
        deal_id   = deal.get("id")
        deal_name = deal.get("name", str(deal_id))
        contact   = deal.get("primary_contact") or {}
        contact_id = contact.get("id") or deal.get("primary_contact_id")

        if not contact_id:
            buckets["no_contact_id"].append(f"{deal_name} (id={deal_id}, {age}d old)")
            continue

        result = call_pipeline_api("GET", f"/people/{contact_id}.json", jwt=jwt)
        if result["status"] != 200:
            buckets["api_error"].append(
                f"{deal_name} (id={deal_id}, person={contact_id}, http={result['status']})"
            )
            continue

        email = (result["data"].get("email") or "").strip()
        if not email:
            person_name = result["data"].get("full_name") or result["data"].get("name", "?")
            buckets["no_email"].append(
                f"{deal_name} (id={deal_id}, person={person_name}/{contact_id})"
            )
            continue

        buckets["would_process"].append(f"{deal_name} (id={deal_id}, → {email})")

    total = sum(len(v) for v in buckets.values())
    lines = [
        f"Stale Deal Scanner — DIAGNOSTIC ONLY ({now.strftime('%Y-%m-%d')})",
        "",
        f"Total stale deals identified: {total}",
        "",
    ]
    for reason in ["would_process", "no_email", "api_error", "no_contact_id"]:
        items = buckets[reason]
        lines.append(f"[{reason}] ({len(items)})")
        for it in items:
            lines.append(f"  • {it}")
        lines.append("")

    try:
        send_email(CHAD_EMAIL,
                   f"Stale Deal Scanner — Diagnostic — {now.strftime('%Y-%m-%d')}",
                   "\n".join(lines))
    except Exception as e:
        logger.error(f"Could not send diagnostic email: {e}")

    logger.info(f"Diagnostic complete: {[(k, len(v)) for k, v in buckets.items()]}")
    return {"statusCode": 200, "diagnostic": True,
            "buckets": {k: len(v) for k, v in buckets.items()}}


def call_pipeline_api(method, endpoint, payload=None, jwt=None):
    base = "https://api.pipelinecrm.com/api/v3"
    url  = f"{base}{endpoint}"
    headers = {
        "Authorization": f"Bearer {jwt}",
        "Content-Type":  "application/json"
    }
    data = json.dumps(payload).encode() if payload else None
    req  = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return {"status": r.status, "data": json.loads(r.read().decode())}
    except urllib.error.HTTPError as e:
        return {"status": e.code, "data": e.read().decode()}
    except Exception as e:
        return {"status": 500, "data": str(e)}


def load_json_from_s3(bucket, key):
    s3  = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(obj['Body'].read())


def load_state():
    try:
        st = load_json_from_s3(CACHE_BUCKET, STATE_KEY)
        if isinstance(st, dict) and isinstance(st.get("deals"), dict):
            return st
    except Exception as e:
        logger.info(f"No existing state ({e}); starting fresh")
    return {"deals": {}}


def save_state(state):
    """Returns True on success. Callers must not send notices if this fails."""
    try:
        state["saved_at"] = datetime.now(timezone.utc).isoformat()
        boto3.client('s3').put_object(
            Bucket=CACHE_BUCKET, Key=STATE_KEY,
            Body=json.dumps(state).encode(), ContentType="application/json")
        return True
    except Exception as e:
        logger.error(f"State save failed: {e}")
        return False


def make_token(deal_id: int) -> str:
    """HMAC-SHA256 token so the form URL can't be guessed."""
    msg = f"{deal_id}".encode()
    sig = hmac.new(HMAC_SECRET.encode(), msg, hashlib.sha256).digest()
    return base64.urlsafe_b64encode(sig).decode().rstrip("=")


def make_reset_token(deal_id) -> str:
    msg = f"reset:{deal_id}".encode()
    sig = hmac.new(HMAC_SECRET.encode(), msg, hashlib.sha256).digest()
    return base64.urlsafe_b64encode(sig).decode().rstrip("=")


def form_url(deal_id: int) -> str:
    token = make_token(deal_id)
    return f"{FORM_LAMBDA_URL}?deal_id={deal_id}&token={token}"


def reset_url(deal_id) -> str:
    if not SCANNER_URL:
        return ""
    return f"{SCANNER_URL.rstrip('/')}/?action=reset&deal_id={deal_id}&token={make_reset_token(deal_id)}"


def parse_pipeline_dt(s: str):
    """Pipeline format '2024/11/26 06:19:44 -0500' → aware datetime (offset ignored, as before)."""
    if not s:
        return None
    try:
        t = s.strip().replace("/", "-")[:19]
        return datetime.strptime(t, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def days_since(updated_at_str: str) -> int:
    """Return days since updated_at. Handles Pipeline format: '2024/11/26 06:19:44 -0500'."""
    dt = parse_pipeline_dt(updated_at_str)
    if not dt:
        return 0
    return (datetime.now(timezone.utc) - dt).days


def parse_cf_value(cf, field):
    """Safely get a custom field value; returns None if missing/empty."""
    v = cf.get(field)
    if isinstance(v, list):
        return v[0] if v else None
    return v


def is_sell(cf) -> bool:
    type_ids = cf.get(DEAL_TYPE_FIELD, [])
    if isinstance(type_ids, list):
        return SELL_TYPE_ID in type_ids
    return type_ids == SELL_TYPE_ID


def fmt_price(value) -> str:
    if value is None or value == "" or value == 0:
        return "not set"
    try:
        return f"${float(value):,.2f}"
    except Exception:
        return str(value)


def fmt_size(mn, mx) -> str:
    mn_s = fmt_price(mn)
    mx_s = fmt_price(mx)
    if mn_s == mx_s:
        return mn_s
    if mn_s == "not set":
        return f"up to {mx_s}"
    if mx_s == "not set":
        return f"from {mn_s}"
    return f"{mn_s}–{mx_s}"


def fmt_day(dt) -> str:
    return dt.strftime("%b %d").replace(" 0", " ")


# ── Opposite trades ───────────────────────────────────────────────────────────

def find_opposite_trades(company_name: str, is_sell_deal: bool, all_deals: list) -> list:
    """
    Return up to 5 opposite-side FIRM deals for the same company.
    Sell deal  → find Buy deals
    Buy  deal  → find Sell deals
    Only Firm stage (111800) — stale/inquiry deals are not useful pricing signals.
    """
    want_sell    = not is_sell_deal
    results      = []
    company_lower = company_name.lower()
    FIRM_STAGE_ID = 111800

    for d in all_deals:
        if company_lower not in (d.get("name") or "").lower() and \
           company_lower not in (d.get("company_name") or "").lower():
            continue
        if (d.get("deal_stage") or {}).get("id") != FIRM_STAGE_ID:
            continue
        cf     = d.get("custom_fields", {})
        d_sell = is_sell(cf)
        if d_sell != want_sell:
            continue
        price = parse_cf_value(cf, NET_FIELD if d_sell else GROSS_FIELD)
        results.append({
            "id":    d["id"],
            "name":  d.get("name", ""),
            "price": price,
            "side":  "Sell" if d_sell else "Buy",
        })

    results.sort(key=lambda x: float(x["price"]) if x["price"] else 0, reverse=True)
    return results[:5]


def summarise_opposite(opposite_trades: list, is_sell_deal: bool) -> str:
    if not opposite_trades:
        return ""
    side = "buyer" if is_sell_deal else "seller"
    prices = [float(t["price"]) for t in opposite_trades if t["price"]]
    if not prices:
        return f"We currently have {len(opposite_trades)} {side}(s) on record for this company."
    lo, hi = min(prices), max(prices)
    if lo == hi:
        return f"We currently have a {side} at ${lo:,.2f}."
    return f"We currently have {side}s ranging from ${lo:,.2f} to ${hi:,.2f}."


# ── Bedrock email composer ────────────────────────────────────────────────────

def compose_email(deal: dict, contact_name: str, contact_email: str,
                  opposite_summary: str, update_url: str, days_old: int,
                  removal_date: str):
    """
    Ask Bedrock to write a personalised email.
    Returns (subject, plain_text, html).
    """
    bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")

    cf        = deal.get("custom_fields", {})
    sell      = is_sell(cf)
    side      = "sell" if sell else "buy"
    price_lbl = "net price" if sell else "gross price"
    price_val = fmt_price(parse_cf_value(cf, NET_FIELD if sell else GROSS_FIELD))
    size_val  = fmt_size(parse_cf_value(cf, MIN_SIZE_FIELD), parse_cf_value(cf, MAX_SIZE_FIELD))
    company   = (deal.get("company") or {}).get("name") or deal.get("company_name") or deal.get("name", "the company")

    prompt = f"""You are writing an automated system notification email for Chad Gracia, a registered broker at Rainmaker Securities. This is NOT from Rainmaker Securities corporate — it is from Chad's personal book of business. Do not sign it with any name. Do not write it as a personal letter.

Context:
- Recipient: {contact_name} ({contact_email})
- They have a {side} order on file for {company}
- {price_lbl.capitalize()}: {price_val}
- Size: {size_val}
- Deal has not been updated in {days_old} days
- It will be removed on {removal_date} unless they update it
- {f"Current Firm {('buyers' if sell else 'sellers')} for {company}: {opposite_summary}" if opposite_summary else ""}

Write a short, neutral notification (2-3 sentences). State that their {company} {side} order is due for review and will be removed from the system on {removal_date} unless they update it using the button below. Professional but not stiff — straightforward and clear. Never use the word "confirm".

Output ONLY the body text — no subject, no headers, no links, no sign-off. Just the 2-3 sentence message. Plain text only.

Output format — respond with ONLY valid JSON, no markdown:
{{"subject": "Your {company} {side} order — action needed", "body": "..."}}"""

    response = bedrock.invoke_model(
        modelId=BEDROCK_MODEL,
        body=json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 600,
            "messages": [{"role": "user", "content": prompt}]
        }),
        contentType="application/json",
        accept="application/json"
    )
    raw  = json.loads(response["body"].read())
    text = raw["content"][0]["text"].strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
    parsed  = json.loads(text.strip())
    subject = parsed.get("subject", f"Your {company} {side} order — action needed")
    body_text = parsed.get("body", "").strip()

    unsub_url = f"{FORM_LAMBDA_URL}?action=unsubscribe&person_id={deal.get('primary_contact_id', '')}&token={make_token(deal.get('primary_contact_id', 0))}"

    html_body = f"""<html><body style="font-family:Arial,sans-serif;font-size:14px;color:#222;max-width:600px;margin:40px auto;line-height:1.6;">
<p style="font-size:11px;text-transform:uppercase;letter-spacing:1px;color:#888;margin-bottom:24px;">AUTOMATED DEAL UPDATE</p>
<p>{body_text}</p>
<p style="margin-top:28px;">
  <a href="{update_url}" style="background:#1a1a1a;color:#fff;padding:10px 20px;text-decoration:none;border-radius:4px;font-size:13px;">Update your order</a>
</p>
<p style="margin-top:40px;font-size:11px;color:#aaa;">
  Questions? Contact Chad Gracia at <a href="mailto:cgracia@rainmakersecurities.com" style="color:#aaa;">cgracia@rainmakersecurities.com</a><br>
  <a href="{unsub_url}" style="color:#aaa;">Unsubscribe</a>
</p>
</body></html>"""

    plain = f"AUTOMATED DEAL UPDATE\n\n{body_text}\n\nUpdate your order: {update_url}\n\nUnsubscribe: {unsub_url}\n\nQuestions? cgracia@rainmakersecurities.com"

    return subject, plain, html_body


# ── SES sender ────────────────────────────────────────────────────────────────

def send_email(to_address: str, subject: str, body_text: str, body_html: str = None,
               source: str = SES_SENDER, reply_to: str = None):
    ses = boto3.client("ses", region_name="us-east-1")
    msg = {"Text": {"Data": body_text}}
    if body_html:
        msg["Html"] = {"Data": body_html}
    kwargs = dict(
        Source=source,
        Destination={"ToAddresses": [to_address]},
        Message={"Subject": {"Data": subject}, "Body": msg},
    )
    if reply_to:
        kwargs["ReplyToAddresses"] = [reply_to]
    ses.send_email(**kwargs)


# ── Reset button (Function URL) ───────────────────────────────────────────────

def _page(title, inner):
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "text/html; charset=utf-8"},
        "body": f"""<!doctype html><html><head><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{html.escape(title)}</title></head>
<body style="font-family:Arial,sans-serif;max-width:520px;margin:60px auto;padding:0 20px;color:#222;line-height:1.5;">
{inner}</body></html>""",
    }


def _fmt_date(s):
    dt = parse_pipeline_dt(s or "")
    return dt.strftime("%b %d, %Y").replace(" 0", " ") if dt else "—"


def _deal_details(deal_id, jwt):
    """Live deal + contact + counterparty counts for the reset page. Returns dict or None."""
    r = call_pipeline_api("GET", f"/deals/{deal_id}.json", jwt=jwt)
    if r["status"] != 200 or not isinstance(r["data"], dict):
        return None
    d = r["data"]
    cf = d.get("custom_fields", {}) or {}
    sell = is_sell(cf)
    stage = d.get("deal_stage") or {}
    stage_id = stage.get("id") or d.get("deal_stage_id")
    stage_name = stage.get("name") or str(stage_id)
    company = (d.get("company") or {}).get("name") or d.get("company_name") or ""

    refresh_raw = parse_cf_value(cf, REFRESH_FIELD)
    stage_default = STAGE_DEFAULTS.get(stage_id, (None, None))[1]
    refresh_days = None
    if refresh_raw is not None and refresh_raw != "":
        try:
            refresh_days = int(float(str(refresh_raw)))
        except (ValueError, TypeError):
            refresh_days = None
    effective = refresh_days if refresh_days is not None else stage_default

    contact = d.get("primary_contact") or {}
    contact_id = contact.get("id") or d.get("primary_contact_id")
    contact_name = contact.get("full_name") or ""
    contact_email, contact_company = "", ""
    if contact_id:
        pr = call_pipeline_api("GET", f"/people/{contact_id}.json", jwt=jwt)
        if pr["status"] == 200 and isinstance(pr["data"], dict):
            contact_email = (pr["data"].get("email") or "").strip()
            contact_name = contact_name or pr["data"].get("full_name") or ""
            contact_company = pr["data"].get("company_name") or ""

    # Counterparties: opposite-side active deals for the same company (snapshot)
    opp_firm, opp_other, opp_prices = 0, 0, []
    try:
        all_deals = load_json_from_s3(CACHE_BUCKET, "deals.json").get("deals", [])
        cid = (d.get("company") or {}).get("id") or d.get("company_id")
        for od in all_deals:
            if str(od.get("id")) == str(deal_id):
                continue
            ocid = (od.get("company") or {}).get("id") or od.get("company_id")
            if not ((cid and ocid == cid) or (company and (od.get("company_name") or "").lower() == company.lower())):
                continue
            ocf = od.get("custom_fields", {}) or {}
            if is_sell(ocf) == sell:
                continue
            osid = (od.get("deal_stage") or {}).get("id")
            if osid == 111800:
                opp_firm += 1
                p = parse_cf_value(ocf, NET_FIELD if not sell else GROSS_FIELD)
                try:
                    if p:
                        opp_prices.append(float(p))
                except (ValueError, TypeError):
                    pass
            else:
                opp_other += 1
    except Exception as e:
        logger.error(f"Counterparty count failed: {e}")

    return {
        "name": d.get("name") or str(deal_id),
        "company": company,
        "side": "Sell" if sell else "Buy",
        "stage_id": stage_id,
        "stage": stage_name,
        "price": fmt_price(parse_cf_value(cf, NET_FIELD if sell else GROSS_FIELD)),
        "price_label": "Net price" if sell else "Gross price",
        "size": fmt_size(parse_cf_value(cf, MIN_SIZE_FIELD), parse_cf_value(cf, MAX_SIZE_FIELD)),
        "created": _fmt_date(d.get("created_at")),
        "updated": _fmt_date(d.get("updated_at")),
        "refresh_days": refresh_days,
        "stage_default": stage_default,
        "effective": effective,
        "contact_name": contact_name,
        "contact_email": contact_email,
        "contact_company": contact_company,
        "opp_firm": opp_firm,
        "opp_other": opp_other,
        "opp_range": (f"${min(opp_prices):,.2f}" if len(set(opp_prices)) == 1 else
                      f"${min(opp_prices):,.2f}–${max(opp_prices):,.2f}") if opp_prices else "",
        "summary": (d.get("summary") or "").strip(),
    }


def handle_web(event):
    method = ((event.get("requestContext") or {}).get("http") or {}).get("method", "GET").upper()
    params = dict(event.get("queryStringParameters") or {})
    if method == "POST":
        body = event.get("body") or ""
        if event.get("isBase64Encoded"):
            body = base64.b64decode(body).decode("utf-8", "replace")
        for k, v in urllib.parse.parse_qs(body).items():
            params[k] = v[0]

    if params.get("action") in ("review", "review_apply", "review_undo"):
        return handle_review(method, params)

    if params.get("action") != "reset":
        return _page("Not found", "<p>Nothing here.</p>")

    deal_id = str(params.get("deal_id", "")).strip()
    token   = str(params.get("token", "")).strip()
    if not deal_id.isdigit() or not hmac.compare_digest(token, make_reset_token(deal_id)):
        return _page("Invalid link", "<p>This reset link is invalid.</p>")

    state = load_state()
    rec = state["deals"].get(deal_id, {})
    if rec.get("obsoleted_at"):
        return _page("Already obsolete",
                     f"<p>Deal {deal_id} was already moved to Obsolete. Revive it in Pipeline instead — editing it there restarts its clock.</p>")

    jwt = get_jwt()
    info = _deal_details(deal_id, jwt)
    if not info:
        return _page("Error", f"<p>Could not load deal {deal_id} from Pipeline. Nothing changed — try again in a minute.</p>")
    e = html.escape

    if method != "POST":
        # GET only shows details + a button — link scanners that pre-open URLs can't trigger a reset.
        cur = info["effective"]
        cur_lbl = (f"{info['refresh_days']} days (Refresh field)" if info["refresh_days"] is not None
                   else (f"{info['stage_default']} days ({e(info['stage'])} default)" if info["stage_default"] else "not scanned at this stage"))
        opp_side = "buyers" if info["side"] == "Sell" else "sellers"
        opp = f"{info['opp_firm']} Firm {opp_side}"
        if info["opp_range"]:
            opp += f" ({info['opp_range']})"
        if info["opp_other"]:
            opp += f", {info['opp_other']} other active"
        contact = e(info["contact_name"] or "—")
        if info["contact_email"]:
            contact += f" &lt;{e(info['contact_email'])}&gt;"
        if info["contact_company"]:
            contact += f"<br><span style=\"color:#666;\">{e(info['contact_company'])}</span>"

        rows = [
            ("Company", e(info["company"] or "—")),
            ("Side", e(info["side"])),
            ("Stage", e(info["stage"])),
            (e(info["price_label"]), e(info["price"])),
            ("Size", e(info["size"])),
            ("Client", contact),
            ("Counterparties", opp),
            ("Created", e(info["created"])),
            ("Last edited", e(info["updated"])),
            ("Current refresh", cur_lbl),
        ]
        if info["summary"]:
            rows.append(("Summary", e(info["summary"][:400])))
        table = "".join(
            f"<tr><td style=\"padding:6px 14px 6px 0;color:#666;vertical-align:top;white-space:nowrap;\">{k}</td>"
            f"<td style=\"padding:6px 0;\">{v}</td></tr>" for k, v in rows)

        opts = []
        if cur:
            opts.append((str(cur), f"Same as now — {cur} days", True))
        for n in (30, 60, 90, 180):
            if n != cur:
                opts.append((str(n), f"{n} days", not cur and n == 60))
        opts.append(("0", "Never go stale (Refresh 0)", False))
        radios = "".join(
            f"<label style=\"display:block;margin:6px 0;\"><input type=\"radio\" name=\"days\" value=\"{v}\"{' checked' if c else ''}> {e(l)}</label>"
            for v, l, c in opts)

        return _page("Reset deal clock", f"""
<h2 style="font-size:20px;margin-bottom:4px;">{e(info['name'])}</h2>
<p style="margin-top:0;color:#666;">Deal {deal_id} ·
  <a href="https://trades.graciagroup.com/deal/{deal_id}" target="_blank">Deal page</a> ·
  <a href="https://app.pipelinecrm.com/deals/{deal_id}" target="_blank">Pipeline</a></p>
<table style="border-collapse:collapse;font-size:14px;margin:12px 0 20px;">{table}</table>
<form method="POST" action="">
  <input type="hidden" name="action" value="reset">
  <input type="hidden" name="deal_id" value="{deal_id}">
  <input type="hidden" name="token" value="{e(token)}">
  <p style="margin-bottom:4px;"><b>Restart the clock with:</b></p>
  {radios}
  <p style="font-size:13px;color:#666;">Choosing a different number also updates the Refresh field in Pipeline.</p>
  <button type="submit" style="background:#1a1a1a;color:#fff;border:0;padding:10px 20px;border-radius:4px;font-size:14px;cursor:pointer;">Reset clock</button>
</form>""")

    # POST — perform the reset
    days_raw = str(params.get("days", "")).strip()
    if not days_raw.isdigit() or int(days_raw) > 730:
        return _page("Error", "<p>Pick a refresh period. Nothing changed.</p>")
    days = int(days_raw)

    if days != info["effective"] or (days == info["effective"] and info["refresh_days"] is None and days != info["stage_default"]):
        r = call_pipeline_api("PUT", f"/deals/{deal_id}.json",
                              {"deal": {"custom_fields": {REFRESH_FIELD: days}}}, jwt=jwt)
        if r["status"] != 200:
            return _page("Error", f"<p>Pipeline rejected the Refresh update ({r['status']}). Nothing changed.</p>")

    if days == 0:
        state["deals"].pop(deal_id, None)
    else:
        rec["reset_at"] = datetime.now(timezone.utc).isoformat()
        rec.pop("noticed_at", None)
        rec.pop("updated_at_at_notice", None)
        state["deals"][deal_id] = rec
    if not save_state(state):
        return _page("Error", "<p>Could not save the reset. Please try again.</p>")

    if days == 0:
        msg = "set to never go stale (Refresh 0)"
    else:
        due = datetime.now(timezone.utc) + timedelta(days=days)
        msg = f"clock restarted — next due {fmt_day(due)} ({days} days)"
    return _page("Done", f"<p><b>{e(info['name'])}</b>: {msg}. Any pending removal is cancelled.</p>")


# ── Obsolete review (admin page on the Function URL) ─────────────────────────
# action=review (GET, read-only) · action=review_apply (POST) · action=review_undo (POST)
# All three require key == ADMIN_KEY. Nothing is written to Pipeline without an explicit tick + Apply.

CONFIRM_STAGE_ID     = 2388323
HOLD_STAGE_ID        = 2094373
LAST_CONFIRMED_FIELD = "custom_label_4012491"   # date, written as YYYY-MM-DD
STRUCTURE_FIELD      = "custom_label_3064360"
STRUCTURE_LABELS     = {6250090: "Direct", 5077906: "Fund", 5077903: "Forward", 6361933: "Unknown", 5077909: "None"}
ACTIVE_STAGE_IDS     = {2109142, 111800, 2381534, 2388323, 2094373}  # Inquiry, Firm, Matched, Confirm, Hold
STAGE_NAMES          = {OBSOLETE_STAGE_ID: "Obsolete", INQUIRY_STAGE_ID: "Inquiry", CONFIRM_STAGE_ID: "Confirm",
                        HOLD_STAGE_ID: "Hold", 111800: "Firm", 2381534: "Matched"}
REVIEW_CACHE_KEY     = "obsolete-review/deals.json"
REVIEW_LOG_KEY       = "obsolete-review/log.json"
REVIEW_CACHE_SECONDS = 15 * 60
REVIEW_BATCH         = 50
REVIEW_DEFAULT_AFTER = "2026-03-30"


def _stage_of(d):
    return (d.get("deal_stage") or {}).get("id") or d.get("deal_stage_id")


def _company_of(d):
    return (d.get("company") or {}).get("name") or d.get("company_name") or ""


def _contact_of(d):
    pc = d.get("primary_contact") or {}
    cid = pc.get("id") or d.get("primary_contact_id")
    name = pc.get("full_name") or " ".join(x for x in (pc.get("first_name"), pc.get("last_name")) if x)
    return cid, name or "", (pc.get("email") or "").strip()


def _is_anthropic_sell(d):
    cf = d.get("custom_fields") or {}
    return is_sell(cf) and (_company_of(d).strip().lower().startswith("anthropic")
                            or (d.get("name") or "").strip().lower().startswith("anthropic"))


def _iso_day(s):
    dt = parse_pipeline_dt(s or "")
    return dt.strftime("%Y-%m-%d") if dt else ""


def _dup_key(d):
    comp = (d.get("company") or {}).get("id") or d.get("company_id") or _company_of(d).strip().lower()
    cid = _contact_of(d)[0]
    if not comp or not cid:
        return None
    return (str(comp), is_sell(d.get("custom_fields") or {}), str(cid))


def _review_row(d):
    cf = d.get("custom_fields") or {}
    sell = is_sell(cf)
    st = cf.get(STRUCTURE_FIELD)
    st_ids = st if isinstance(st, list) else ([st] if st else [])
    cid, cname, cemail = _contact_of(d)
    return {
        "id": d.get("id"),
        "name": d.get("name") or str(d.get("id")),
        "side": "Sell" if sell else "Buy",
        "structure": ", ".join(STRUCTURE_LABELS.get(x, str(x)) for x in st_ids),
        "size": fmt_size(parse_cf_value(cf, MIN_SIZE_FIELD), parse_cf_value(cf, MAX_SIZE_FIELD)),
        "price": fmt_price(parse_cf_value(cf, NET_FIELD if sell else GROSS_FIELD)),
        "contact": cname,
        "email": cemail,
        "created": _iso_day(d.get("created_at")),
        "updated": _iso_day(d.get("updated_at")),
        "updated_raw": d.get("updated_at") or "",
        "anthropic_sell": _is_anthropic_sell(d),
        "dup_key": list(_dup_key(d) or []),
    }


def _retryable(status):
    return status == 429 or status >= 500


def _get_with_retry(endpoint, jwt):
    r = call_pipeline_api("GET", endpoint, jwt=jwt)
    for wait in (2, 4, 8):
        if not _retryable(r["status"]):
            break
        time.sleep(wait)
        r = call_pipeline_api("GET", endpoint, jwt=jwt)
    return r


def _put_with_retry(endpoint, payload, jwt):
    """One retry after a back-off on 429; any other failure is returned as-is."""
    r = call_pipeline_api("PUT", endpoint, payload, jwt=jwt)
    if r["status"] == 429:
        time.sleep(5)
        r = call_pipeline_api("PUT", endpoint, payload, jwt=jwt)
    return r


def fetch_review_rows(jwt):
    """Every Obsolete deal, live. NOTE: conditions[deal_stage]=ID with NO [] — the [] form is
    silently ignored by Pipeline and returns unfiltered deals. Stage is re-checked per row anyway."""
    out, page, total = [], 1, None
    while page <= 200:
        if page > 1:
            time.sleep(0.25)
        res = _get_with_retry(f"/deals.json?conditions[deal_stage]={OBSOLETE_STAGE_ID}&per_page=200&page={page}", jwt)
        if res["status"] != 200 or not isinstance(res["data"], dict):
            raise RuntimeError(f"Pipeline list failed on page {page}: {res['status']} {str(res['data'])[:200]}")
        entries = res["data"].get("entries") or []
        total = (res["data"].get("pagination") or {}).get("total", total)
        out.extend(entries)
        if not entries or (total is not None and len(out) >= total):
            break
        page += 1
    rows = [_review_row(d) for d in out if _stage_of(d) == OBSOLETE_STAGE_ID]
    rows.sort(key=lambda r: (r["updated_raw"] or "").replace("-", "/")[:19], reverse=True)
    return rows


def load_review_rows(refresh=False):
    """(rows, fetched_at_iso). Uses the 15-minute S3 cache unless refresh."""
    now = datetime.now(timezone.utc)
    if not refresh:
        try:
            c = load_json_from_s3(CACHE_BUCKET, REVIEW_CACHE_KEY)
            fetched = datetime.fromisoformat(c["fetched_at"])
            if (now - fetched).total_seconds() < REVIEW_CACHE_SECONDS and isinstance(c.get("rows"), list):
                return c["rows"], c["fetched_at"]
        except Exception:
            pass
    rows = fetch_review_rows(get_jwt())
    fetched_at = now.isoformat()
    try:
        boto3.client('s3').put_object(Bucket=CACHE_BUCKET, Key=REVIEW_CACHE_KEY,
                                      Body=json.dumps({"fetched_at": fetched_at, "rows": rows}).encode(),
                                      ContentType="application/json")
    except Exception as e:
        logger.error(f"review: cache save failed: {e}")
    return rows, fetched_at


def load_review_log():
    """(entries, ok). A missing log is an empty log; an unreadable one is not ok."""
    try:
        data = load_json_from_s3(CACHE_BUCKET, REVIEW_LOG_KEY)
    except Exception as e:
        code = ""
        resp = getattr(e, "response", None)
        if isinstance(resp, dict):
            code = str((resp.get("Error") or {}).get("Code", ""))
        if code in ("NoSuchKey", "404") or "NoSuchKey" in str(e):
            return [], True
        logger.error(f"review: log read failed: {e}")
        return [], False
    entries = data.get("entries") if isinstance(data, dict) else None
    if not isinstance(entries, list):
        logger.error("review: log has unexpected shape")
        return [], False
    return entries, True


def save_review_log(entries):
    try:
        boto3.client('s3').put_object(Bucket=CACHE_BUCKET, Key=REVIEW_LOG_KEY,
                                      Body=json.dumps({"entries": entries}).encode(),
                                      ContentType="application/json")
        return True
    except Exception as e:
        logger.error(f"review: log save failed: {e}")
        return False


def _latest_by_deal(entries):
    latest = {}
    for en in entries:
        latest[str(en.get("deal_id"))] = en
    return latest


def _id_list(raw):
    out, seen = [], set()
    for x in str(raw or "").replace(" ", "").split(","):
        if x.isdigit() and x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _wide_page(title, inner):
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store"},
        "body": f"""<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{html.escape(title)}</title></head>
<body style="font-family:Arial,sans-serif;font-size:14px;max-width:900px;margin:30px auto;padding:0 16px;color:#222;line-height:1.5;">
{inner}</body></html>""",
    }


def _undo_form(did, key):
    e = html.escape
    return (f'<form style="display:inline;" method="POST" action="" '
            f'onsubmit="return confirm(\'Move deal {e(str(did))} back to Obsolete?\')">'
            f'<input type="hidden" name="action" value="review_undo">'
            f'<input type="hidden" name="key" value="{e(key)}">'
            f'<input type="hidden" name="deal_id" value="{e(str(did))}">'
            f'<button style="background:none;border:0;color:#06c;text-decoration:underline;padding:0;cursor:pointer;font-size:12px;">Undo</button></form>')


def _deal_link(did, name):
    return f'<a href="https://app.pipelinecrm.com/deals/{html.escape(str(did))}" target="_blank">{html.escape(name)}</a>'


def handle_review(method, params):
    key = str(params.get("key", "")).strip()
    if not ADMIN_KEY or not hmac.compare_digest(key.encode(), ADMIN_KEY.encode()):
        return _page("Not found", "<p>Nothing here.</p>")
    action = params.get("action")
    if action == "review":
        return review_page(params)
    if method != "POST":
        return _page("Not found", "<p>Nothing here.</p>")
    if action == "review_apply":
        return review_apply(params)
    return review_undo(params)


REVIEW_TEMPLATE = r"""<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Obsolete review</title>
<style>
body{font-family:Arial,sans-serif;font-size:13px;color:#222;margin:20px;}
table{border-collapse:collapse;width:100%;}
th,td{padding:5px 8px;border-bottom:1px solid #eee;text-align:left;vertical-align:top;}
th{background:#f6f6f6;position:sticky;top:0;}
.chip{display:inline-block;font-size:11px;padding:1px 7px;border-radius:9px;margin:1px 2px;background:#fde8e8;color:#a00;white-space:nowrap;}
.chip.hold{background:#fff4e5;color:#a60;}
.chip.muted{background:#eee;color:#777;}
.bar{display:flex;flex-wrap:wrap;gap:10px 16px;align-items:center;margin:12px 0;}
.muted{color:#888;}
#apply{background:#1a1a1a;color:#fff;border:0;padding:8px 16px;border-radius:4px;cursor:pointer;}
.undo{background:none;border:0;color:#06c;text-decoration:underline;padding:0;font-size:11px;cursor:pointer;}
</style></head><body>
<h2 style="margin:0 0 4px;">Obsolete deals — review</h2>
<div class="muted">Every deal stays Obsolete unless you tick it and click Apply. List fetched __FETCHED__ · <a href="__REFRESH__">Refresh list</a></div>
<form id="f" method="POST" action="" onsubmit="return go()">
<input type="hidden" name="action" value="review_apply">
<input type="hidden" name="key" value="__KEY__">
<input type="hidden" name="ids" id="ids"><input type="hidden" name="kept" id="kept">
<div class="bar">
 <b><span id="shown">0</span> shown / __TOTAL__</b>
 <label>Restore ticked to <select name="stage" id="stage"><option value="inquiry" selected>Inquiry</option><option value="confirm">Confirm</option></select></label>
 <label>Side <select id="side"><option>All</option><option>Buy</option><option>Sell</option></select></label>
 <label>Last updated after <input type="date" id="after" value="__AFTER__"></label>
 <input type="search" id="q" placeholder="Search name or client">
 <label><input type="checkbox" id="hideRev" checked> Hide deals already reviewed</label>
 <button type="button" onclick="selAll()">Select all visible</button>
 <button type="button" onclick="clr()">Clear</button>
 <button type="submit" id="apply">Apply (0)</button>
</div></form>
<table><thead><tr><th></th><th>Deal</th><th>Side</th><th>Structure</th><th>Size</th><th>Price</th><th>Client</th><th>Created</th><th>Last updated</th></tr></thead>
<tbody id="tb"></tbody></table>
<script>
const ROWS = __DATA__;
const KEY = __KEYJS__;
const ticked = new Set();
function esc(s){return String(s==null?"":s).replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));}
function restored(r){return r.reviewed && r.reviewed.action==="restored";}
function reviewed(r){return r.reviewed && (r.reviewed.action==="restored" || r.reviewed.action==="kept");}
function vis(){
  const side=document.getElementById("side").value, after=document.getElementById("after").value,
        q=document.getElementById("q").value.trim().toLowerCase(), hide=document.getElementById("hideRev").checked;
  return ROWS.filter(r=>(side==="All"||r.side===side) && (!after || r.updated>after) &&
    (!q || (r.name+" "+r.contact+" "+r.email).toLowerCase().includes(q)) && !(hide && reviewed(r)));
}
function undoForm(id){
  return '<form style="display:inline" method="POST" action="" onsubmit="return confirm(\'Move deal '+id+' back to Obsolete?\')">'+
    '<input type="hidden" name="action" value="review_undo"><input type="hidden" name="key" value="'+esc(KEY)+'">'+
    '<input type="hidden" name="deal_id" value="'+id+'"><button class="undo">Undo</button></form>';
}
function render(){
  const v=vis(); document.getElementById("shown").textContent=v.length;
  document.getElementById("tb").innerHTML=v.map(r=>{
    let chips="";
    if(r.dup) chips+='<span class="chip">Live duplicate</span>';
    if(r.anthropic_sell) chips+='<span class="chip hold">Anthropic sell → will go to Hold</span>';
    if(r.reviewed){
      const a=r.reviewed.action, d=esc((r.reviewed.at||"").slice(0,10));
      if(a==="restored") chips+='<span class="chip muted">Restored '+d+'</span> '+undoForm(r.id);
      else if(a==="kept") chips+='<span class="chip muted">Kept '+d+'</span>';
      else if(a==="undone") chips+='<span class="chip muted">Undone '+d+'</span>';
    }
    const client=esc(r.contact||"—")+(r.email?'<br><span class="muted">'+esc(r.email)+'</span>':"");
    return '<tr><td><input type="checkbox" data-id="'+r.id+'"'+(ticked.has(r.id)?" checked":"")+(restored(r)?" disabled":"")+' onchange="tick(this)"></td>'+
      '<td><a href="https://app.pipelinecrm.com/deals/'+r.id+'" target="_blank">'+esc(r.name)+'</a> '+chips+'</td>'+
      '<td>'+esc(r.side)+'</td><td>'+esc(r.structure)+'</td><td>'+esc(r.size)+'</td><td>'+esc(r.price)+'</td>'+
      '<td>'+client+'</td><td>'+esc(r.created)+'</td><td>'+esc(r.updated)+'</td></tr>';
  }).join("");
  upd();
}
function tick(el){const id=Number(el.dataset.id); if(el.checked) ticked.add(id); else ticked.delete(id); upd();}
function picked(){return vis().filter(r=>ticked.has(r.id) && !restored(r));}
function upd(){document.getElementById("apply").textContent="Apply ("+picked().length+")";}
function selAll(){vis().forEach(r=>{if(!restored(r)) ticked.add(r.id);}); render();}
function clr(){ticked.clear(); render();}
function go(){
  const v=vis(), t=picked(), k=v.filter(r=>!ticked.has(r.id) && !restored(r));
  const st=document.getElementById("stage").selectedOptions[0].text;
  const msg="Restore "+t.length+" deal(s) to "+st+" (Anthropic sells go to Hold)?\n"+k.length+" other visible deal(s) will be recorded as kept Obsolete."+
    (t.length>50?"\n\n"+t.length+" ticked: the first 50 are applied now; you'll get a Continue button for the rest.":"");
  if(!confirm(msg)) return false;
  document.getElementById("ids").value=t.map(r=>r.id).join(",");
  document.getElementById("kept").value=k.map(r=>r.id).join(",");
  return true;
}
["side","after","q","hideRev"].forEach(id=>document.getElementById(id).addEventListener("input",render));
document.getElementById("hideRev").addEventListener("change",render);
render();
</script></body></html>"""


def review_page(params):
    """GET: read-only. Never writes to Pipeline (only the S3 list cache)."""
    key = str(params.get("key", "")).strip()
    refresh = str(params.get("refresh", "")) == "1"
    try:
        rows, fetched_at = load_review_rows(refresh=refresh)
    except Exception as ex:
        logger.error(f"review: list failed: {ex}")
        return _page("Error", f"<p>Could not load Obsolete deals from Pipeline: {html.escape(str(ex)[:300])}</p>")

    dup_keys = set()
    try:
        for d in load_json_from_s3(CACHE_BUCKET, "deals.json").get("deals", []):
            if _stage_of(d) in ACTIVE_STAGE_IDS:
                k = _dup_key(d)
                if k:
                    dup_keys.add(k)
    except Exception as ex:
        logger.error(f"review: snapshot load failed: {ex}")

    entries, _ok = load_review_log()
    latest = _latest_by_deal(entries)
    out = []
    for r in rows:
        r = dict(r)
        r["dup"] = bool(r.get("dup_key")) and tuple(r["dup_key"]) in dup_keys
        en = latest.get(str(r["id"]))
        r["reviewed"] = {"action": en.get("action"), "at": en.get("at", "")} if en else None
        r.pop("dup_key", None)
        r.pop("updated_raw", None)
        out.append(r)

    base = SCANNER_URL.rstrip('/') + "/" if SCANNER_URL else ""
    refresh_url = f"{base}?action=review&key={urllib.parse.quote(key)}&refresh=1"
    try:
        fetched_lbl = datetime.fromisoformat(fetched_at).strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        fetched_lbl = fetched_at
    page = (REVIEW_TEMPLATE
            .replace("__DATA__", json.dumps(out).replace("</", "<\\/"))
            .replace("__KEYJS__", json.dumps(key).replace("</", "<\\/"))
            .replace("__KEY__", html.escape(key))
            .replace("__FETCHED__", html.escape(fetched_lbl))
            .replace("__REFRESH__", html.escape(refresh_url))
            .replace("__TOTAL__", str(len(out)))
            .replace("__AFTER__", REVIEW_DEFAULT_AFTER))
    return {"statusCode": 200, "headers": {"Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store"},
            "body": page}


def review_apply(params):
    key = str(params.get("key", "")).strip()
    chosen = CONFIRM_STAGE_ID if params.get("stage") == "confirm" else INQUIRY_STAGE_ID
    stage_param = "confirm" if chosen == CONFIRM_STAGE_ID else "inquiry"
    ids = _id_list(params.get("ids"))
    kept_ids = [k for k in _id_list(params.get("kept")) if k not in set(ids)]
    batch, rest = ids[:REVIEW_BATCH], ids[REVIEW_BATCH:]
    now = datetime.now(timezone.utc)
    e = html.escape

    entries, ok = load_review_log()
    if not ok:
        return _wide_page("Error", "<p><b>Could not read the review log.</b> Nothing was changed in Pipeline. Try again.</p>")

    cached = {}
    if kept_ids:
        try:
            cached = {str(r.get("id")): r for r in load_json_from_s3(CACHE_BUCKET, REVIEW_CACHE_KEY).get("rows", [])}
        except Exception:
            cached = {}
    for k in kept_ids:
        r = cached.get(k, {})
        entries.append({"deal_id": int(k), "name": r.get("name", ""), "action": "kept",
                        "previous_stage": OBSOLETE_STAGE_ID, "previous_updated_at": r.get("updated_raw", ""),
                        "at": now.isoformat()})
    # Log must be writable before anything is written to Pipeline.
    if not save_review_log(entries):
        return _wide_page("Error", "<p><b>Could not save the review log.</b> Nothing was changed in Pipeline. "
                                   "The scanner role needs s3:PutObject on full-pipeline-cache/obsolete-review/*.</p>")

    jwt = get_jwt() if batch else None
    restored, skipped, failed = [], [], []
    for i, did in enumerate(batch):
        if i:
            time.sleep(1)
        r = _get_with_retry(f"/deals/{did}.json", jwt)
        if r["status"] != 200 or not isinstance(r["data"], dict):
            failed.append((did, did, f"read failed ({r['status']})"))
            continue
        d = r["data"]
        name = d.get("name") or did
        prev_stage = _stage_of(d)
        if prev_stage != OBSOLETE_STAGE_ID:
            skipped.append((did, name, f"no longer Obsolete (now {STAGE_NAMES.get(prev_stage, prev_stage)})"))
            continue
        target = HOLD_STAGE_ID if _is_anthropic_sell(d) else chosen
        prev_updated = d.get("updated_at") or ""
        payload = {"deal": {"deal_stage_id": target}}
        day = _iso_day(prev_updated)
        if day:
            payload["deal"]["custom_fields"] = {LAST_CONFIRMED_FIELD: day}

        entry = {"deal_id": int(did), "name": name, "action": "restored", "new_stage": STAGE_NAMES[target],
                 "new_stage_id": target, "previous_stage": prev_stage, "previous_updated_at": prev_updated,
                 "at": datetime.now(timezone.utc).isoformat()}
        entries.append(entry)
        if not save_review_log(entries):
            entries.pop()
            rest = batch[i:] + rest
            failed.append((did, name, "log save failed — stopped this batch before writing"))
            break
        pr = _put_with_retry(f"/deals/{did}.json", payload, jwt)
        if pr["status"] != 200:
            entry["action"] = "restore_failed"
            entry["error"] = str(pr["status"])
            save_review_log(entries)
            failed.append((did, name, f"Pipeline rejected ({pr['status']})"))
            continue
        restored.append((did, name, STAGE_NAMES[target]))

    # One summary email to Chad per batch.
    lines = [f"Obsolete review — {now.strftime('%Y-%m-%d %H:%M UTC')}",
             f"Restored {len(restored)}, kept {len(kept_ids)}, skipped {len(skipped)}, failed {len(failed)}, "
             f"still to do {len(rest)}", ""]
    lines += [f"Restored ({len(restored)}):"] + [f"  • {n} → {s} (id={d})" for d, n, s in restored] + [""]
    lines += [f"Skipped ({len(skipped)}):"] + [f"  • {n} — {why} (id={d})" for d, n, why in skipped] + [""]
    lines += [f"Failed ({len(failed)}):"] + [f"  • {n} — {why} (id={d})" for d, n, why in failed] + [""]
    lines += [f"Kept Obsolete ({len(kept_ids)}):"] + [f"  • {cached.get(k, {}).get('name') or k} (id={k})" for k in kept_ids]
    try:
        send_email(CHAD_EMAIL,
                   f"Obsolete review — restored {len(restored)}, kept {len(kept_ids)}, skipped {len(skipped)}, failed {len(failed)}",
                   "\n".join(lines))
    except Exception as ex:
        logger.error(f"review: summary email failed: {ex}")

    def ul(items):
        return "<ul>" + "".join(f"<li>{x}</li>" for x in items) + "</ul>" if items else "<p class=\"muted\" style=\"color:#888;\">(none)</p>"

    parts = [f"<h2 style=\"margin-bottom:4px;\">Obsolete review — applied</h2>",
             f"<p>Restored {len(restored)} · kept {len(kept_ids)} · skipped {len(skipped)} · failed {len(failed)}</p>",
             f"<h3>Restored ({len(restored)})</h3>",
             ul([f"{_deal_link(d, n)} → {e(s)} {_undo_form(d, key)}" for d, n, s in restored]),
             f"<h3>Skipped ({len(skipped)})</h3>",
             ul([f"{_deal_link(d, n)} — {e(why)}" for d, n, why in skipped]),
             f"<h3>Failed ({len(failed)})</h3>",
             ul([f"{_deal_link(d, n)} — {e(why)}" for d, n, why in failed])]
    if rest:
        nxt = min(len(rest), REVIEW_BATCH)
        parts.append(f"""<h3>Still to do ({len(rest)})</h3>
<form method="POST" action="" onsubmit="return confirm('Restore the next {nxt} deal(s)?')">
  <input type="hidden" name="action" value="review_apply">
  <input type="hidden" name="key" value="{e(key)}">
  <input type="hidden" name="stage" value="{stage_param}">
  <input type="hidden" name="ids" value="{e(','.join(rest))}">
  <input type="hidden" name="kept" value="">
  <button type="submit" style="background:#1a1a1a;color:#fff;border:0;padding:10px 20px;border-radius:4px;cursor:pointer;">Continue — next {nxt}</button>
</form>""")
        parts.append(ul([_deal_link(d, cached.get(d, {}).get("name") or f"Deal {d}") for d in rest]))
    base = SCANNER_URL.rstrip('/') + "/" if SCANNER_URL else ""
    parts.append(f'<p><a href="{e(base)}?action=review&amp;key={e(urllib.parse.quote(key))}">Back to the review list</a></p>')
    return _wide_page("Obsolete review — applied", "".join(parts))


def review_undo(params):
    key = str(params.get("key", "")).strip()
    did = str(params.get("deal_id", "")).strip()
    e = html.escape
    base = SCANNER_URL.rstrip('/') + "/" if SCANNER_URL else ""
    back = f'<p><a href="{e(base)}?action=review&amp;key={e(urllib.parse.quote(key))}">Back to the review list</a></p>'
    if not did.isdigit():
        return _wide_page("Error", "<p>Missing deal id. Nothing changed.</p>" + back)
    entries, ok = load_review_log()
    if not ok:
        return _wide_page("Error", "<p>Could not read the review log. Nothing changed.</p>" + back)
    en = _latest_by_deal(entries).get(did)
    if not en or en.get("action") != "restored":
        return _wide_page("Error", f"<p>The review log does not show deal {e(did)} as restored. Nothing changed.</p>" + back)

    jwt = get_jwt()
    r = _get_with_retry(f"/deals/{did}.json", jwt)
    if r["status"] != 200 or not isinstance(r["data"], dict):
        return _wide_page("Error", f"<p>Could not load deal {e(did)} from Pipeline ({r['status']}). Nothing changed.</p>" + back)
    d = r["data"]
    name = d.get("name") or did
    cur = _stage_of(d)
    if cur == OBSOLETE_STAGE_ID:
        return _wide_page("Already Obsolete", f"<p>{_deal_link(did, name)} is already Obsolete. Nothing changed.</p>" + back)

    entry = {"deal_id": int(did), "name": name, "action": "undone", "previous_stage": cur,
             "previous_updated_at": d.get("updated_at") or "", "at": datetime.now(timezone.utc).isoformat()}
    entries.append(entry)
    if not save_review_log(entries):
        return _wide_page("Error", "<p>Could not save the review log. Nothing was changed in Pipeline.</p>" + back)
    pr = _put_with_retry(f"/deals/{did}.json", {"deal": {"deal_stage_id": OBSOLETE_STAGE_ID}}, jwt)
    if pr["status"] != 200:
        entry["action"] = "undo_failed"
        entry["error"] = str(pr["status"])
        save_review_log(entries)
        return _wide_page("Error", f"<p>Pipeline rejected the change ({pr['status']}). {_deal_link(did, name)} was not changed.</p>" + back)
    return _wide_page("Undone", f"<p>{_deal_link(did, name)} moved back to Obsolete "
                                f"(was {e(str(STAGE_NAMES.get(cur, cur)))}). Nothing else changed.</p>" + back)


# ── One-time restore ──────────────────────────────────────────────────────────

def fetch_obsolete_deals(jwt):
    out, page, total = [], 1, None
    while page <= 100:
        q = urllib.parse.urlencode([("conditions[deal_stage][]", OBSOLETE_STAGE_ID),
                                    ("page", page), ("per_page", 200)])
        res = call_pipeline_api("GET", f"/deals.json?{q}", jwt=jwt)
        if res["status"] != 200 or not isinstance(res["data"], dict):
            raise RuntimeError(f"Pipeline list failed on page {page}: {res['status']} {str(res['data'])[:200]}")
        entries = res["data"].get("entries") or []
        total = (res["data"].get("pagination") or {}).get("total", total)
        out.extend(entries)
        if not entries or (total is not None and len(out) >= total):
            break
        page += 1
    return out


def run_restore(mode):
    execute = (mode == "execute")
    jwt = get_jwt()
    now = datetime.now(timezone.utc)
    lines = []
    try:
        deals = fetch_obsolete_deals(jwt)
    except Exception as e:
        send_email(CHAD_EMAIL, "Stale Scanner Restore — FAILED", f"Could not list Obsolete deals: {e}")
        return {"statusCode": 500, "error": str(e)}

    matches = []
    for d in deals:
        stage_id = (d.get("deal_stage") or {}).get("id") or d.get("deal_stage_id")
        if stage_id != OBSOLETE_STAGE_ID:
            continue
        upd = (d.get("updated_at") or "").replace("-", "/")[:10]
        if upd != RESTORE_DATE:
            continue
        if (d.get("name") or "").strip() not in RESTORE_NAMES:
            continue
        matches.append(d)

    state_ok = save_state(load_state())
    lines.append(f"Obsolete deals scanned: {len(deals)}")
    lines.append(f"Matched (Obsolete, edited {RESTORE_DATE}, name in today's report): {len(matches)} — expected {RESTORE_EXPECTED}")
    lines.append(f"State file write test: {'OK' if state_ok else 'FAILED — scanner role needs s3:PutObject on full-pipeline-cache'}")
    lines.append("")

    done, failed = 0, 0
    for d in sorted(matches, key=lambda x: x.get("name") or ""):
        did = d.get("id")
        if execute:
            r = call_pipeline_api("PUT", f"/deals/{did}.json",
                                  {"deal": {"deal_stage_id": INQUIRY_STAGE_ID,
                                            "custom_fields": {REFRESH_FIELD: 90}}}, jwt=jwt)
            if r["status"] == 200:
                done += 1
                lines.append(f"  • {d.get('name')} (id={did}) → Inquiry, Refresh 90")
            else:
                failed += 1
                lines.append(f"  • {d.get('name')} (id={did}) FAILED {r['status']}")
        else:
            lines.append(f"  • {d.get('name')} (id={did})")

    tag = "EXECUTED" if execute else "DRY RUN — nothing changed"
    if execute:
        lines.insert(0, f"Restored {done}, failed {failed}")
    send_email(CHAD_EMAIL, f"Stale Scanner Restore — {tag} — {now.strftime('%Y-%m-%d')}", "\n".join(lines))
    return {"statusCode": 200, "mode": mode, "matched": len(matches), "restored": done, "failed": failed}


# ── One-time Last Confirmed backfill (manual invocation only) ────────────────

def _is_blank(v):
    if isinstance(v, list):
        v = v[0] if v else None
    return v is None or str(v).strip() == ""


def _snapshot_date(updated_at):
    """'2024/11/26 06:19:44 -0500' -> '2024-11-26' (None if unparseable)."""
    raw = str(updated_at or "").strip()[:10].replace("/", "-")
    try:
        return datetime.strptime(raw, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        return None


def _api_with_retry(method, endpoint, payload=None, jwt=None):
    """call_pipeline_api with backoff on 429/5xx (up to BACKFILL_MAX_RETRIES retries)."""
    res = call_pipeline_api(method, endpoint, payload, jwt=jwt)
    for attempt in range(BACKFILL_MAX_RETRIES):
        if not (res["status"] == 429 or res["status"] >= 500):
            break
        time.sleep(2 ** (attempt + 1))
        res = call_pipeline_api(method, endpoint, payload, jwt=jwt)
    return res


def run_backfill_last_confirmed(mode):
    execute = (mode == "execute")
    now = datetime.now(timezone.utc)
    tag_day = now.strftime('%Y-%m-%d')
    try:
        all_deals = (load_json_from_s3(CACHE_BUCKET, "deals.json") or {}).get("deals") or []
    except Exception as e:
        logger.error(f"backfill: deals.json load failed: {e}")
        all_deals = []
    if not all_deals:
        send_email(CHAD_EMAIL, f"Last Confirmed Backfill — ABORTED — {tag_day}",
                   "deals.json snapshot was empty or missing. Nothing written.")
        return {"statusCode": 500, "error": "empty snapshot"}

    selected, unparseable, restore_dated = [], [], []
    for d in all_deals:
        stage_id = (d.get("deal_stage") or {}).get("id")
        if stage_id not in BACKFILL_STAGES:
            continue
        if not _is_blank((d.get("custom_fields") or {}).get(LAST_CONFIRMED_FIELD)):
            continue  # already has a value: never touch
        date = _snapshot_date(d.get("updated_at") or d.get("updated"))
        if not date:
            unparseable.append(d)
            continue
        if date == BACKFILL_SKIP_DATE:
            restore_dated.append((d, BACKFILL_STAGES[stage_id], date))
            continue
        selected.append((d, BACKFILL_STAGES[stage_id], date))
    selected.sort(key=lambda x: (x[1], x[0].get("name") or ""))

    def label(d, stage, date):
        return f"{d.get('name', '')} (id={d.get('id')}, {stage}) → {date}"

    tail = [f"  • {d.get('name', '')} (id={d.get('id')}) — no usable updated_at, skipped" for d in unparseable]
    restore_dated.sort(key=lambda x: (x[1], x[0].get("name") or ""))
    restore_rows = [f"  • {d.get('name', '')} (id={d.get('id')}, {stage})" for d, stage, _ in restore_dated]
    restore_head = f"Left blank — Sep 23 restore date ({len(restore_rows)}):"

    if not execute:
        lines = [f"Last Confirmed backfill — DRY RUN, nothing changed ({tag_day})",
                 f"Deals that would be written: {len(selected)}", ""]
        lines += [f"  • {label(*x)}" for x in selected]
        if restore_rows:
            lines += ["", restore_head] + restore_rows
        if tail:
            lines += ["", f"Skipped, no usable updated_at ({len(tail)}):"] + tail
        send_email(CHAD_EMAIL, f"Last Confirmed Backfill — DRY RUN — {tag_day}", "\n".join(lines))
        return {"statusCode": 200, "mode": mode, "selected": len(selected),
                "left_blank_restore": len(restore_rows)}

    jwt = get_jwt()
    written, skipped, failed, not_tried = [], [], [], []
    consecutive = 0
    stopped = False
    for i, (d, stage, date) in enumerate(selected):
        if stopped:
            not_tried.append(f"  • {label(d, stage, date)}")
            continue
        did = d.get("id")
        live = _api_with_retry("GET", f"/deals/{did}.json", jwt=jwt)
        if live["status"] != 200 or not isinstance(live["data"], dict):
            failed.append(f"  • {label(d, stage, date)} — live GET failed {live['status']}")
            continue
        live_val = (live["data"].get("custom_fields") or {}).get(LAST_CONFIRMED_FIELD)
        if not _is_blank(live_val):
            skipped.append(f"  • {label(d, stage, date)} — already set live ({live_val})")
            continue
        if written or failed:
            time.sleep(BACKFILL_PACE_SECONDS)
        r = _api_with_retry("PUT", f"/deals/{did}.json",
                            {"deal": {"custom_fields": {LAST_CONFIRMED_FIELD: date}}}, jwt=jwt)
        if r["status"] == 200:
            written.append(f"  • {label(d, stage, date)}")
            consecutive = 0
        else:
            failed.append(f"  • {label(d, stage, date)} — PUT failed {r['status']} {str(r['data'])[:120]}")
            consecutive += 1
            if consecutive >= BACKFILL_MAX_CONSECUTIVE_FAILURES:
                stopped = True

    summary = f"Written {len(written)}, skipped {len(skipped) + len(tail)}, failed {len(failed)}"
    if stopped:
        summary += f", not attempted {len(not_tried)}"
    summary += f", left blank (Sep 23 restore) {len(restore_rows)}"
    lines = [f"Last Confirmed backfill — EXECUTED ({tag_day})", summary]
    if stopped:
        lines.append(f"STOPPED after {BACKFILL_MAX_CONSECUTIVE_FAILURES} PUT failures in a row.")
    for title, rows in (("Written", written), ("Skipped — already set live", skipped),
                        ("Skipped — no usable updated_at", tail), ("Failed", failed),
                        ("Not attempted (stopped)", not_tried)):
        if rows:
            lines += ["", f"{title} ({len(rows)}):"] + rows
    if restore_rows:
        lines += ["", restore_head] + restore_rows
    send_email(CHAD_EMAIL, f"Last Confirmed Backfill — EXECUTED — {tag_day}", "\n".join(lines))
    return {"statusCode": 200, "mode": mode, "written": len(written),
            "skipped": len(skipped) + len(tail), "failed": len(failed), "stopped": stopped,
            "left_blank_restore": len(restore_rows)}


# ── Main handler ──────────────────────────────────────────────────────────────

def lambda_handler(event, context):
    event = event or {}
    if isinstance(event.get("requestContext"), dict) and "http" in event["requestContext"]:
        return handle_web(event)
    if (event.get("backfill_last_confirmed") in ("dry", "execute")
            and event.get("source") != "aws.events"):  # manual invoke only; never EventBridge
        return run_backfill_last_confirmed(event["backfill_last_confirmed"])
    if event.get("restore") in ("dry", "execute"):
        return run_restore(event["restore"])
    if event.get("diagnose"):
        return diagnose_stale_deals()
    return run_scan()


def run_scan():
    """Report-only daily scan. Never changes a deal in Pipeline and never emails a client;
    the only email it sends is Chad's report."""
    logger.info(f"Stale deal scanner started (DRY_RUN={DRY_RUN})")
    now = datetime.now(timezone.utc)

    deals_data = load_json_from_s3(CACHE_BUCKET, "deals.json")
    all_deals  = deals_data.get("deals", [])
    logger.info(f"Loaded {len(all_deals)} deals")
    if not all_deals:
        send_email(CHAD_EMAIL, f"Stale Deal Scanner — ABORTED — {now.strftime('%Y-%m-%d')}",
                   "deals.json snapshot was empty. Nothing sent, nothing changed.")
        return {"statusCode": 500, "error": "empty snapshot"}

    state = load_state()
    sd = state["deals"]

    # Fail safe: never send client emails unless state can be saved (kept for the future reminder feature).
    if not DRY_RUN and not save_state(state):
        send_email(CHAD_EMAIL, f"Stale Deal Scanner — ABORTED — {now.strftime('%Y-%m-%d')}",
                   "Could not write stale-scanner-state.json to full-pipeline-cache. "
                   "No notices sent and no deals changed. The scanner's IAM role needs s3:PutObject on that bucket.")
        return {"statusCode": 500, "error": "state not writable"}

    # Old removal notices must never resume: strip their keys from every record.
    for rec in sd.values():
        if isinstance(rec, dict):
            rec.pop("noticed_at", None)
            rec.pop("updated_at_at_notice", None)

    active_ids = set()
    due, headsup = [], []

    for deal in all_deals:
        cf       = deal.get("custom_fields", {})
        stage_id = (deal.get("deal_stage") or {}).get("id")
        if stage_id not in STAGE_DEFAULTS:
            continue
        did = str(deal.get("id"))
        if stage_id == 2094373:  # Hold — skipped completely
            sd.pop(did, None)
            continue
        active_ids.add(did)
        stage_name, stage_default_days = STAGE_DEFAULTS[stage_id]
        refresh_raw = parse_cf_value(cf, REFRESH_FIELD)
        if refresh_raw is not None and refresh_raw != "":
            try:
                refresh_days = int(float(str(refresh_raw)))
            except (ValueError, TypeError):
                refresh_days = stage_default_days
        else:
            refresh_days = stage_default_days
        if refresh_days == 0:
            sd.pop(did, None)
            continue

        updated_str = deal.get("updated_at") or deal.get("updated", "")
        updated_dt  = parse_pipeline_dt(updated_str)
        if not updated_dt:
            continue
        rec = sd.get(did, {})
        rec.pop("obsoleted_at", None)  # deal is active again (revived)

        clock_start = updated_dt
        if rec.get("reset_at"):
            try:
                clock_start = max(clock_start, datetime.fromisoformat(rec["reset_at"]))
            except Exception:
                pass
        age = (now - clock_start).days
        due_on = clock_start + timedelta(days=refresh_days)

        if age >= refresh_days:
            due.append((age, stage_name, deal))
        elif (due_on - now).days < HEADSUP_DAYS:
            headsup.append((due_on, stage_name, deal))

        if rec:
            sd[did] = rec
        else:
            sd.pop(did, None)

    # Prune records for deals no longer active (keep obsoleted markers 30 days)
    for did in list(sd.keys()):
        if did in active_ids:
            continue
        ob = sd[did].get("obsoleted_at")
        if ob and (now - datetime.fromisoformat(ob)).days < 30:
            continue
        sd.pop(did, None)

    # ── Report to Chad ──
    def link(did):
        u = reset_url(did)
        return f' — <a href="{html.escape(u)}">Reset</a>' if u else ""

    def section(title, rows):
        h = f"<h3 style=\"font-size:15px;margin:22px 0 6px;\">{html.escape(title)}</h3>"
        if not rows:
            return h + "<p style=\"color:#888;margin:0;\">(none)</p>"
        return h + "<ul style=\"margin:0;padding-left:18px;\">" + "".join(f"<li>{r}</li>" for r in rows) + "</ul>"

    dry = " [DRY RUN — state not saved]" if DRY_RUN else ""
    due.sort(key=lambda x: x[0], reverse=True)   # oldest first
    headsup.sort(key=lambda x: x[0])

    h_due  = [f"{html.escape(d.get('name',''))} — {s}, {a} days{link(d.get('id'))}" for a, s, d in due]
    h_head = [f"{html.escape(d.get('name',''))} — due {fmt_day(r)} ({s}){link(d.get('id'))}" for r, s, d in headsup]

    parts = [f"<p><b>Stale Deal Scanner — {now.strftime('%Y-%m-%d')}</b>{html.escape(dry)}</p>",
             "<p style=\"color:#888;\">Report only — the scanner no longer obsoletes deals or emails clients.</p>"]
    review_link = f"{SCANNER_URL.rstrip('/')}/?action=review&key={ADMIN_KEY}" if SCANNER_URL and ADMIN_KEY else ""
    if review_link:
        parts.insert(1, f"<p><a href=\"{html.escape(review_link)}\">Review Obsolete deals</a></p>")
    if not SCANNER_URL:
        parts.append("<p style=\"color:#b00;\">Reset links disabled: SCANNER_URL env var is blank.</p>")
    parts.append(section(f"Due for review ({len(h_due)})", h_due))
    parts.append(section(f"Heads-up — due in the next {HEADSUP_DAYS} days ({len(h_head)})", h_head))
    report_html = "<html><body style=\"font-family:Arial,sans-serif;font-size:14px;color:#222;line-height:1.5;\">" + "".join(parts) + "</body></html>"

    plain = [f"Stale Deal Scanner — {now.strftime('%Y-%m-%d')}{dry}",
             *([f"Review Obsolete deals: {review_link}"] if review_link else []),
             "Report only — the scanner no longer obsoletes deals or emails clients.", "",
             f"Due for review ({len(due)}):", *[f"  • {d.get('name','')} — {s}, {a} days" for a, s, d in due], "",
             f"Heads-up — due in the next {HEADSUP_DAYS} days ({len(headsup)}):",
             *[f"  • {d.get('name','')} — due {fmt_day(r)}" for r, s, d in headsup]]
    try:
        send_email(CHAD_EMAIL,
                   f"{'DRY RUN — ' if DRY_RUN else ''}Stale Deal Scanner Report — {now.strftime('%Y-%m-%d')}",
                   "\n".join(plain), report_html)
    except Exception as e:
        logger.error(f"Could not send summary to Chad: {e}")

    if not DRY_RUN:
        save_state(state)
    logger.info(f"Done. due={len(due)} headsup={len(headsup)}")
    return {"statusCode": 200, "dry_run": DRY_RUN, "due": len(due), "headsup": len(headsup)}
