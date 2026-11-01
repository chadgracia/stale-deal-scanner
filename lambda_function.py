"""
stale_deal_scanner.py
─────────────────────
Daily Lambda (EventBridge, 8 AM ET).

Flow per active deal (Confirm / Firm / Inquiry / Hold):
  clock start = later of Pipeline updated_at and Chad's last Reset click
  1. HEADS-UP  — due within 7 days → listed in Chad's report with a Reset link
  2. NOTICE    — due → client gets a removal notice (removal date = +3 days);
                 deal is NOT moved yet. Max 25 notices per run.
  3. OBSOLETE  — 3+ days after notice, only if the deal has not been edited
                 since the notice (client form update, Chad edit, or Reset
                 all cancel it) → moved to Obsolete, Refresh cleared.

State: s3://full-pipeline-cache/stale-scanner-state.json
  If state cannot be saved, the run stops before sending anything.

Stage defaults (days):  Confirm=1, Firm=60, Inquiry=60, Hold=90
Refresh=0 → skip forever | Refresh=N → N days | blank → stage default

Invocations:
  EventBridge (no special keys)      → daily scan
  {"diagnose": true}                 → legacy diagnostic (unchanged)
  {"restore": "dry"}                 → list the Sep 23 2026 obsoleted deals, change nothing
  {"restore": "execute"}             → move them to Inquiry, Refresh = 90
  Function URL GET/POST ?action=reset → Chad's Reset button

Env vars: FORM_LAMBDA_URL, HMAC_SECRET, DRY_RUN, TEST_EMAIL_OVERRIDE,
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
        pending = ""
        if rec.get("noticed_at"):
            try:
                rm = datetime.fromisoformat(rec["noticed_at"]) + timedelta(days=GRACE_DAYS)
                pending = f"<p style=\"background:#fff4e5;padding:8px 12px;border-radius:4px;\">Removal notice sent — scheduled for Obsolete on {fmt_day(rm)}.</p>"
            except Exception:
                pass
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
{pending}
<table style="border-collapse:collapse;font-size:14px;margin:12px 0 20px;">{table}</table>
<form method="POST" action="">
  <input type="hidden" name="action" value="reset">
  <input type="hidden" name="deal_id" value="{deal_id}">
  <input type="hidden" name="token" value="{e(token)}">
  <p style="margin-bottom:4px;"><b>Restart the clock with:</b></p>
  {radios}
  <p style="font-size:13px;color:#666;">Any pending removal notice is cancelled. Choosing a different number also updates the Refresh field in Pipeline.</p>
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


# ── Main handler ──────────────────────────────────────────────────────────────

def lambda_handler(event, context):
    event = event or {}
    if isinstance(event.get("requestContext"), dict) and "http" in event["requestContext"]:
        return handle_web(event)
    if event.get("restore") in ("dry", "execute"):
        return run_restore(event["restore"])
    if event.get("diagnose"):
        return diagnose_stale_deals()
    return run_scan()


def run_scan():
    logger.info(f"Stale deal scanner started (DRY_RUN={DRY_RUN})")
    now = datetime.now(timezone.utc)
    jwt = get_jwt()

    deals_data = load_json_from_s3(CACHE_BUCKET, "deals.json")
    all_deals  = deals_data.get("deals", [])
    logger.info(f"Loaded {len(all_deals)} deals")
    if not all_deals:
        send_email(CHAD_EMAIL, f"Stale Deal Scanner — ABORTED — {now.strftime('%Y-%m-%d')}",
                   "deals.json snapshot was empty. Nothing sent, nothing changed.")
        return {"statusCode": 500, "error": "empty snapshot"}

    state = load_state()
    sd = state["deals"]

    # Fail safe: never send notices unless we can record them.
    if not DRY_RUN and not save_state(state):
        send_email(CHAD_EMAIL, f"Stale Deal Scanner — ABORTED — {now.strftime('%Y-%m-%d')}",
                   "Could not write stale-scanner-state.json to full-pipeline-cache. "
                   "No notices sent and no deals changed. The scanner's IAM role needs s3:PutObject on that bucket.")
        return {"statusCode": 500, "error": "state not writable"}

    active_ids = set()
    headsup, to_notice, to_obsolete, pending = [], [], [], []

    for deal in all_deals:
        cf       = deal.get("custom_fields", {})
        stage_id = (deal.get("deal_stage") or {}).get("id")
        if stage_id not in STAGE_DEFAULTS:
            continue
        did = str(deal.get("id"))
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

        # Pending notice?
        if rec.get("noticed_at"):
            noticed_dt = datetime.fromisoformat(rec["noticed_at"])
            if updated_str != rec.get("updated_at_at_notice"):
                # Edited since notice (client form, Chad, anyone) → cancel removal
                rec.pop("noticed_at", None)
                rec.pop("updated_at_at_notice", None)
            else:
                remove_on = noticed_dt + timedelta(days=GRACE_DAYS)
                if now >= remove_on:
                    to_obsolete.append((stage_name, deal))
                else:
                    pending.append((remove_on, stage_name, deal))
                sd[did] = rec
                continue

        clock_start = updated_dt
        if rec.get("reset_at"):
            try:
                clock_start = max(clock_start, datetime.fromisoformat(rec["reset_at"]))
            except Exception:
                pass
        age = (now - clock_start).days
        due_on = clock_start + timedelta(days=refresh_days)

        if age >= refresh_days:
            to_notice.append((age, stage_name, deal, updated_str))
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

    to_notice.sort(key=lambda x: x[0], reverse=True)

    # ── Obsolete deals whose grace period expired ──
    obsoleted_lines = []
    for stage_name, deal in to_obsolete:
        did = str(deal.get("id"))
        name = deal.get("name", did)
        if DRY_RUN:
            obsoleted_lines.append(f"[DRY RUN] {name} (was {stage_name})")
            continue
        r = call_pipeline_api("PUT", f"/deals/{did}.json",
                              {"deal": {"deal_stage_id": OBSOLETE_STAGE_ID,
                                        "custom_fields": {REFRESH_FIELD: None}}}, jwt=jwt)
        if r["status"] == 200:
            sd[did] = {"obsoleted_at": now.isoformat()}
            obsoleted_lines.append(f"{name} (was {stage_name})")
        else:
            obsoleted_lines.append(f"{name} — Obsolete FAILED ({r['status']}), will retry tomorrow")
    if not DRY_RUN:
        save_state(state)

    # ── Send notices ──
    noticed = []   # (name, did, email, remove_on)
    no_email = []
    deferred = 0
    remove_on = now + timedelta(days=GRACE_DAYS)
    for age, stage_name, deal, updated_str in to_notice:
        if len(noticed) >= MAX_NOTICES_PER_RUN:
            deferred += 1
            continue
        did  = str(deal.get("id"))
        name = deal.get("name", did)
        contact    = deal.get("primary_contact") or {}
        contact_id = contact.get("id") or deal.get("primary_contact_id")
        contact_name  = contact.get("full_name", "")
        contact_email = ""
        if contact_id:
            pr = call_pipeline_api("GET", f"/people/{contact_id}.json", jwt=jwt)
            if pr["status"] == 200 and isinstance(pr["data"], dict):
                contact_email = (pr["data"].get("email") or "").strip()
        if not contact_email:
            no_email.append((name, did))
            continue

        try:
            subject, body, body_html = compose_email(
                deal, contact_name, contact_email, "", form_url(int(did)), age,
                remove_on.strftime("%B %d, %Y").replace(" 0", " "))
        except Exception as e:
            logger.error(f"Bedrock error for deal {did}: {e}")
            continue

        if DRY_RUN:
            noticed.append((f"[DRY RUN] {name}", did, contact_email, remove_on))
            continue

        actual = TEST_EMAIL_OVERRIDE or contact_email
        if TEST_EMAIL_OVERRIDE:
            body = f"[TEST — real recipient would be: {contact_email}]\n\n{body}"
            body_html = f"<p style=\"background:#fffbe6;padding:8px;font-size:12px;\">[TEST — real recipient would be: {contact_email}]</p>" + body_html
        try:
            send_email(actual, subject, body, body_html, source=CLIENT_FROM, reply_to=REPLY_TO)
        except Exception as e:
            logger.error(f"SES error for deal {did}: {e}")
            continue

        sd[did] = {"noticed_at": now.isoformat(), "updated_at_at_notice": updated_str}
        if not save_state(state):
            logger.error("State save failed mid-run; stopping notices")
            noticed.append((name, did, actual, remove_on))
            break
        noticed.append((name, did, actual, remove_on))

    # ── Report to Chad ──
    def link(did):
        u = reset_url(did)
        return f' — <a href="{html.escape(u)}">Reset</a>' if u else ""

    def section(title, rows):
        h = f"<h3 style=\"font-size:15px;margin:22px 0 6px;\">{html.escape(title)}</h3>"
        if not rows:
            return h + "<p style=\"color:#888;margin:0;\">(none)</p>"
        return h + "<ul style=\"margin:0;padding-left:18px;\">" + "".join(f"<li>{r}</li>" for r in rows) + "</ul>"

    dry = " [DRY RUN — no emails to clients, no deals changed]" if DRY_RUN else ""
    headsup.sort(key=lambda x: x[0])
    pending.sort(key=lambda x: x[0])

    h_obs  = [html.escape(x) for x in obsoleted_lines]
    h_not  = [f"{html.escape(n)} → {html.escape(e)} (removal {fmt_day(r)}){link(d)}" for n, d, e, r in noticed]
    h_pend = [f"{html.escape(d.get('name',''))} — removal {fmt_day(r)} ({s}){link(d.get('id'))}" for r, s, d in pending]
    h_head = [f"{html.escape(d.get('name',''))} — due {fmt_day(r)} ({s}){link(d.get('id'))}" for r, s, d in headsup]
    h_noem = [f"{html.escape(n)} (id={d}) — no primary contact email" for n, d in no_email]

    parts = [f"<p><b>Stale Deal Scanner — {now.strftime('%Y-%m-%d')}</b>{html.escape(dry)}</p>"]
    if not SCANNER_URL:
        parts.append("<p style=\"color:#b00;\">Reset links disabled: SCANNER_URL env var is blank.</p>")
    parts.append(section(f"Moved to Obsolete — no update in {GRACE_DAYS} days after notice ({len(h_obs)})", h_obs))
    parts.append(section(f"Removal notices sent today ({len(h_not)})", h_not))
    if deferred:
        parts.append(f"<p style=\"color:#888;\">{deferred} more due deals held for tomorrow (max {MAX_NOTICES_PER_RUN} notices per run).</p>")
    parts.append(section(f"Pending removal — notice already sent ({len(h_pend)})", h_pend))
    parts.append(section(f"Heads-up — due in the next {HEADSUP_DAYS} days ({len(h_head)})", h_head))
    if h_noem:
        parts.append(section(f"Due but skipped ({len(h_noem)})", h_noem))
    report_html = "<html><body style=\"font-family:Arial,sans-serif;font-size:14px;color:#222;line-height:1.5;\">" + "".join(parts) + "</body></html>"

    plain = [f"Stale Deal Scanner — {now.strftime('%Y-%m-%d')}{dry}", "",
             f"Moved to Obsolete ({len(obsoleted_lines)}):", *[f"  • {x}" for x in obsoleted_lines], "",
             f"Notices sent ({len(noticed)}):", *[f"  • {n} → {e} (removal {fmt_day(r)})" for n, d, e, r in noticed], "",
             f"Pending removal ({len(pending)}):", *[f"  • {d.get('name','')} — {fmt_day(r)}" for r, s, d in pending], "",
             f"Heads-up ({len(headsup)}):", *[f"  • {d.get('name','')} — due {fmt_day(r)}" for r, s, d in headsup]]
    try:
        send_email(CHAD_EMAIL,
                   f"{'DRY RUN — ' if DRY_RUN else ''}Stale Deal Scanner Report — {now.strftime('%Y-%m-%d')}",
                   "\n".join(plain), report_html)
    except Exception as e:
        logger.error(f"Could not send summary to Chad: {e}")

    if not DRY_RUN:
        save_state(state)
    logger.info(f"Done. obsoleted={len(obsoleted_lines)} noticed={len(noticed)} pending={len(pending)} headsup={len(headsup)}")
    return {"statusCode": 200, "dry_run": DRY_RUN, "obsoleted": len(obsoleted_lines),
            "noticed": len(noticed), "pending": len(pending), "headsup": len(headsup)}
