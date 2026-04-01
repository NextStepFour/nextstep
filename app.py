import io
import json
import os
import sqlite3
import hashlib
import hmac
import statistics
import time
from datetime import datetime
from xml.sax.saxutils import escape

import pandas as pd
import stripe
import streamlit as st
from openai import OpenAI
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer


APP_NAME = "NextStepSignal"
APP_TAGLINE = "Market intelligence for operational expansion and opportunity discovery"
DB_PATH = os.getenv("NEXTSTEP_DB_PATH", "nextstep_portal.db")
DEFAULT_CREDITS = 50
TIME_OPTIONS = ["2 weeks", "1 month", "2 months", "3 months"]
APP_BASE_URL = os.getenv("APP_BASE_URL", "http://localhost:8501")
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY", "")
DISCOVERY_MODEL = os.getenv("OPENAI_DISCOVERY_MODEL", "gpt-5-mini")
SYNTHESIS_MODEL = os.getenv("OPENAI_SYNTHESIS_MODEL", "gpt-5-mini")
PLANS = {
    "starter": {
        "name": "Starter",
        "price_id": os.getenv("STRIPE_PRICE_ID_STARTER", ""),
        "monthly_credits": 50,
    },
    "pro": {
        "name": "Pro",
        "price_id": os.getenv("STRIPE_PRICE_ID_PRO", ""),
        "monthly_credits": 200,
    },
}
EVIDENCE_COLUMNS = [
    "matched_service",
    "company_name",
    "job_title",
    "location",
    "country",
    "source_type",
    "opportunity_status",
    "posted_date",
    "match_score",
    "match_type",
    "likely_service_need",
    "buyer_department",
    "outreach_next_step",
    "why_it_matches",
    "matching_responsibilities",
    "matching_keywords",
    "source_url",
]
COMPANY_COLUMNS = [
    "buyer_company",
    "job_posting_title",
    "matched_services",
    "opportunity_score",
    "likely_buyer_department_general",
    "best_matching_postings",
    "source_urls",
]
EXPANSION_COLUMNS = [
    "suggested_service",
    "market_frequency_score",
    "supporting_signal_count",
    "expansion_priority",
    "adjacency_type",
    "why_it_is_relevant",
    "sample_companies",
    "sample_job_titles",
    "helpful_for",
    "go_to_market_note",
]
DISPLAY_NAME_MAP = {
    "buyer_company": "Buyer Company",
    "job_posting_title": "Job Posting Title",
    "matched_services": "Matched Services",
    "opportunity_score": "Opportunity Score",
    "likely_buyer_department_general": "Likely Buyer Department (General)",
    "best_matching_postings": "Best Matching Posting",
    "source_urls": "Source URLs",
    "matched_service": "Matched Service",
    "company_name": "Company Name",
    "job_title": "Job Title",
    "source_type": "Source Type",
    "opportunity_status": "Opportunity Status",
    "posted_date": "Posted Date",
    "match_score": "Match Score",
    "match_type": "Match Type",
    "likely_service_need": "Likely Service Need",
    "buyer_department": "Buyer Department",
    "matching_responsibilities": "Matching Responsibilities",
    "matching_keywords": "Matching Keywords",
    "source_url": "Source URL",
    "run_name": "Run Name",
    "services_text": "Services",
    "location_filter": "Location Filter",
    "time_window": "Time Window",
    "mode": "Search Mode",
    "credits_used": "Credits Used",
    "created_at": "Created At",
    "estimated_search_time": "Actual Run Time",
    "target_location": "Target Location",
    "service_name": "Service Name",
    "service_description": "Service Description",
    "suggested_service": "Suggested Service",
    "market_frequency_score": "Market Frequency Score",
    "supporting_signal_count": "Supporting Signal Count",
    "expansion_priority": "Expansion Priority",
    "adjacency_type": "Adjacency Type",
    "why_it_is_relevant": "Why It Is Relevant",
    "sample_companies": "Sample Companies",
    "sample_job_titles": "Sample Job Titles",
    "helpful_for": "Helpful For",
    "go_to_market_note": "Go To Market Note",
    "source_run_id": "Source Run ID",
    "source_run_name": "Source Run Name",
    "source_run_created_at": "Source Run Created At",
    "source_services": "Source Services",
}

st.set_page_config(page_title=APP_NAME, layout="wide")


PROMPT_TEMPLATE = """You are a market intelligence engine for solar service sales.

Your task is to search the public web for U.S. solar job postings, recently filled roles, RFPs, and similar opportunities from the last {{TIME_WINDOW}} that overlap with the service description below.

Search broadly across public websites, job boards, company careers pages, and RFP/procurement pages.

Target geography inside the United States:
{{LOCATION_FILTER}}

Search source emphasis:
{{SOURCE_GUIDANCE}}

{{RELEVANCE_RULE}}
Only include results that appear to be from the last {{TIME_WINDOW}}.
Only include results that are clearly located in the United States.
{{VOLUME_RULE}}

Service description:
{{SERVICE_DESCRIPTION}}

Return valid JSON only using this schema:
{
  "results": [
    {
      "company_name": null,
      "job_title": null,
      "location": null,
      "country": null,
      "source_type": null,
      "opportunity_status": "Open|Filled|Unknown",
      "posted_date": null,
      "match_score": 0,
      "match_type": "Direct|Peripheral|Weak|None",
      "likely_service_need": null,
      "why_it_matches": [],
      "matching_responsibilities": [],
      "matching_keywords": [],
      "buyer_department": null,
      "outreach_next_step": null,
      "source_url": null
    }
  ]
}

Rules:
- Search the internet first before answering
- Include only public web results you actually found
- Include only U.S. results
- Include only results that appear to be from the last {{TIME_WINDOW}}
- Prioritize results in the target geography when possible
- opportunity_status must be one of: Open, Filled, Unknown
- match_score must be 0 to 100
- match_type must be one of: Direct, Peripheral, Weak, None
- Direct = the posting clearly describes work that overlaps strongly with the service
- Peripheral = adjacent responsibilities suggest potential need
- Weak = minor overlap only
- None = no meaningful overlap
- why_it_matches must be a list
- matching_responsibilities must be a list
- matching_keywords must be a list
- buyer_department should reflect the team most likely tied to the hiring signal when supported by the posting
- outreach_next_step should be a short, practical next step based only on the posting and likely buyer department
- If a field is unknown, return null
- source_url must be the public URL for the result
- Return JSON only"""


RESPONSE_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "results": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "company_name": {"type": ["string", "null"]},
                    "job_title": {"type": ["string", "null"]},
                    "location": {"type": ["string", "null"]},
                    "country": {"type": ["string", "null"]},
                    "source_type": {"type": ["string", "null"]},
                    "opportunity_status": {
                        "type": "string",
                        "enum": ["Open", "Filled", "Unknown"],
                    },
                    "posted_date": {"type": ["string", "null"]},
                    "match_score": {"type": "integer", "minimum": 0, "maximum": 100},
                    "match_type": {
                        "type": "string",
                        "enum": ["Direct", "Peripheral", "Weak", "None"],
                    },
                    "likely_service_need": {"type": ["string", "null"]},
                    "why_it_matches": {"type": "array", "items": {"type": "string"}},
                    "matching_responsibilities": {"type": "array", "items": {"type": "string"}},
                    "matching_keywords": {"type": "array", "items": {"type": "string"}},
                    "buyer_department": {"type": ["string", "null"]},
                    "outreach_next_step": {"type": ["string", "null"]},
                    "source_url": {"type": ["string", "null"]},
                },
                "required": [
                    "company_name",
                    "job_title",
                    "location",
                    "country",
                    "source_type",
                    "opportunity_status",
                    "posted_date",
                    "match_score",
                    "match_type",
                    "likely_service_need",
                    "why_it_matches",
                    "matching_responsibilities",
                    "matching_keywords",
                    "buyer_department",
                    "outreach_next_step",
                    "source_url",
                ],
            },
        }
    },
    "required": ["results"],
}

EXPANSION_PROMPT_TEMPLATE = """You are a market intelligence engine for solar service sales strategy.

Your task is to review a client's current service coverage and recent market evidence, then suggest peripheral service expansions that appear to be requested by the market but are not explicitly covered by the current service set.

Current service profiles:
{{CURRENT_SERVICES}}

Market evidence from recent U.S. solar jobs, filled roles, RFPs, and similar opportunities:
{{MARKET_EVIDENCE}}

Return valid JSON only using this schema:
{
  "expansions": [
    {
      "suggested_service": null,
      "market_frequency_score": 0,
      "supporting_signal_count": 0,
      "expansion_priority": "High|Medium|Low",
      "adjacency_type": null,
      "why_it_is_relevant": null,
      "sample_companies": [],
      "sample_job_titles": [],
      "helpful_for": null,
      "go_to_market_note": null
    }
  ]
}

Rules:
- Suggest services that are adjacent or peripheral to the current service set
- Do not repeat services already explicitly covered
- Base suggestions only on evidence shown in the market evidence
- market_frequency_score must be 0 to 100
- supporting_signal_count must be an integer
- expansion_priority must be one of: High, Medium, Low
- sample_companies must be a list
- sample_job_titles must be a list
- Return 3 to 8 expansion ideas when possible
- Return JSON only"""

EXPANSION_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "expansions": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "suggested_service": {"type": ["string", "null"]},
                    "market_frequency_score": {"type": "integer", "minimum": 0, "maximum": 100},
                    "supporting_signal_count": {"type": "integer", "minimum": 0},
                    "expansion_priority": {
                        "type": "string",
                        "enum": ["High", "Medium", "Low"],
                    },
                    "adjacency_type": {"type": ["string", "null"]},
                    "why_it_is_relevant": {"type": ["string", "null"]},
                    "sample_companies": {"type": "array", "items": {"type": "string"}},
                    "sample_job_titles": {"type": "array", "items": {"type": "string"}},
                    "helpful_for": {"type": ["string", "null"]},
                    "go_to_market_note": {"type": ["string", "null"]},
                },
                "required": [
                    "suggested_service",
                    "market_frequency_score",
                    "supporting_signal_count",
                    "expansion_priority",
                    "adjacency_type",
                    "why_it_is_relevant",
                    "sample_companies",
                    "sample_job_titles",
                    "helpful_for",
                    "go_to_market_note",
                ],
            },
        }
    },
    "required": ["expansions"],
}


def conn():
    db_dir = os.path.dirname(DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    db = sqlite3.connect(DB_PATH, timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA foreign_keys=ON")
    return db


def init_db():
    with conn() as db:
        db.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                full_name TEXT NOT NULL,
                email TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                stripe_customer_id TEXT,
                subscription_status TEXT NOT NULL DEFAULT 'inactive',
                plan_name TEXT,
                monthly_credit_allowance INTEGER NOT NULL DEFAULT 0,
                credit_balance INTEGER NOT NULL DEFAULT 0,
                last_credit_refresh TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS services (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                service_name TEXT NOT NULL,
                service_description TEXT NOT NULL,
                target_location TEXT NOT NULL,
                default_time_window TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS searches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                run_name TEXT NOT NULL,
                services_text TEXT NOT NULL,
                service_count INTEGER NOT NULL DEFAULT 0,
                location_filter TEXT NOT NULL,
                time_window TEXT NOT NULL,
                high_volume_mode INTEGER NOT NULL,
                enrichment_enabled INTEGER NOT NULL DEFAULT 1,
                credits_used INTEGER NOT NULL,
                duration_seconds REAL,
                created_at TEXT NOT NULL,
                company_json TEXT NOT NULL,
                evidence_json TEXT NOT NULL
            )
            """
        )
        db.execute(
            "INSERT OR IGNORE INTO settings (key, value) VALUES ('credits_balance', ?)",
            (str(DEFAULT_CREDITS),),
        )
        service_columns = [row["name"] for row in db.execute("PRAGMA table_info(services)").fetchall()]
        if "user_id" not in service_columns:
            db.execute("ALTER TABLE services ADD COLUMN user_id INTEGER")
        search_columns = [row["name"] for row in db.execute("PRAGMA table_info(searches)").fetchall()]
        if "user_id" not in search_columns:
            db.execute("ALTER TABLE searches ADD COLUMN user_id INTEGER")
        if "service_count" not in search_columns:
            db.execute("ALTER TABLE searches ADD COLUMN service_count INTEGER NOT NULL DEFAULT 0")
        if "enrichment_enabled" not in search_columns:
            db.execute("ALTER TABLE searches ADD COLUMN enrichment_enabled INTEGER NOT NULL DEFAULT 1")
        if "duration_seconds" not in search_columns:
            db.execute("ALTER TABLE searches ADD COLUMN duration_seconds REAL")


def hash_password(password):
    salt = os.urandom(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 120000)
    return f"{salt.hex()}:{digest.hex()}"


def verify_password(password, password_hash):
    try:
        salt_hex, digest_hex = password_hash.split(":")
    except ValueError:
        return False
    salt = bytes.fromhex(salt_hex)
    expected = bytes.fromhex(digest_hex)
    actual = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 120000)
    return hmac.compare_digest(actual, expected)


def get_user_by_email(email):
    with conn() as db:
        row = db.execute(
            "SELECT * FROM users WHERE lower(email) = lower(?)",
            (email.strip(),),
        ).fetchone()
    return dict(row) if row else None


def get_user_by_id(user_id):
    if not user_id:
        return None
    with conn() as db:
        row = db.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
    return dict(row) if row else None


def create_user(full_name, email, password):
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    with conn() as db:
        cursor = db.execute(
            """
            INSERT INTO users (
                full_name, email, password_hash, subscription_status,
                plan_name, monthly_credit_allowance, credit_balance, created_at
            ) VALUES (?, ?, ?, 'inactive', null, 0, 5, ?)
            """,
            (
                full_name.strip(),
                email.strip().lower(),
                hash_password(password),
                created_at,
            ),
        )
    return get_user_by_id(cursor.lastrowid)


def update_user_fields(user_id, **fields):
    if not fields:
        return
    assignments = ", ".join([f"{key} = ?" for key in fields.keys()])
    values = list(fields.values()) + [user_id]
    with conn() as db:
        db.execute(f"UPDATE users SET {assignments} WHERE id = ?", values)


def current_user():
    return get_user_by_id(st.session_state.get("user_id"))


def set_current_user(user):
    if user:
        st.session_state["user_id"] = user["id"]
    else:
        st.session_state.pop("user_id", None)


def credits(user_id=None):
    user = get_user_by_id(user_id) if user_id else current_user()
    return int(user["credit_balance"]) if user else 0


def set_credits(value, user_id=None):
    user = get_user_by_id(user_id) if user_id else current_user()
    if user:
        update_user_fields(user["id"], credit_balance=max(0, int(value)))


def add_credits(delta, user_id=None):
    user = get_user_by_id(user_id) if user_id else current_user()
    if not user:
        return 0
    new_value = max(0, int(user["credit_balance"]) + int(delta))
    update_user_fields(user["id"], credit_balance=new_value)
    return new_value


def services_df(user_id=None):
    user = get_user_by_id(user_id) if user_id else current_user()
    if not user:
        return pd.DataFrame()
    with conn() as db:
        rows = db.execute(
            "SELECT * FROM services WHERE user_id = ? ORDER BY id DESC",
            (user["id"],),
        ).fetchall()
    return pd.DataFrame([dict(r) for r in rows])


def runs_df(user_id=None):
    user = get_user_by_id(user_id) if user_id else current_user()
    if not user:
        return pd.DataFrame()
    with conn() as db:
        rows = db.execute(
            "SELECT * FROM searches WHERE user_id = ? ORDER BY id DESC",
            (user["id"],),
        ).fetchall()
    df = pd.DataFrame([dict(r) for r in rows])
    if not df.empty:
        df["mode"] = df["high_volume_mode"].map({1: "High volume", 0: "Focused"})
        if "duration_seconds" in df.columns:
            df["estimated_search_time"] = df["duration_seconds"].apply(format_duration_text)
    return df


def get_run(run_id, user_id=None):
    user = get_user_by_id(user_id) if user_id else current_user()
    if not user:
        return None
    with conn() as db:
        row = db.execute(
            "SELECT * FROM searches WHERE id = ? AND user_id = ?",
            (run_id, user["id"]),
        ).fetchone()
    return dict(row) if row else None


def delete_run(run_id, user_id=None):
    user = get_user_by_id(user_id) if user_id else current_user()
    if not user:
        raise ValueError("Please sign in to delete saved lists.")
    with conn() as db:
        db.execute(
            "DELETE FROM searches WHERE id = ? AND user_id = ?",
            (run_id, user["id"]),
        )


def save_service(name, description, location_filter, user_id=None):
    user = get_user_by_id(user_id) if user_id else current_user()
    if not user:
        raise ValueError("Please sign in to save service profiles.")
    with conn() as db:
        db.execute(
            """
            INSERT INTO services (
                user_id, service_name, service_description, target_location,
                default_time_window, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                user["id"],
                name.strip(),
                description.strip(),
                location_filter.strip(),
                "2 months",
                datetime.now().strftime("%Y-%m-%d %H:%M"),
            ),
        )


def save_run(
    run_name,
    services_text,
    service_count,
    location_filter,
    time_window,
    high_volume_mode,
    enrichment_enabled,
    credits_used,
    duration_seconds,
    company_df,
    evidence_df,
    user_id=None,
):
    user = get_user_by_id(user_id) if user_id else current_user()
    if not user:
        raise ValueError("Please sign in to save lists.")
    with conn() as db:
        cursor = db.execute(
            """
            INSERT INTO searches (
                user_id, run_name, services_text, service_count, location_filter, time_window,
                high_volume_mode, enrichment_enabled, credits_used, duration_seconds,
                created_at, company_json, evidence_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user["id"],
                run_name,
                services_text,
                int(service_count),
                location_filter,
                time_window,
                int(high_volume_mode),
                int(enrichment_enabled),
                credits_used,
                float(duration_seconds) if duration_seconds is not None else None,
                datetime.now().strftime("%Y-%m-%d %H:%M"),
                company_df.to_json(orient="records"),
                evidence_df.to_json(orient="records"),
            ),
        )
    return cursor.lastrowid


def load_df(json_text):
    try:
        return pd.DataFrame(json.loads(json_text or "[]"))
    except (TypeError, json.JSONDecodeError):
        return pd.DataFrame()


def client():
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY is not set.")
    return OpenAI(api_key=api_key)


def format_duration_text(seconds):
    if seconds is None or pd.isna(seconds):
        return ""
    total_seconds = max(0, int(round(float(seconds))))
    minutes, remaining_seconds = divmod(total_seconds, 60)
    if minutes and remaining_seconds:
        return f"{minutes} min {remaining_seconds} sec"
    if minutes:
        return f"{minutes} min"
    return f"{remaining_seconds} sec"


def format_duration_range_text(min_seconds, max_seconds):
    low = format_duration_text(min_seconds)
    high = format_duration_text(max_seconds)
    if low == high:
        return low
    return f"{low} to {high}"


def fallback_estimate_seconds(service_count, high_volume_mode, time_window):
    search_calls = sum(2 if high_volume_mode else 1 for _ in range(service_count))
    time_factor = {
        "2 weeks": 0,
        "1 month": 2,
        "2 months": 4,
        "3 months": 6,
    }.get(time_window, 4)
    search_seconds_per_call = 10 if high_volume_mode else 7
    estimate = search_calls * (search_seconds_per_call + time_factor)
    return int(estimate)


def estimate_search_time(service_count, high_volume_mode, time_window, user_id=None):
    user = get_user_by_id(user_id) if user_id else current_user()
    if not user or service_count <= 0:
        base = fallback_estimate_seconds(service_count, high_volume_mode, time_window)
        return (max(15, int(base * 0.8)), int(base * 1.3)), "default"

    with conn() as db:
        rows = db.execute(
            """
            SELECT service_count, high_volume_mode, duration_seconds
            FROM searches
            WHERE user_id = ? AND duration_seconds IS NOT NULL
            ORDER BY id DESC
            LIMIT 30
            """,
            (user["id"],),
        ).fetchall()

    history = [dict(row) for row in rows]
    exact_matches = [
        float(row["duration_seconds"])
        for row in history
        if int(row["service_count"] or 0) == int(service_count)
        and int(row["high_volume_mode"] or 0) == int(high_volume_mode)
    ]
    if exact_matches:
        median_seconds = int(round(statistics.median(exact_matches)))
        spread = max(10, int(median_seconds * 0.2))
        return (max(10, median_seconds - spread), median_seconds + spread), "history"

    similar_runs = [
        float(row["duration_seconds"]) / max(1, int(row["service_count"] or 1))
        for row in history
        if int(row["high_volume_mode"] or 0) == int(high_volume_mode)
    ]
    if similar_runs:
        median_seconds = int(round(statistics.median(similar_runs) * service_count))
        spread = max(15, int(median_seconds * 0.25))
        return (max(15, median_seconds - spread), median_seconds + spread), "history"

    base = fallback_estimate_seconds(service_count, high_volume_mode, time_window)
    return (max(15, int(base * 0.8)), int(base * 1.3)), "default"


def stripe_ready():
    return bool(STRIPE_SECRET_KEY and any(plan["price_id"] for plan in PLANS.values()))


def price_to_plan(price_id):
    for plan_key, plan in PLANS.items():
        if plan["price_id"] == price_id:
            return plan_key, plan
    return None, None


def stripe_api():
    if not STRIPE_SECRET_KEY:
        raise ValueError("STRIPE_SECRET_KEY is not set.")
    stripe.api_key = STRIPE_SECRET_KEY
    return stripe


def refresh_user_credits_for_plan(user):
    if not user or user.get("subscription_status") not in {"active", "trialing"}:
        return
    allowance = int(user.get("monthly_credit_allowance") or 0)
    if allowance <= 0:
        return
    current_month = datetime.now().strftime("%Y-%m")
    if user.get("last_credit_refresh") != current_month:
        update_user_fields(
            user["id"],
            credit_balance=allowance,
            last_credit_refresh=current_month,
        )


def sync_user_billing(user):
    if not user or not stripe_ready():
        return user
    stripe_mod = stripe_api()
    customer = None
    if user.get("stripe_customer_id"):
        try:
            customer = stripe_mod.Customer.retrieve(user["stripe_customer_id"])
        except Exception:
            customer = None
    if not customer:
        customers = stripe_mod.Customer.list(email=user["email"], limit=1)
        customer = customers.data[0] if customers.data else None

    fields = {}
    if customer:
        fields["stripe_customer_id"] = customer.id
        subscriptions = stripe_mod.Subscription.list(customer=customer.id, status="all", limit=10)
        active_sub = next(
            (
                sub
                for sub in subscriptions.data
                if sub.status in {"active", "trialing", "past_due", "unpaid"}
            ),
            None,
        )
        if active_sub:
            price_id = active_sub["items"]["data"][0]["price"]["id"]
            plan_key, plan = price_to_plan(price_id)
            fields["subscription_status"] = active_sub.status
            fields["plan_name"] = plan["name"] if plan else None
            fields["monthly_credit_allowance"] = plan["monthly_credits"] if plan else 0
        else:
            fields["subscription_status"] = "inactive"
            fields["plan_name"] = None
            fields["monthly_credit_allowance"] = 0
    else:
        fields["subscription_status"] = "inactive"
        fields["plan_name"] = None
        fields["monthly_credit_allowance"] = 0

    update_user_fields(user["id"], **fields)
    updated_user = get_user_by_id(user["id"])
    refresh_user_credits_for_plan(updated_user)
    return get_user_by_id(user["id"])


def checkout_url_for_plan(user, plan_key):
    plan = PLANS[plan_key]
    if not plan["price_id"]:
        raise ValueError(f"Stripe price ID for {plan['name']} is not set.")
    stripe_mod = stripe_api()
    session = stripe_mod.checkout.Session.create(
        mode="subscription",
        line_items=[{"price": plan["price_id"], "quantity": 1}],
        success_url=f"{APP_BASE_URL}?billing=success",
        cancel_url=f"{APP_BASE_URL}?billing=cancel",
        customer_email=user["email"],
        metadata={"user_id": str(user["id"]), "plan_key": plan_key},
        allow_promotion_codes=True,
    )
    return session.url


def billing_portal_url(user):
    if not user.get("stripe_customer_id"):
        raise ValueError("No Stripe customer found for this account yet.")
    stripe_mod = stripe_api()
    session = stripe_mod.billing_portal.Session.create(
        customer=user["stripe_customer_id"],
        return_url=APP_BASE_URL,
    )
    return session.url


def build_prompt(description, location_filter, time_window, high_volume_mode):
    prompt = PROMPT_TEMPLATE
    prompt = prompt.replace("{{SERVICE_DESCRIPTION}}", description.strip())
    prompt = prompt.replace("{{LOCATION_FILTER}}", location_filter.strip())
    prompt = prompt.replace("{{TIME_WINDOW}}", time_window)
    prompt = prompt.replace(
        "{{SOURCE_GUIDANCE}}",
        (
            "Search across company career pages, ATS sites such as Greenhouse, Lever, Workday, iCIMS, SmartRecruiters, Ashby, and public procurement or RFP pages. Also search adjacent job-title variants and subcontracting-related responsibilities."
            if high_volume_mode
            else "Prioritize company career pages, public ATS sites, and relevant procurement or RFP pages."
        ),
    )
    prompt = prompt.replace(
        "{{RELEVANCE_RULE}}",
        "Include strongly relevant results, plus adjacent or partially overlapping results that could still indicate a likely buyer need."
        if high_volume_mode
        else "Only include results that are clearly relevant to the service description.",
    )
    prompt = prompt.replace(
        "{{VOLUME_RULE}}",
        "In high volume mode, prefer broader recall. Include Direct, Peripheral, and Weak matches when they still indicate a possible buyer signal. Return up to 25 results when available."
        if high_volume_mode
        else "Prefer higher-confidence matches over broad recall. Return up to 15 results when available.",
    )
    return prompt


def search_variants(service_row, high_volume_mode):
    base_description = safe_text(service_row["service_description"])
    service_name = safe_text(service_row["service_name"])
    variants = [base_description]
    if high_volume_mode:
        variants.append(
            f"{service_name}\n\nFocus on role-title variants, adjacent responsibilities, company careers pages, ATS pages, and strong hiring signals tied to this service."
        )
    return variants


def normalize_search_record(item, service_name):
    row = dict(item)
    row["matched_service"] = service_name
    row["company_name"] = row.get("company_name") or "Unknown Company"
    for column in EVIDENCE_COLUMNS:
        row.setdefault(column, None)
    return row


def dedupe_search_records(records):
    deduped = {}
    for row in records:
        key = (
            safe_text(row.get("source_url")),
            safe_text(row.get("company_name")),
            safe_text(row.get("job_title")),
        )
        existing = deduped.get(key)
        if not existing or int(row.get("match_score") or 0) > int(existing.get("match_score") or 0):
            deduped[key] = row
    return list(deduped.values())


def search_service(api_client, service_row, location_filter, time_window, high_volume_mode):
    raw_responses = []
    collected_records = []
    for idx, variant_description in enumerate(search_variants(service_row, high_volume_mode), start=1):
        response = api_client.responses.create(
            model=DISCOVERY_MODEL,
            reasoning={"effort": "low"},
            tools=[{"type": "web_search", "user_location": {"type": "approximate", "country": "US", "timezone": "America/New_York"}}],
            tool_choice="auto",
            include=["web_search_call.action.sources"],
            input=build_prompt(variant_description, location_filter, time_window, high_volume_mode),
            text={"format": {"type": "json_schema", "name": f"nextstep_portal_search_{idx}", "strict": True, "schema": RESPONSE_SCHEMA}},
        )
        raw_json = response.output_text if getattr(response, "output_text", None) else ""
        raw_responses.append(raw_json)
        try:
            parsed = json.loads(raw_json)
        except json.JSONDecodeError as exc:
            raise ValueError("The API returned an invalid JSON response.") from exc
        for item in parsed.get("results", []):
            collected_records.append(normalize_search_record(item, service_row["service_name"]))
    return raw_responses, dedupe_search_records(collected_records)


def build_expansion_context(selected_services_df, evidence_df):
    services_payload = []
    for _, row in selected_services_df.iterrows():
        services_payload.append(
            {
                "service_name": row["service_name"],
                "service_description": row["service_description"],
                "target_location": row["target_location"],
            }
        )

    evidence_payload = []
    for _, row in evidence_df.head(50).iterrows():
        evidence_payload.append(
            {
                "matched_service": row["matched_service"],
                "company_name": row["company_name"],
                "job_title": row["job_title"],
                "location": row["location"],
                "match_score": row["match_score"],
                "match_type": row["match_type"],
                "likely_service_need": row["likely_service_need"],
                "buyer_department": row["buyer_department"],
                "matching_responsibilities": row["matching_responsibilities"],
                "matching_keywords": row["matching_keywords"],
                "source_url": row["source_url"],
            }
        )

    return json.dumps(services_payload, indent=2), json.dumps(evidence_payload, indent=2)


def analyze_expansions(api_client, selected_services_df, evidence_df):
    current_services_json, market_evidence_json = build_expansion_context(
        selected_services_df, evidence_df
    )
    prompt = EXPANSION_PROMPT_TEMPLATE
    prompt = prompt.replace("{{CURRENT_SERVICES}}", current_services_json)
    prompt = prompt.replace("{{MARKET_EVIDENCE}}", market_evidence_json)

    response = api_client.responses.create(
        model=SYNTHESIS_MODEL,
        reasoning={"effort": "low"},
        input=prompt,
        text={
            "format": {
                "type": "json_schema",
                "name": "nextstep_potential_expansions",
                "strict": True,
                "schema": EXPANSION_SCHEMA,
            }
        },
    )

    raw_json = response.output_text if getattr(response, "output_text", None) else ""
    try:
        parsed = json.loads(raw_json)
    except json.JSONDecodeError as exc:
        raise ValueError("The API returned invalid JSON for potential expansions.") from exc

    expansion_df = pd.DataFrame(parsed.get("expansions", []))
    if expansion_df.empty:
        return raw_json, pd.DataFrame(columns=EXPANSION_COLUMNS)

    expansion_df = expansion_df[EXPANSION_COLUMNS].sort_values(
        ["market_frequency_score", "supporting_signal_count"],
        ascending=[False, False],
    ).reset_index(drop=True)
    return raw_json, expansion_df


def ensure_evidence_columns(evidence_df):
    working = evidence_df.copy()
    for column in EVIDENCE_COLUMNS:
        if column not in working.columns:
            working[column] = None
    return working[EVIDENCE_COLUMNS]


def flatten_unique(values):
    found = []
    for value in values:
        if isinstance(value, list):
            for item in value:
                if pd.notna(item):
                    item_text = str(item).strip()
                    if item_text and item_text not in found:
                        found.append(item_text)
        elif pd.notna(value):
            value_text = str(value).strip()
            if value_text and value_text not in found:
                found.append(value_text)
    return found


def safe_text(value, default=""):
    if pd.isna(value):
        return default
    text = str(value).strip()
    return text if text else default


def aggregate_companies(evidence_df):
    if evidence_df.empty:
        return pd.DataFrame(columns=COMPANY_COLUMNS)

    temp = evidence_df.copy()
    temp["posted_date_parsed"] = pd.to_datetime(temp["posted_date"], errors="coerce")
    rows = []
    for company, group in temp.groupby("company_name", dropna=False):
        best = group.sort_values("match_score", ascending=False).iloc[0]
        matched_services = flatten_unique(group["matched_service"].tolist())
        urls = flatten_unique(group["source_url"].tolist())[:5]
        direct = int((group["match_type"] == "Direct").sum())
        peripheral = int((group["match_type"] == "Peripheral").sum())
        score = min(
            100,
            int(group["match_score"].max()) + min(len(group) - 1, 4) * 5 + min(direct, 2) * 5 + min(peripheral, 2) * 3,
        )
        likely_buyer_department = (
            pd.Series([x for x in group["buyer_department"] if pd.notna(x) and str(x).strip()]).mode().iloc[0]
            if any(pd.notna(group["buyer_department"]))
            else None
        )
        best_postings = flatten_unique(group.sort_values("match_score", ascending=False)["job_title"].tolist())[:5]
        rows.append(
            {
                "buyer_company": safe_text(company, "Unknown Company"),
                "job_posting_title": safe_text(best["job_title"]),
                "matched_services": "; ".join(matched_services),
                "opportunity_score": score,
                "likely_buyer_department_general": likely_buyer_department,
                "best_matching_postings": " | ".join(best_postings),
                "source_urls": " | ".join(urls),
            }
        )
    df = pd.DataFrame(rows)
    return df[COMPANY_COLUMNS].sort_values(["opportunity_score", "buyer_company"], ascending=[False, True]).reset_index(drop=True)


def format_lists_for_display(df):
    display = df.copy()
    for column in [
        "why_it_matches",
        "matching_responsibilities",
        "matching_keywords",
        "sample_companies",
        "sample_job_titles",
    ]:
        if column in display.columns:
            display[column] = display[column].apply(lambda value: "; ".join(value) if isinstance(value, list) else value)
    return display


def pretty_df(df):
    return df.rename(
        columns={
            column: DISPLAY_NAME_MAP.get(
                column,
                column.replace("_", " ").title().replace(" Id", " ID").replace(" Url", " URL"),
            )
            for column in df.columns
        }
    )


def csv_data(df):
    buffer = io.StringIO()
    pretty_df(df).to_csv(buffer, index=False)
    return buffer.getvalue()


def pdf_data(company_df, meta):
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    styles = getSampleStyleSheet()
    story = [
        Paragraph(f"{APP_NAME} Opportunity Report", styles["Title"]),
        Spacer(1, 10),
        Paragraph(escape(f"Run name: {meta['run_name']}"), styles["Normal"]),
        Paragraph(escape(f"Generated: {meta['created_at']}"), styles["Normal"]),
        Paragraph(escape(f"Services: {meta['services_text']}"), styles["Normal"]),
        Paragraph(escape(f"Location filter: {meta['location_filter']}"), styles["Normal"]),
        Paragraph(escape(f"Time window: {meta['time_window']} | Search mode: {meta['mode']}"), styles["Normal"]),
        Spacer(1, 14),
        Paragraph("Buyer Company List", styles["Heading1"]),
    ]
    for _, row in company_df.head(20).iterrows():
        story.extend(
            [
                Paragraph(escape(str(row["buyer_company"])), styles["Heading2"]),
                Paragraph(escape(f"Job posting title: {row['job_posting_title'] or 'Unknown'}"), styles["Normal"]),
                Paragraph(escape(f"Opportunity score: {row['opportunity_score']}"), styles["Normal"]),
                Paragraph(escape(f"Matched services: {row['matched_services']}"), styles["Normal"]),
                Paragraph(escape(f"Likely buyer department: {row['likely_buyer_department_general'] or 'Unknown'}"), styles["Normal"]),
                Paragraph(escape(f"Best matching posting: {row['best_matching_postings'] or 'Unknown'}"), styles["Normal"]),
                Paragraph(escape(f"Source URLs: {row['source_urls'] or 'Unknown'}"), styles["Normal"]),
                Spacer(1, 10),
            ]
        )
    doc.build(story)
    buffer.seek(0)
    return buffer.getvalue()


def expansion_pdf_data(expansion_df, meta):
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    styles = getSampleStyleSheet()
    story = [
        Paragraph(f"{APP_NAME} Potential Expansions Report", styles["Title"]),
        Spacer(1, 10),
        Paragraph(escape(f"Generated: {meta['created_at']}"), styles["Normal"]),
        Paragraph(escape(f"Services analyzed: {meta['services_text']}"), styles["Normal"]),
        Paragraph(escape(f"Location filter: {meta['location_filter']}"), styles["Normal"]),
        Paragraph(escape(f"Time window: {meta['time_window']} | Search mode: {meta['mode']}"), styles["Normal"]),
        Spacer(1, 14),
    ]
    for _, row in expansion_df.iterrows():
        story.extend(
            [
                Paragraph(escape(str(row["suggested_service"] or "Unknown expansion")), styles["Heading2"]),
                Paragraph(escape(f"Market frequency score: {row['market_frequency_score']}"), styles["Normal"]),
                Paragraph(escape(f"Supporting signal count: {row['supporting_signal_count']} | Priority: {row['expansion_priority']}"), styles["Normal"]),
                Paragraph(escape(f"Adjacency type: {row['adjacency_type'] or 'Unknown'}"), styles["Normal"]),
                Paragraph(escape(f"Why it is relevant: {row['why_it_is_relevant'] or 'Unknown'}"), styles["Normal"]),
                Paragraph(escape(f"Helpful for: {row['helpful_for'] or 'Unknown'}"), styles["Normal"]),
                Paragraph(escape(f"Go-to-market note: {row['go_to_market_note'] or 'Unknown'}"), styles["Normal"]),
                Paragraph(escape(f"Sample companies: {row['sample_companies']}"), styles["Normal"]),
                Paragraph(escape(f"Sample job titles: {row['sample_job_titles']}"), styles["Normal"]),
                Spacer(1, 10),
            ]
        )
    doc.build(story)
    buffer.seek(0)
    return buffer.getvalue()


def show_run(run_record, key_prefix):
    evidence_df = ensure_evidence_columns(load_df(run_record["evidence_json"]))
    if evidence_df.empty:
        st.info("This list has no saved evidence.")
        return

    company_df = aggregate_companies(evidence_df)

    st.subheader("Buyer Company List")
    st.dataframe(pretty_df(company_df), use_container_width=True)
    st.download_button(
        "Download buyer company list as CSV",
        data=csv_data(company_df),
        file_name=f"nextstepsignal_buyer_company_list_{run_record['id']}.csv",
        mime="text/csv",
        key=f"{key_prefix}_company_csv",
    )
    st.download_button(
        "Download buyer company list as PDF",
        data=pdf_data(
            company_df,
            {
                "run_name": run_record["run_name"],
                "created_at": run_record["created_at"],
                "services_text": run_record["services_text"],
                "location_filter": run_record["location_filter"],
                "time_window": run_record["time_window"],
                "mode": "High volume" if run_record["high_volume_mode"] else "Focused",
            },
        ),
        file_name=f"nextstepsignal_opportunity_report_{run_record['id']}.pdf",
        mime="application/pdf",
        key=f"{key_prefix}_company_pdf",
    )


def build_master_saved_data():
    runs = runs_df()
    if runs.empty:
        return pd.DataFrame()

    evidence_frames = []
    for _, run_row in runs.iterrows():
        run_record = get_run(run_row["id"])
        if not run_record:
            continue
        evidence_df = ensure_evidence_columns(load_df(run_record["evidence_json"]))
        if evidence_df.empty:
            continue
        evidence_df["source_run_id"] = run_record["id"]
        evidence_df["source_run_name"] = run_record["run_name"]
        evidence_df["source_run_created_at"] = run_record["created_at"]
        evidence_df["source_services"] = run_record["services_text"]
        evidence_frames.append(evidence_df)

    if not evidence_frames:
        return pd.DataFrame()

    master_evidence_df = pd.concat(evidence_frames, ignore_index=True)
    master_evidence_df = master_evidence_df.sort_values(
        by="match_score", ascending=False
    ).reset_index(drop=True)

    return aggregate_companies(master_evidence_df)


def portal_access_allowed(user):
    if not user:
        return False
    return user.get("subscription_status") in {"active", "trialing"} or int(user.get("credit_balance") or 0) > 0


def page_auth():
    st.title(APP_NAME)
    st.subheader(APP_TAGLINE)
    st.write("Create an account to save services, generate prospect lists, and manage subscription access.")
    login_tab, signup_tab = st.tabs(["Sign In", "Create Account"])

    with login_tab:
        with st.form("login_form"):
            email = st.text_input("Email", key="login_email")
            password = st.text_input("Password", type="password", key="login_password")
            submitted = st.form_submit_button("Sign In")
        if submitted:
            user = get_user_by_email(email)
            if not user or not verify_password(password, user["password_hash"]):
                st.error("Invalid email or password.")
            else:
                user = sync_user_billing(user)
                set_current_user(user)
                st.success("Signed in.")
                st.rerun()

    with signup_tab:
        with st.form("signup_form"):
            full_name = st.text_input("Full name", key="signup_name")
            email = st.text_input("Email", key="signup_email")
            password = st.text_input("Password", type="password", key="signup_password")
            submitted = st.form_submit_button("Create Account")
        if submitted:
            if not full_name.strip() or not email.strip() or not password.strip():
                st.error("Please complete all fields.")
            elif get_user_by_email(email):
                st.error("An account with that email already exists.")
            else:
                user = create_user(full_name, email, password)
                set_current_user(user)
                st.success("Account created. You can use starter demo credits or subscribe below.")
                st.rerun()


def page_billing(user):
    st.title("Plans & Billing")
    st.write(f"Choose a plan to make {APP_NAME} public-facing and subscription ready. Demo accounts also keep a small starter credit balance for testing.")
    st.metric("Credits Remaining", credits(user["id"]))
    st.metric("Subscription Status", user.get("subscription_status", "inactive").title())
    st.metric("Current Plan", user.get("plan_name") or "None")

    col1, col2 = st.columns(2)
    for col, plan_key in zip([col1, col2], ["starter", "pro"]):
        plan = PLANS[plan_key]
        with col:
            st.markdown(f"**{plan['name']}**")
            st.write(f"{plan['monthly_credits']} credits per month")
            if stripe_ready() and plan["price_id"]:
                if st.button(f"Subscribe to {plan['name']}", key=f"subscribe_{plan_key}"):
                    try:
                        url = checkout_url_for_plan(user, plan_key)
                        st.markdown(f"[Open Stripe Checkout]({url})")
                    except Exception as exc:
                        st.error(f"Stripe checkout could not be created: {exc}")
            else:
                st.info(f"Set the Stripe price ID for {plan['name']} to enable checkout.")

    if stripe_ready():
        left, right = st.columns(2)
        with left:
            if st.button("Refresh Billing Status"):
                updated = sync_user_billing(user)
                set_current_user(updated)
                st.success("Billing status refreshed.")
                st.rerun()
        with right:
            if st.button("Open Billing Portal"):
                try:
                    url = billing_portal_url(user)
                    st.markdown(f"[Open Stripe Billing Portal]({url})")
                except Exception as exc:
                    st.error(f"Billing portal is not available yet: {exc}")
    else:
        st.info("To enable Stripe, set `STRIPE_SECRET_KEY`, `STRIPE_PRICE_ID_STARTER`, `STRIPE_PRICE_ID_PRO`, and `APP_BASE_URL`.")


def page_dashboard():
    st.title(APP_NAME)
    st.subheader(APP_TAGLINE)
    svc = services_df()
    runs = runs_df()
    c1, c2, c3 = st.columns(3)
    c1.metric("Credits Remaining", credits())
    c2.metric("Saved Services", len(svc))
    c3.metric("Saved Lists", len(runs))
    st.write("Save service profiles, generate prospect lists with credits, and keep those lists for later review and export.")
    with st.expander("Credit controls"):
        amount = st.number_input("Add demo credits", min_value=1, max_value=500, value=10, step=1)
        if st.button("Add credits"):
            st.success(f"Credits updated to {add_credits(int(amount))}.")
            st.rerun()
    if runs.empty:
        st.info("No saved lists yet.")
    else:
        st.dataframe(
            pretty_df(
                runs[
                    [
                        "run_name",
                        "services_text",
                        "location_filter",
                        "time_window",
                        "mode",
                        "estimated_search_time",
                        "credits_used",
                        "created_at",
                    ]
                ]
            ),
            use_container_width=True,
        )


def page_services():
    st.title("Service Profiles")
    with st.form("service_form"):
        name = st.text_input("Service name")
        description = st.text_area("Service description", height=180, placeholder="Describe the service, scope, titles, and keywords.")
        location_filter = st.text_input("Default target location", value="Any U.S. location")
        submit = st.form_submit_button("Save service profile")
    if submit:
        if not name.strip() or not description.strip():
            st.error("Please enter both a service name and a service description.")
        else:
            save_service(name, description, location_filter)
            st.success("Service profile saved.")
            st.rerun()
    svc = services_df()
    if svc.empty:
        st.info("No service profiles saved yet.")
    else:
        service_display = svc[["id", "service_name", "target_location", "created_at"]].copy()
        st.dataframe(pretty_df(service_display), use_container_width=True)


def page_generate():
    st.title("Generate List")
    svc = services_df()
    if svc.empty:
        st.info("Create a service profile first.")
        return

    options = {f"{row['id']} - {row['service_name']}": row for _, row in svc.iterrows()}
    selected = st.multiselect("Select saved services", list(options.keys()))
    location_filter = st.text_input("Location filter", value="Any U.S. location")
    time_window = st.selectbox("Time window", TIME_OPTIONS, index=2)
    high_volume = st.checkbox("High volume mode (broader search, more opportunities, lower precision)", value=True)
    credits_needed = len(selected) * (2 if high_volume else 1)
    estimated_range, estimate_basis = estimate_search_time(
        len(selected),
        high_volume,
        time_window,
    )
    basis_text = "based on your recent runs" if estimate_basis == "history" else "based on current settings"
    st.caption(
        f"Credits needed: {credits_needed} | Credits remaining: {credits()} | Estimated search time: {format_duration_range_text(estimated_range[0], estimated_range[1])} ({basis_text})"
    )
    st.caption("Search time can still vary because live web search depends on outside websites and OpenAI response time.")

    if st.button("Generate and save list", type="primary"):
        if not selected:
            st.error("Please select at least one saved service.")
            return
        if credits() < credits_needed:
            st.error("Not enough credits. Add more on the Dashboard.")
            return
        try:
            api_client = client()
            all_records = []
            raw_search_responses = []
            start_time = time.time()
            progress = st.progress(0, text="Starting search...")
            for index, label in enumerate(selected, start=1):
                progress_percent = int(((index - 1) / max(1, len(selected))) * 70)
                progress.progress(
                    progress_percent,
                    text=f"Searching service {index} of {len(selected)}: {options[label]['service_name']}",
                )
                raw_json_list, records = search_service(
                    api_client,
                    options[label],
                    location_filter,
                    time_window,
                    high_volume,
                )
                raw_search_responses.extend(raw_json_list)
                all_records.extend(records)
            evidence_df = pd.DataFrame(all_records)
            if evidence_df.empty:
                progress.empty()
                st.info(f"No matching U.S. results from the last {time_window} were found.")
                return
            evidence_df = ensure_evidence_columns(evidence_df)
            progress.progress(90, text="Ranking and organizing results...")
            evidence_df = evidence_df.sort_values("match_score", ascending=False).reset_index(drop=True)
            company_df = aggregate_companies(evidence_df)
            duration_seconds = time.time() - start_time
            run_id = save_run(
                run_name=f"{datetime.now().strftime('%Y-%m-%d %H:%M')} | {len(selected)} service(s)",
                services_text="; ".join([options[label]["service_name"] for label in selected]),
                service_count=len(selected),
                location_filter=location_filter,
                time_window=time_window,
                high_volume_mode=high_volume,
                enrichment_enabled=False,
                credits_used=credits_needed,
                duration_seconds=duration_seconds,
                company_df=company_df,
                evidence_df=evidence_df,
            )
            remaining = add_credits(-credits_needed)
            progress.progress(100, text="List complete.")
            st.success(
                f"Saved list #{run_id} created in {format_duration_text(duration_seconds)}. Credits remaining: {remaining}"
            )
            show_run(get_run(run_id), f"new_{run_id}")
            with st.expander("Raw search responses"):
                for idx, raw_json in enumerate(raw_search_responses, start=1):
                    st.markdown(f"**Search response {idx}**")
                    st.code(raw_json, language="json")
        except ValueError as exc:
            st.error(str(exc))
        except Exception as exc:
            st.error(f"Something went wrong while searching or calling the OpenAI API: {exc}")


def page_saved_lists():
    st.title("Saved Lists")
    runs = runs_df()
    if runs.empty:
        st.info("No saved lists yet.")
        return

    master_company_df = build_master_saved_data()
    if not master_company_df.empty:
        st.markdown("**Master exports**")
        st.dataframe(pretty_df(master_company_df), use_container_width=True)
        st.download_button(
            "Download master company list as CSV",
            data=csv_data(master_company_df),
            file_name="nextstepsignal_master_company_list.csv",
            mime="text/csv",
            key="master_company_csv",
        )

    display_runs = runs[
        [
            "id",
            "run_name",
            "services_text",
            "location_filter",
            "time_window",
            "mode",
            "estimated_search_time",
            "credits_used",
            "created_at",
        ]
    ].copy()
    st.dataframe(pretty_df(display_runs), use_container_width=True)
    selected_id = st.selectbox(
        "Open saved list",
        options=runs["id"].tolist(),
        format_func=lambda rid: next(
            f"#{row['id']} | {row['run_name']} | {row['services_text']}"
            for _, row in runs.iterrows()
            if row["id"] == rid
        ),
    )
    action_col1, action_col2 = st.columns([1, 4])
    with action_col1:
        if st.button("Delete selected list", key=f"delete_list_{selected_id}"):
            delete_run(selected_id)
            st.success("Saved list deleted.")
            st.rerun()
    show_run(get_run(selected_id), f"saved_{selected_id}")


def page_potential_expansions():
    st.title("Potential Expansions")
    st.write(
        "Select 3 or more saved services to identify adjacent scopes the market appears to be requesting that are not explicitly covered in the current service set."
    )
    svc = services_df()
    if svc.empty:
        st.info("Create service profiles first.")
        return

    options = {f"{row['id']} - {row['service_name']}": row for _, row in svc.iterrows()}
    selected = st.multiselect("Select 3 or more services", list(options.keys()))
    location_filter = st.text_input("Location filter", value="Any U.S. location", key="exp_location")
    time_window = st.selectbox("Time window", TIME_OPTIONS, index=2, key="exp_time")
    high_volume = st.checkbox(
        "High volume mode (broader search, more opportunity signals)",
        value=True,
        key="exp_high_volume",
    )
    credits_needed = len(selected) * (2 if high_volume else 1) + (1 if selected else 0)
    st.caption(
        f"Credits needed: {credits_needed} | Includes market search plus 1 expansion analysis credit | Credits remaining: {credits()}"
    )

    if st.button("Generate expansion ideas", type="primary"):
        if len(selected) < 3:
            st.error("Please select at least 3 saved services.")
            return
        if credits() < credits_needed:
            st.error("Not enough credits. Add more on the Dashboard.")
            return
        try:
            api_client = client()
            selected_rows = pd.DataFrame([options[label] for label in selected])
            all_records = []
            with st.spinner("Searching the market and identifying peripheral service expansions..."):
                for _, row in selected_rows.iterrows():
                    _, records = search_service(
                        api_client,
                        row,
                        location_filter,
                        time_window,
                        high_volume,
                    )
                    all_records.extend(records)

                evidence_df = pd.DataFrame(all_records)
                if evidence_df.empty:
                    st.info(f"No matching U.S. results from the last {time_window} were found.")
                    return

                evidence_df = evidence_df[EVIDENCE_COLUMNS].sort_values(
                    "match_score", ascending=False
                ).reset_index(drop=True)
                raw_json, expansion_df = analyze_expansions(
                    api_client,
                    selected_rows,
                    evidence_df,
                )

            if expansion_df.empty:
                st.info("No clear expansion ideas were found from the current evidence.")
                return

            remaining = add_credits(-credits_needed)
            display_expansion_df = format_lists_for_display(expansion_df)
            st.success(f"Expansion analysis complete. Credits remaining: {remaining}")
            st.dataframe(pretty_df(display_expansion_df), use_container_width=True)

            services_text = "; ".join(selected_rows["service_name"].tolist())
            st.download_button(
                "Download potential expansions as CSV",
                data=csv_data(display_expansion_df),
                file_name="nextstepsignal_potential_expansions.csv",
                mime="text/csv",
            )
            st.download_button(
                "Download potential expansions as PDF",
                data=expansion_pdf_data(
                    display_expansion_df,
                    {
                        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
                        "services_text": services_text,
                        "location_filter": location_filter,
                        "time_window": time_window,
                        "mode": "High volume" if high_volume else "Focused",
                    },
                ),
                file_name="nextstepsignal_potential_expansions.pdf",
                mime="application/pdf",
            )

            with st.expander("Supporting evidence used for expansion analysis"):
                st.dataframe(format_lists_for_display(evidence_df), use_container_width=True)

            with st.expander("Raw expansion JSON"):
                st.code(raw_json, language="json")

        except ValueError as exc:
            st.error(str(exc))
        except Exception as exc:
            st.error(f"Something went wrong while generating potential expansions: {exc}")


init_db()
user = current_user()
if user:
    user = sync_user_billing(user)
    set_current_user(user)

params = st.query_params
if params.get("billing") == "success" and user:
    user = sync_user_billing(user)
    set_current_user(user)
    st.success("Billing completed. Subscription status refreshed.")

if not user:
    page_auth()
else:
    with st.sidebar:
        st.title(APP_NAME)
        st.write(user["full_name"])
        st.write(user["email"])
        st.metric("Credits Remaining", credits(user["id"]))
        st.write(f"Plan: {user.get('plan_name') or 'None'}")
        st.write(f"Status: {user.get('subscription_status', 'inactive').title()}")
        page = st.radio(
            "Navigate",
            ["Dashboard", "Plans & Billing", "Service Profiles", "Generate List", "Saved Lists", "Potential Expansions"],
        )
        if st.button("Sign Out"):
            set_current_user(None)
            st.rerun()

    if page == "Plans & Billing":
        page_billing(user)
    elif not portal_access_allowed(user):
        st.warning("Your account needs an active subscription or available demo credits to use the portal.")
        page_billing(user)
    elif page == "Dashboard":
        page_dashboard()
    elif page == "Service Profiles":
        page_services()
    elif page == "Generate List":
        page_generate()
    elif page == "Potential Expansions":
        page_potential_expansions()
    else:
        page_saved_lists()
