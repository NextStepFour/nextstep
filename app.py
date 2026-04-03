import io
import json
import os
import re
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
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "rgordon@heliovolta.com").strip().lower()
ADMIN_DEMO_CREDITS = int(os.getenv("ADMIN_DEMO_CREDITS", "100"))
TIME_OPTIONS = ["1 week", "2 weeks", "1 month", "2 months", "3 months"]
APP_BASE_URL = os.getenv("APP_BASE_URL", "http://localhost:8501")
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY", "")
DISCOVERY_MODEL = os.getenv("OPENAI_DISCOVERY_MODEL", "gpt-5-mini")
SYNTHESIS_MODEL = os.getenv("OPENAI_SYNTHESIS_MODEL", "gpt-5-mini")
PLANS = {
    "starter": {
        "name": "Starter",
        "price_id": os.getenv("STRIPE_PRICE_ID_STARTER", ""),
        "monthly_credits": 50,
        "monthly_price": "$7",
        "credit_note": "50 credits per month",
        "features": [
            "Buyer company list generation",
            "Save service profiles",
            "Saved lists",
            "CSV and PDF downloads",
        ],
    },
    "pro": {
        "name": "Pro",
        "price_id": os.getenv("STRIPE_PRICE_ID_PRO", ""),
        "monthly_credits": 200,
        "monthly_price": "$15",
        "credit_note": "200 credits per month",
        "features": [
            "Higher monthly search volume",
            "Potential expansions analysis",
            "Saved lists",
            "CSV and PDF downloads",
        ],
    },
}
EVIDENCE_COLUMNS = [
    "matched_service",
    "company_name",
    "job_title",
    "base_salary",
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
    "base_salary",
    "matched_services",
    "likely_buyer_department_general",
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
    "date_generated": "Date Generated",
    "buyer_company": "Buyer Company",
    "job_posting_title": "Job Posting Title",
    "base_salary": "Base Salary",
    "matched_services": "Matched Services",
    "likely_buyer_department_general": "Likely Buyer Department (General)",
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
    "relevant_posting_count": "Relevant Posting Count",
    "most_recent_posted_date": "Most Recent Posting Date",
    "salary_signal": "Salary Signal",
    "why_highlighted": "Why Highlighted",
    "suggested_next_step": "Suggested Next Step",
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


def inject_global_styles():
    st.markdown(
        """
        <style>
        :root {
            --brand-blue: #60a5fa;
            --brand-blue-dark: #4f95ec;
            --brand-blue-soft: rgba(96, 165, 250, 0.14);
            --brand-border: rgba(96, 165, 250, 0.35);
            --panel-border: rgba(255, 255, 255, 0.08);
        }
        .stApp h1, .stApp h2, .stApp h3 {
            color: #dbeafe;
        }
        .stApp a {
            color: var(--brand-blue);
        }
        .stButton > button,
        .stDownloadButton > button,
        .stFormSubmitButton > button {
            background: var(--brand-blue);
            color: #0f172a;
            border: 1px solid var(--brand-blue);
            border-radius: 0.75rem;
            font-weight: 650;
        }
        .stButton > button:hover,
        .stDownloadButton > button:hover,
        .stFormSubmitButton > button:hover {
            background: var(--brand-blue-dark);
            border-color: var(--brand-blue-dark);
            color: #0f172a;
        }
        .stButton > button[kind="secondary"] {
            background: transparent;
            color: #dbeafe;
            border: 1px solid var(--brand-border);
        }
        .stButton > button[kind="secondary"]:hover {
            background: var(--brand-blue-soft);
            color: #e0f2fe;
            border-color: var(--brand-blue);
        }
        [data-baseweb="tab-list"] {
            gap: 0.4rem;
        }
        [data-baseweb="tab"] {
            border-radius: 0.65rem 0.65rem 0 0;
        }
        [data-baseweb="tab"][aria-selected="true"] {
            color: var(--brand-blue) !important;
            border-bottom-color: var(--brand-blue) !important;
        }
        [data-baseweb="select"] > div,
        .stTextInput > div > div > input,
        .stTextArea textarea,
        .stNumberInput input {
            border-color: var(--panel-border);
            border-radius: 0.75rem;
        }
        [data-testid="stMetric"] {
            border: 1px solid var(--panel-border);
            border-radius: 0.95rem;
            padding: 0.9rem 1rem;
            background: rgba(255, 255, 255, 0.02);
        }
        [data-testid="stSidebar"] h1,
        [data-testid="stSidebar"] h2,
        [data-testid="stSidebar"] h3 {
            color: #dbeafe;
        }
        [data-testid="stSidebar"] > div:first-child {
            background: #f8fbff;
        }
        [data-testid="stSidebar"] {
            border-right: 1px solid rgba(148, 163, 184, 0.18);
        }
        [data-testid="stSidebar"] .stMarkdown,
        [data-testid="stSidebar"] p,
        [data-testid="stSidebar"] label {
            color: #0f172a !important;
        }
        [data-testid="stSidebar"] [data-testid="stMetric"] {
            display: none;
        }
        [data-testid="stSidebar"] [data-baseweb="radio"] {
            margin-bottom: 0.48rem;
            border-radius: 0.9rem;
            padding: 0.12rem 0.18rem;
            border: 1px solid rgba(148, 163, 184, 0.18);
            background: #ffffff;
            box-shadow: 0 6px 18px rgba(15, 23, 42, 0.04);
        }
        [data-testid="stSidebar"] [data-baseweb="radio"] > div {
            background: transparent !important;
        }
        [data-testid="stSidebar"] [data-baseweb="radio"] input {
            position: absolute !important;
            opacity: 0 !important;
            pointer-events: none !important;
            width: 0 !important;
            height: 0 !important;
        }
        [data-testid="stSidebar"] [data-baseweb="radio"] svg {
            display: none !important;
        }
        [data-testid="stSidebar"] [data-baseweb="radio"] label,
        [data-testid="stSidebar"] [data-baseweb="radio"] div[role="radio"] {
            width: 100%;
        }
        [data-testid="stSidebar"] [data-baseweb="radio"] label {
            padding: 0.32rem 0.55rem;
            border-radius: 0.75rem;
        }
        [data-testid="stSidebar"] [data-baseweb="radio"]:has(input:checked) {
            background: #4f7cf0;
            box-shadow: 0 10px 22px rgba(79, 124, 240, 0.22);
            border-color: #4f7cf0;
        }
        [data-testid="stSidebar"] [data-baseweb="radio"]:has(input:checked) p,
        [data-testid="stSidebar"] [data-baseweb="radio"]:has(input:checked) label,
        [data-testid="stSidebar"] [data-baseweb="radio"]:has(input:checked) span {
            color: #ffffff !important;
            font-weight: 600;
        }
        [data-testid="stSidebar"] [data-baseweb="radio"] p,
        [data-testid="stSidebar"] [data-baseweb="radio"] span {
            font-size: 0.97rem;
            line-height: 1.2;
        }
        [data-testid="stSidebar"] .sidebar-brand {
            font-size: 1.5rem;
            font-weight: 800;
            color: #111827;
            margin-bottom: 0.2rem;
        }
        [data-testid="stSidebar"] .sidebar-card {
            background: #ffffff;
            border: 1px solid rgba(148, 163, 184, 0.18);
            border-radius: 1rem;
            padding: 1rem 0.95rem;
            box-shadow: 0 10px 28px rgba(15, 23, 42, 0.06);
            margin-bottom: 0.95rem;
        }
        [data-testid="stSidebar"] .sidebar-user-name {
            font-size: 1rem;
            font-weight: 700;
            color: #111827;
            margin-bottom: 0.2rem;
        }
        [data-testid="stSidebar"] .sidebar-user-email {
            font-size: 0.9rem;
            color: #475569;
            margin-bottom: 0.8rem;
            word-break: break-word;
        }
        [data-testid="stSidebar"] .sidebar-mini-grid {
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 0.45rem;
        }
        [data-testid="stSidebar"] .sidebar-mini-item {
            background: #f8fbff;
            border: 1px solid rgba(148, 163, 184, 0.14);
            border-radius: 0.75rem;
            padding: 0.45rem 0.4rem;
            text-align: center;
        }
        [data-testid="stSidebar"] .sidebar-mini-label {
            font-size: 0.68rem;
            font-weight: 700;
            color: #64748b;
            text-transform: uppercase;
            margin-bottom: 0.18rem;
        }
        [data-testid="stSidebar"] .sidebar-mini-value {
            font-size: 0.88rem;
            font-weight: 700;
            color: #111827;
            line-height: 1.15;
        }
        [data-testid="stSidebar"] button[kind="secondary"],
        [data-testid="stSidebar"] button[kind="primary"] {
            width: 100%;
            border-radius: 0.8rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


inject_global_styles()


def auth_space_scene_html():
    scene_rows = [
        "............................",
        "...*.......+......*.....+...",
        "........*..............*....",
        "......*......mm.............",
        ".............mmm......*.....",
        "............mmmmm...........",
        "..+..........mmm.......+....",
        "..............m.............",
        ".................*..........",
        "............rr..............",
        "...........rrrr.............",
        "...........rwwr......*......",
        "..........rrwwrr............",
        "...........rrrr.............",
        "............rr..............",
        "............ff..............",
        "...........fssf.............",
        "bbbbbbbbbbssssssbbbbbbbbbbbb",
        "bbbbbbbbssssssssssbbbbbbbbbb",
        "bbbbbbssssssssssssssbbbbbbbb",
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    ]
    color_map = {
        ".": "#140b34",
        "*": "#f8fafc",
        "+": "#7dd3fc",
        "m": "#f9a8d4",
        "r": "#fb923c",
        "w": "#dbeafe",
        "f": "#fde047",
        "s": "#e2e8f0",
        "b": "#1d4ed8",
    }

    cells = []
    for row in scene_rows:
        for char in row:
            color = color_map.get(char, color_map["."])
            cells.append(f'<span class="space-pixel" style="background:{color};"></span>')

    return (
        '<div class="auth-hero">'
        f'<div class="space-pixel-scene">{"".join(cells)}</div>'
        "</div>"
    )


PROMPT_TEMPLATE = """You are a market intelligence engine for solar service sales.

Your task is to search the public web for U.S. solar job postings, recently filled roles, RFPs, and similar opportunities from the last {{TIME_WINDOW}} that overlap with the service description below.

Search broadly across public websites, structured company careers pages, ATS-hosted job pages, major public job boards, and RFP/procurement pages.

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
      "base_salary": null,
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
- Prefer official company career pages and structured ATS/job board pages over reposts or weak aggregators
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
- base_salary should include only explicit base salary from the posting or source page
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
                    "base_salary": {"type": ["string", "null"]},
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
                    "base_salary",
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

COMPANY_DEEP_DIVE_PROMPT_TEMPLATE = """You are a market intelligence engine for service sales pursuit strategy.

Your task is to search the public web for additional U.S. job postings from a single company that may help explain broader hiring demand around a service opportunity.

Target company:
{{COMPANY_NAME}}

Service context:
{{MATCHED_SERVICES}}

Known posting hints from earlier analysis:
{{KNOWN_POSTINGS}}

Location hints already seen:
{{LOCATION_HINTS}}

Search public sources from the last 3 months. Prioritize official employer career pages, structured ATS pages, and publicly visible structured job board pages.

Return valid JSON only using this schema:
{
  "results": [
    {
      "company_name": null,
      "job_title": null,
      "base_salary": null,
      "location": null,
      "source_type": null,
      "opportunity_status": "Open|Filled|Unknown",
      "posted_date": null,
      "relevance_bucket": "Directly relevant|Adjacent|Broader company context",
      "why_it_matters": null,
      "source_url": null
    }
  ]
}

Rules:
- Search the internet first before answering
- Only include public web results you actually found
- Only include results from the same company
- Only include U.S. results
- Only include results that appear to be from the last 3 months
- Include up to 12 results when available
- Use the service context to decide whether a role is Directly relevant, Adjacent, or Broader company context
- Directly relevant = clearly overlaps with the service context
- Adjacent = nearby function that supports or surrounds the service context
- Broader company context = not a direct fit, but still helps explain organizational hiring around the opportunity
- base_salary should include only explicit base salary from the posting or source page
- If a field is unknown, return null
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

COMPANY_DEEP_DIVE_SCHEMA = {
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
                    "base_salary": {"type": ["string", "null"]},
                    "location": {"type": ["string", "null"]},
                    "source_type": {"type": ["string", "null"]},
                    "opportunity_status": {
                        "type": "string",
                        "enum": ["Open", "Filled", "Unknown"],
                    },
                    "posted_date": {"type": ["string", "null"]},
                    "relevance_bucket": {
                        "type": "string",
                        "enum": ["Directly relevant", "Adjacent", "Broader company context"],
                    },
                    "why_it_matters": {"type": ["string", "null"]},
                    "source_url": {"type": ["string", "null"]},
                },
                "required": [
                    "company_name",
                    "job_title",
                    "base_salary",
                    "location",
                    "source_type",
                    "opportunity_status",
                    "posted_date",
                    "relevance_bucket",
                    "why_it_matters",
                    "source_url",
                ],
            },
        }
    },
    "required": ["results"],
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
                is_admin INTEGER NOT NULL DEFAULT 0,
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
        user_columns = [row["name"] for row in db.execute("PRAGMA table_info(users)").fetchall()]
        if "is_admin" not in user_columns:
            db.execute("ALTER TABLE users ADD COLUMN is_admin INTEGER NOT NULL DEFAULT 0")
        db.execute(
            "UPDATE users SET is_admin = 1 WHERE lower(email) = ?",
            (ADMIN_EMAIL,),
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


def is_admin_user(user):
    if not user:
        return False
    return bool(int(user.get("is_admin") or 0))


def create_user(full_name, email, password):
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    normalized_email = email.strip().lower()
    wants_admin_email = normalized_email == ADMIN_EMAIL
    is_admin = 1 if wants_admin_email else 0
    starting_credits = ADMIN_DEMO_CREDITS if is_admin else 0
    with conn() as db:
        cursor = db.execute(
            """
            INSERT INTO users (
                full_name, email, password_hash, is_admin, subscription_status,
                plan_name, monthly_credit_allowance, credit_balance, created_at
            ) VALUES (?, ?, ?, ?, 'inactive', null, 0, ?, ?)
            """,
            (
                full_name.strip(),
                normalized_email,
                hash_password(password),
                is_admin,
                starting_credits,
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


def users_df():
    with conn() as db:
        rows = db.execute(
            """
            SELECT id, full_name, email, is_admin, subscription_status, plan_name,
                   credit_balance, monthly_credit_allowance, created_at
            FROM users
            ORDER BY created_at DESC, id DESC
            """
        ).fetchall()
    return pd.DataFrame([dict(r) for r in rows])


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
        "1 week": 0,
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


def get_cached_checkout_url(user, plan_key):
    cache_key = f"checkout_url_{user['id']}_{plan_key}"
    cached = st.session_state.get(cache_key)
    if cached:
        return cached
    url = checkout_url_for_plan(user, plan_key)
    st.session_state[cache_key] = url
    return url


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
            "Search across official company career pages, structured ATS pages such as Greenhouse, Lever, Workday, iCIMS, SmartRecruiters, and Ashby, plus public job board pages on LinkedIn, Indeed, Glassdoor, Built In, and similar sites when the postings are publicly visible. Also check public procurement or RFP pages."
            if high_volume_mode
            else "Prioritize official company career pages, public ATS pages, and publicly visible structured job board pages before weaker reposts or aggregators."
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
    variants = [
        base_description,
        (
            f"{service_name}\n\nFocus on official employer career pages, structured ATS-hosted job pages, and other public structured posting sources tied to this service."
            if service_name
            else f"{base_description}\n\nFocus on official employer career pages, structured ATS-hosted job pages, and other public structured posting sources."
        ),
    ]
    if high_volume_mode:
        variants.append(
            f"{service_name}\n\nExpand recall using public structured job board pages such as LinkedIn, Indeed, Glassdoor, Built In, and official ATS/careers pages. Derive role-title variants and adjacent responsibilities only from this service description."
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


def build_company_deep_dive_prompt(company_name, matched_services_text, company_evidence_df):
    known_titles = flatten_unique(company_evidence_df["job_title"].tolist())[:6]
    location_hints = flatten_unique(company_evidence_df["location"].tolist())[:5]
    prompt = COMPANY_DEEP_DIVE_PROMPT_TEMPLATE
    prompt = prompt.replace("{{COMPANY_NAME}}", safe_text(company_name, "Unknown Company"))
    prompt = prompt.replace("{{MATCHED_SERVICES}}", matched_services_text or "No matched services recorded")
    prompt = prompt.replace(
        "{{KNOWN_POSTINGS}}",
        "\n".join(f"- {title}" for title in known_titles) if known_titles else "- No known postings captured",
    )
    prompt = prompt.replace(
        "{{LOCATION_HINTS}}",
        "; ".join(location_hints) if location_hints else "No location hints captured",
    )
    return prompt


def normalize_company_deep_dive_record(item, company_name):
    row = dict(item)
    row["company_name"] = row.get("company_name") or company_name
    for column in [
        "company_name",
        "job_title",
        "base_salary",
        "location",
        "source_type",
        "opportunity_status",
        "posted_date",
        "relevance_bucket",
        "why_it_matters",
        "source_url",
    ]:
        row.setdefault(column, None)
    return row


def dedupe_company_deep_dive_records(records):
    deduped = {}
    for row in records:
        key = (
            safe_text(row.get("source_url")),
            safe_text(row.get("company_name")),
            safe_text(row.get("job_title")),
        )
        if key not in deduped:
            deduped[key] = row
    return list(deduped.values())


def search_company_deep_dive(api_client, company_name, matched_services_text, company_evidence_df):
    response = api_client.responses.create(
        model=DISCOVERY_MODEL,
        reasoning={"effort": "low"},
        tools=[
            {
                "type": "web_search",
                "user_location": {"type": "approximate", "country": "US", "timezone": "America/New_York"},
            }
        ],
        tool_choice="auto",
        include=["web_search_call.action.sources"],
        input=build_company_deep_dive_prompt(company_name, matched_services_text, company_evidence_df),
        text={
            "format": {
                "type": "json_schema",
                "name": "nextstep_company_deep_dive",
                "strict": True,
                "schema": COMPANY_DEEP_DIVE_SCHEMA,
            }
        },
    )
    raw_json = response.output_text if getattr(response, "output_text", None) else ""
    try:
        parsed = json.loads(raw_json)
    except json.JSONDecodeError as exc:
        raise ValueError("The API returned invalid JSON for the company deep-dive search.") from exc

    records = [
        normalize_company_deep_dive_record(item, company_name)
        for item in parsed.get("results", [])
    ]
    df = pd.DataFrame(dedupe_company_deep_dive_records(records))
    if df.empty:
        return raw_json, df

    existing_keys = {
        (
            safe_text(row.get("source_url")),
            safe_text(row.get("company_name")),
            safe_text(row.get("job_title")),
        )
        for _, row in company_evidence_df.iterrows()
    }
    df = df[
        ~df.apply(
            lambda row: (
                safe_text(row.get("source_url")),
                safe_text(row.get("company_name")),
                safe_text(row.get("job_title")),
            )
            in existing_keys,
            axis=1,
        )
    ].copy()
    if df.empty:
        return raw_json, df

    df["posted_date_parsed"] = pd.to_datetime(df["posted_date"], errors="coerce")
    return raw_json, df.sort_values(
        ["posted_date_parsed", "job_title"],
        ascending=[False, True],
    ).drop(columns=["posted_date_parsed"]).reset_index(drop=True)


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


def split_service_values(value):
    if pd.isna(value):
        return []
    if isinstance(value, list):
        items = value
    else:
        items = str(value).split(";")
    return [item.strip() for item in items if str(item).strip()]


def ensure_company_columns(company_df):
    working = company_df.copy()
    for column in COMPANY_COLUMNS:
        if column not in working.columns:
            working[column] = None
    return working[COMPANY_COLUMNS]


def aggregate_companies(evidence_df):
    if evidence_df.empty:
        return pd.DataFrame(columns=COMPANY_COLUMNS)

    temp = evidence_df.copy()
    temp["posted_date_parsed"] = pd.to_datetime(temp["posted_date"], errors="coerce")
    rows = []
    for (company, job_title), group in temp.groupby(["company_name", "job_title"], dropna=False):
        best = group.sort_values("match_score", ascending=False).iloc[0]
        matched_services = flatten_unique(group["matched_service"].tolist())
        urls = flatten_unique(group["source_url"].tolist())[:5]
        salaries = flatten_unique(group["base_salary"].tolist())
        likely_buyer_department = (
            pd.Series([x for x in group["buyer_department"] if pd.notna(x) and str(x).strip()]).mode().iloc[0]
            if any(pd.notna(group["buyer_department"]))
            else None
        )
        rows.append(
            {
                "buyer_company": safe_text(company, "Unknown Company"),
                "job_posting_title": safe_text(job_title) or safe_text(best["job_title"]),
                "base_salary": salaries[0] if salaries else None,
                "matched_services": "; ".join(matched_services),
                "likely_buyer_department_general": likely_buyer_department,
                "source_urls": " | ".join(urls),
            }
        )
    df = pd.DataFrame(rows)
    return df[COMPANY_COLUMNS].sort_values(["buyer_company", "job_posting_title"], ascending=[True, True]).reset_index(drop=True)


def merge_company_lists(company_df):
    if company_df.empty:
        return pd.DataFrame(columns=COMPANY_COLUMNS)

    temp = ensure_company_columns(company_df)
    rows = []
    for (buyer_company, job_posting_title), group in temp.groupby(["buyer_company", "job_posting_title"], dropna=False):
        best = group.iloc[0]
        matched_services = flatten_unique(group["matched_services"].tolist())
        source_urls = flatten_unique(group["source_urls"].tolist())[:5]
        salaries = flatten_unique(group["base_salary"].tolist())
        likely_buyer_department = (
            pd.Series([x for x in group["likely_buyer_department_general"] if pd.notna(x) and str(x).strip()]).mode().iloc[0]
            if any(pd.notna(group["likely_buyer_department_general"]))
            else None
        )
        rows.append(
            {
                "buyer_company": safe_text(buyer_company, "Unknown Company"),
                "job_posting_title": safe_text(job_posting_title) or safe_text(best["job_posting_title"]),
                "base_salary": salaries[0] if salaries else None,
                "matched_services": "; ".join(matched_services),
                "likely_buyer_department_general": likely_buyer_department,
                "source_urls": " | ".join(source_urls),
            }
        )

    return pd.DataFrame(rows)[COMPANY_COLUMNS].sort_values(
        ["buyer_company", "job_posting_title"],
        ascending=[True, True],
    ).reset_index(drop=True)


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


def format_short_date(value):
    try:
        return pd.to_datetime(value, errors="coerce").strftime("%m/%d/%y")
    except Exception:
        return ""


def build_master_evidence_data():
    runs = runs_df()
    if runs.empty:
        return pd.DataFrame(columns=EVIDENCE_COLUMNS)

    evidence_frames = []
    for _, run_row in runs.iterrows():
        run_record = get_run(run_row["id"])
        if not run_record:
            continue
        evidence_df = ensure_evidence_columns(load_df(run_record["evidence_json"]))
        if evidence_df.empty:
            continue
        evidence_df["source_run_id"] = run_record["id"]
        evidence_df["source_run_created_at"] = run_record["created_at"]
        evidence_df["source_services"] = run_record["services_text"]
        evidence_frames.append(evidence_df)

    if not evidence_frames:
        return pd.DataFrame(columns=EVIDENCE_COLUMNS)

    master_evidence_df = pd.concat(evidence_frames, ignore_index=True)
    master_evidence_df = master_evidence_df.drop_duplicates(
        subset=["source_url", "company_name", "job_title", "matched_service"],
        keep="first",
    ).reset_index(drop=True)
    return master_evidence_df


def parse_salary_high_value(salary_text):
    if not salary_text or pd.isna(salary_text):
        return None
    cleaned = str(salary_text).replace(",", "")
    matches = re.findall(r"\$?\s*(\d+(?:\.\d+)?)\s*([kK]?)", cleaned)
    values = []
    for raw_number, has_k in matches:
        try:
            number = float(raw_number)
        except ValueError:
            continue
        if has_k:
            number *= 1000
        values.append(number)
    if not values:
        return None
    return max(values)


def build_next_steps_company_table(evidence_df):
    if evidence_df.empty:
        return pd.DataFrame()

    temp = ensure_evidence_columns(evidence_df).copy()
    temp["posted_date_parsed"] = pd.to_datetime(temp["posted_date"], errors="coerce")
    rows = []
    now_ts = pd.Timestamp.now().normalize()

    for company, group in temp.groupby("company_name", dropna=False):
        job_rows = group.drop_duplicates(subset=["source_url", "job_title"], keep="first").copy()
        posting_count = len(job_rows)
        if posting_count == 0:
            continue

        matched_services = flatten_unique(group["matched_service"].tolist())
        likely_buyer_department = (
            pd.Series([x for x in group["buyer_department"] if pd.notna(x) and str(x).strip()]).mode().iloc[0]
            if any(pd.notna(group["buyer_department"]))
            else None
        )
        source_urls = flatten_unique(group["source_url"].tolist())[:5]
        salary_values = flatten_unique(group["base_salary"].tolist())
        salary_numeric_values = [value for value in [parse_salary_high_value(s) for s in salary_values] if value is not None]
        salary_signal = salary_values[0] if salary_values else None

        recent_dates = job_rows["posted_date_parsed"].dropna()
        most_recent_posted = recent_dates.max() if not recent_dates.empty else pd.NaT
        if pd.notna(most_recent_posted):
            age_days = max(0, (now_ts - most_recent_posted.normalize()).days)
            if age_days <= 7:
                recency_points = 35
            elif age_days <= 14:
                recency_points = 28
            elif age_days <= 30:
                recency_points = 20
            elif age_days <= 60:
                recency_points = 12
            else:
                recency_points = 6
            most_recent_posted_text = most_recent_posted.strftime("%m/%d/%y")
        else:
            recency_points = 0
            most_recent_posted_text = "Unknown"

        posting_points = min(posting_count, 5) * 14
        service_points = min(len(matched_services), 3) * 5
        salary_points = 0
        if salary_values:
            salary_points += 8
        highest_salary = max(salary_numeric_values) if salary_numeric_values else None
        if highest_salary is not None:
            if highest_salary >= 150000:
                salary_points += 10
            elif highest_salary >= 100000:
                salary_points += 7
            elif highest_salary >= 70000:
                salary_points += 4
            else:
                salary_points += 2

        priority_score = posting_points + recency_points + salary_points + service_points

        why_parts = [
            f"{posting_count} relevant posting{'s' if posting_count != 1 else ''} found",
        ]
        if most_recent_posted_text != "Unknown":
            why_parts.append(f"most recent posting dated {most_recent_posted_text}")
        if salary_signal:
            why_parts.append(f"explicit base salary disclosed ({salary_signal})")

        suggested_next_step = (
            f"Prioritize outreach to the {likely_buyer_department} team and reference the matching postings."
            if likely_buyer_department
            else "Prioritize outreach to the team responsible for this function and reference the matching postings."
        )

        rows.append(
            {
                "buyer_company": safe_text(company, "Unknown Company"),
                "relevant_posting_count": posting_count,
                "most_recent_posted_date": most_recent_posted_text,
                "salary_signal": salary_signal or "Not disclosed",
                "matched_services": "; ".join(matched_services),
                "likely_buyer_department_general": likely_buyer_department,
                "why_highlighted": ". ".join(why_parts) + ".",
                "suggested_next_step": suggested_next_step,
                "source_urls": " | ".join(source_urls),
                "_priority_score": priority_score,
            }
        )

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows).sort_values(
        ["_priority_score", "relevant_posting_count", "buyer_company"],
        ascending=[False, False, True],
    ).reset_index(drop=True)


def build_next_steps_summary(top_df, all_df):
    total_companies = len(all_df)
    multiple_postings = int((all_df["relevant_posting_count"] >= 2).sum()) if not all_df.empty else 0
    salary_disclosed = int((all_df["salary_signal"] != "Not disclosed").sum()) if not all_df.empty else 0
    freshest_date = (
        next((value for value in all_df["most_recent_posted_date"].tolist() if value and value != "Unknown"), "Unknown")
        if not all_df.empty
        else "Unknown"
    )

    lines = [
        f"Companies reviewed: {total_companies}.",
        f"Companies with multiple relevant postings: {multiple_postings}.",
        f"Companies with explicit base salary disclosed: {salary_disclosed}.",
        f"Freshest posting date observed: {freshest_date}.",
    ]

    for _, row in top_df.iterrows():
        lines.append(
            (
                f"{row['buyer_company']}: {row['relevant_posting_count']} relevant posting"
                f"{'s' if row['relevant_posting_count'] != 1 else ''}; "
                f"most recent posting date {row['most_recent_posted_date']}; "
                f"salary signal {row['salary_signal']}; "
                f"matched services {row['matched_services'] or 'None listed'}."
            )
        )

    return lines


def build_next_steps_takeaways(top_df, all_df):
    takeaways = []
    if all_df.empty:
        return takeaways

    multiple_postings = int((all_df["relevant_posting_count"] >= 2).sum())
    salary_disclosed = int((all_df["salary_signal"] != "Not disclosed").sum())
    top_company = safe_text(top_df.iloc[0]["buyer_company"]) if not top_df.empty else "Unknown"
    top_company_count = int(top_df.iloc[0]["relevant_posting_count"]) if not top_df.empty else 0

    takeaways.append(
        f"{multiple_postings} compan{'ies' if multiple_postings != 1 else 'y'} show repeated relevant hiring signals, which is the strongest indicator of non-isolated demand."
    )
    takeaways.append(
        f"{salary_disclosed} compan{'ies' if salary_disclosed != 1 else 'y'} disclosed explicit base salary, so compensation is a supporting signal rather than the main ranking driver in this dataset."
    )
    if top_company:
        takeaways.append(
            f"{top_company} ranks at the top of the current review based on the strongest combined signal set, including {top_company_count} relevant posting{'s' if top_company_count != 1 else ''} and recent activity."
        )

    matched_service_counts = {}
    for value in all_df["matched_services"].fillna(""):
        for service in split_service_values(value):
            matched_service_counts[service] = matched_service_counts.get(service, 0) + 1
    if matched_service_counts:
        top_service = sorted(matched_service_counts.items(), key=lambda item: (-item[1], item[0]))[0][0]
        takeaways.append(
            f"The most common matched service signal in this review is {top_service}, which is showing up across multiple buyer-company results."
        )

    return takeaways[:4]


def build_company_next_steps_description(company_row, company_evidence_df):
    salary_disclosed_count = int(company_evidence_df["base_salary"].fillna("").astype(str).str.strip().ne("").sum())
    description_parts = [
        f"{company_row['relevant_posting_count']} tracked posting{'s' if company_row['relevant_posting_count'] != 1 else ''}",
    ]
    if company_row["most_recent_posted_date"] != "Unknown":
        description_parts.append(f"most recent posting date {company_row['most_recent_posted_date']}")
    if salary_disclosed_count:
        description_parts.append(f"{salary_disclosed_count} posting{'s' if salary_disclosed_count != 1 else ''} with explicit base salary")
    if company_row["likely_buyer_department_general"]:
        description_parts.append(f"likely buyer department {company_row['likely_buyer_department_general']}")
    if company_row["matched_services"]:
        description_parts.append(f"matched services {company_row['matched_services']}")
    return ". ".join(description_parts) + "."


def build_company_business_description(company_name, company_evidence_df):
    source_types = flatten_unique(company_evidence_df["source_type"].tolist())
    departments = flatten_unique(company_evidence_df["buyer_department"].tolist())
    services = flatten_unique(company_evidence_df["matched_service"].tolist())
    titles = flatten_unique(company_evidence_df["job_title"].tolist())
    lifecycle_signals = []

    title_text = " ".join(titles).lower()
    if any(keyword in title_text for keyword in ["commission", "startup", "mechanical completion"]):
        lifecycle_signals.append("commissioning and readiness work")
    if any(keyword in title_text for keyword in ["field", "site", "superintendent", "construction"]):
        lifecycle_signals.append("field and project execution")
    if any(keyword in title_text for keyword in ["operations", "asset", "performance", "maintenance", "technician"]):
        lifecycle_signals.append("operations and long-term asset support")
    if any(keyword in title_text for keyword in ["quality", "qa", "qc", "inspection"]):
        lifecycle_signals.append("quality and inspection-related activity")
    if any(keyword in title_text for keyword in ["engineering", "engineer", "controls", "scada"]):
        lifecycle_signals.append("engineering and technical support")

    lifecycle_signals = flatten_unique(lifecycle_signals)
    dept_text = ", ".join(departments[:3]) if departments else "multiple operational teams"
    service_text = ", ".join(services[:3]) if services else "the selected service scope"
    source_text = ", ".join(source_types[:2]).lower() if source_types else "public hiring sources"

    if lifecycle_signals:
        lifecycle_text = ", ".join(lifecycle_signals[:3])
        return (
            f"{safe_text(company_name, 'This company')} appears active across {lifecycle_text}. "
            f"Public hiring signals from {source_text} suggest current demand linked to {service_text}, "
            f"with relevant roles connected to {dept_text}."
        )

    return (
        f"{safe_text(company_name, 'This company')} shows public hiring activity connected to {service_text}. "
        f"The roles captured in this report suggest active demand across {dept_text}, based on the job postings found."
    )


def render_next_steps_job_block(job_row):
    why_matches = flatten_unique(job_row.get("why_it_matches", []))
    why_text = "; ".join(why_matches) if why_matches else "No additional evidence notes captured."
    responsibilities = flatten_unique(job_row.get("matching_responsibilities", []))
    responsibilities_text = "; ".join(responsibilities) if responsibilities else "None captured."
    source_url = safe_text(job_row.get("source_url"))
    source_line = f"[Open source posting]({source_url})" if source_url else "No source URL saved."

    st.markdown(
        (
            '<div style="border:1px solid rgba(255,255,255,0.08); border-radius:0.9rem; padding:0.85rem 0.95rem; '
            'background:rgba(255,255,255,0.02); margin-bottom:0.7rem;">'
            f'<div style="font-size:1rem; font-weight:700; color:#eff6ff; margin-bottom:0.35rem;">{escape(safe_text(job_row.get("job_title"), "Unknown job title"))}</div>'
            f'<div style="color:#cbd5e1; margin-bottom:0.2rem;"><strong>Base Salary:</strong> {escape(safe_text(job_row.get("base_salary"), "Not disclosed"))}</div>'
            f'<div style="color:#cbd5e1; margin-bottom:0.2rem;"><strong>Posted Date:</strong> {escape(safe_text(job_row.get("posted_date"), "Unknown"))}</div>'
            f'<div style="color:#cbd5e1; margin-bottom:0.2rem;"><strong>Match Type:</strong> {escape(safe_text(job_row.get("match_type"), "Unknown"))}</div>'
            f'<div style="color:#cbd5e1; margin-bottom:0.2rem;"><strong>Likely Service Need:</strong> {escape(safe_text(job_row.get("likely_service_need"), "Not specified"))}</div>'
            f'<div style="color:#cbd5e1; margin-bottom:0.2rem;"><strong>Evidence:</strong> {escape(why_text)}</div>'
            f'<div style="color:#cbd5e1; margin-bottom:0.2rem;"><strong>Relevant Responsibilities:</strong> {escape(responsibilities_text)}</div>'
            '</div>'
        ),
        unsafe_allow_html=True,
    )
    st.markdown(source_line)


def render_company_deep_dive_job_block(job_row):
    source_url = safe_text(job_row.get("source_url"))
    source_line = f"[Open source posting]({source_url})" if source_url else "No source URL saved."
    st.markdown(
        (
            '<div style="border:1px solid rgba(255,255,255,0.08); border-radius:0.9rem; padding:0.85rem 0.95rem; '
            'background:rgba(255,255,255,0.02); margin-bottom:0.7rem;">'
            f'<div style="font-size:1rem; font-weight:700; color:#eff6ff; margin-bottom:0.35rem;">{escape(safe_text(job_row.get("job_title"), "Unknown job title"))}</div>'
            f'<div style="color:#cbd5e1; margin-bottom:0.2rem;"><strong>Relevance:</strong> {escape(safe_text(job_row.get("relevance_bucket"), "Unknown"))}</div>'
            f'<div style="color:#cbd5e1; margin-bottom:0.2rem;"><strong>Base Salary:</strong> {escape(safe_text(job_row.get("base_salary"), "Not disclosed"))}</div>'
            f'<div style="color:#cbd5e1; margin-bottom:0.2rem;"><strong>Posted Date:</strong> {escape(safe_text(job_row.get("posted_date"), "Unknown"))}</div>'
            f'<div style="color:#cbd5e1; margin-bottom:0.2rem;"><strong>Location:</strong> {escape(safe_text(job_row.get("location"), "Unknown"))}</div>'
            f'<div style="color:#cbd5e1; margin-bottom:0.2rem;"><strong>Why It Matters:</strong> {escape(safe_text(job_row.get("why_it_matters"), "No additional context captured."))}</div>'
            '</div>'
        ),
        unsafe_allow_html=True,
    )
    st.markdown(source_line)


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
                Paragraph(escape(f"Base salary: {row['base_salary'] or 'Unknown'}"), styles["Normal"]),
                Paragraph(escape(f"Matched services: {row['matched_services']}"), styles["Normal"]),
                Paragraph(escape(f"Likely buyer department: {row['likely_buyer_department_general'] or 'Unknown'}"), styles["Normal"]),
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
    company_df = ensure_company_columns(load_df(run_record["company_json"]))
    if company_df.empty:
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

    company_frames = []
    for _, run_row in runs.iterrows():
        run_record = get_run(run_row["id"])
        if not run_record:
            continue
        company_df = ensure_company_columns(load_df(run_record["company_json"]))
        if company_df.empty:
            evidence_df = ensure_evidence_columns(load_df(run_record["evidence_json"]))
            if evidence_df.empty:
                continue
            company_df = aggregate_companies(evidence_df)
        if company_df.empty:
            continue
        company_df["date_generated"] = format_short_date(run_record["created_at"])
        company_df["source_run_id"] = run_record["id"]
        company_df["source_run_name"] = run_record["run_name"]
        company_df["source_run_created_at"] = run_record["created_at"]
        company_df["source_services"] = run_record["services_text"]
        company_frames.append(company_df)

    if not company_frames:
        return pd.DataFrame()

    master_company_df = pd.concat(company_frames, ignore_index=True)
    master_company_df["_date_generated_sort"] = pd.to_datetime(
        master_company_df["source_run_created_at"],
        errors="coerce",
    )
    visible_columns = ["date_generated"] + COMPANY_COLUMNS
    for column in visible_columns:
        if column not in master_company_df.columns:
            master_company_df[column] = None
    return master_company_df.sort_values(
        ["_date_generated_sort", "buyer_company", "job_posting_title"],
        ascending=[False, True, True],
    ).reset_index(drop=True)[visible_columns]


def portal_access_allowed(user):
    if not user:
        return False
    return user.get("subscription_status") in {"active", "trialing"} or int(user.get("credit_balance") or 0) > 0


def page_auth():
    st.markdown(
        """
        <style>
        .auth-brand {
            font-size: 2.9rem;
            line-height: 1;
            font-weight: 800;
            color: #eff6ff;
            margin-bottom: 0.65rem;
        }
        .auth-copy {
            color: #cbd5e1;
            margin-bottom: 1rem;
            font-size: 1rem;
        }
        .auth-left {
            padding-right: 1.2rem;
        }
        .auth-right {
            min-height: 100%;
        }
        .auth-hero {
            position: relative;
            min-height: 760px;
            width: 100%;
            overflow: hidden;
            border-radius: 1.35rem;
            background:
                linear-gradient(180deg, #120834 0%, #1d145d 48%, #1d4ed8 100%);
        }
        .auth-hero::before {
            content: "";
            position: absolute;
            inset: 0;
            background:
                linear-gradient(90deg, rgba(15, 23, 42, 0.96) 0%, rgba(15, 23, 42, 0.70) 18%, rgba(15, 23, 42, 0.18) 42%, rgba(15, 23, 42, 0.00) 62%);
            z-index: 2;
            pointer-events: none;
        }
        .space-pixel-scene {
            position: absolute;
            inset: 0;
            display: grid;
            grid-template-columns: repeat(28, 12px);
            gap: 0;
            place-content: center;
            image-rendering: pixelated;
            transform: scale(1.9);
            transform-origin: center center;
        }
        .space-pixel {
            width: 12px;
            height: 12px;
            display: block;
        }
        @media (max-width: 1200px) {
            .auth-left {
                padding-right: 0;
            }
            .auth-hero {
                min-height: 520px;
                margin-top: 1rem;
            }
            .space-pixel-scene {
                grid-template-columns: repeat(28, 10px);
                transform: scale(1.45);
            }
            .space-pixel {
                width: 10px;
                height: 10px;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    left, right = st.columns([1.02, 1.18], gap="large")

    with left:
        st.markdown('<div class="auth-left">', unsafe_allow_html=True)
        st.markdown(f'<div class="auth-brand">{APP_NAME}</div>', unsafe_allow_html=True)
        st.subheader(APP_TAGLINE)
        st.markdown(
            '<div class="auth-copy">Create an account to save services, generate prospect lists, and manage subscription access.</div>',
            unsafe_allow_html=True,
        )
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
                    if user.get("email", "").strip().lower() == ADMIN_EMAIL:
                        update_user_fields(
                            user["id"],
                            is_admin=1,
                            credit_balance=max(int(user.get("credit_balance") or 0), ADMIN_DEMO_CREDITS),
                        )
                        user = get_user_by_id(user["id"])
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
                    try:
                        user = create_user(full_name, email, password)
                        set_current_user(user)
                        st.success("Account created. You can use starter demo credits or subscribe below.")
                        st.rerun()
                    except ValueError as exc:
                        st.error(str(exc))
        st.markdown("</div>", unsafe_allow_html=True)

    with right:
        st.markdown('<div class="auth-right">', unsafe_allow_html=True)
        st.markdown(auth_space_scene_html(), unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)


def page_billing(user):
    st.title("Plans & Billing")
    st.markdown(
        '<div style="display:inline-block; background: rgba(96, 165, 250, 0.18); color: #dbeafe; padding: 0.45rem 0.65rem; border-radius: 0.45rem; font-weight: 600; margin-bottom: 1rem;">Choose a monthly credit plan for buyer-company market intelligence.</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        """
        <style>
        .billing-summary-grid {
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 0.75rem;
            margin: 0.25rem 0 1rem 0;
        }
        .billing-summary-card {
            border: 1px solid var(--panel-border);
            border-radius: 0.9rem;
            padding: 0.8rem 0.9rem;
            background: rgba(255, 255, 255, 0.02);
        }
        .billing-summary-label {
            font-size: 0.82rem;
            color: #93c5fd;
            margin-bottom: 0.25rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.02em;
        }
        .billing-summary-value {
            font-size: 1.35rem;
            font-weight: 700;
            color: #eff6ff;
            line-height: 1.15;
        }
        @media (max-width: 1100px) {
            .billing-summary-grid {
                grid-template-columns: 1fr;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(
        (
            '<div class="billing-summary-grid">'
            f'<div class="billing-summary-card"><div class="billing-summary-label">Credits Remaining</div><div class="billing-summary-value">{credits(user["id"])}</div></div>'
            f'<div class="billing-summary-card"><div class="billing-summary-label">Subscription Status</div><div class="billing-summary-value">{escape(user.get("subscription_status", "inactive").title())}</div></div>'
            f'<div class="billing-summary-card"><div class="billing-summary-label">Current Plan</div><div class="billing-summary-value">{escape(user.get("plan_name") or "None")}</div></div>'
            '</div>'
        ),
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <style>
        .pricing-card {
            border: 1px solid var(--brand-border);
            border-radius: 1rem;
            padding: 1.2rem 1.15rem 1rem 1.15rem;
            background: rgba(96, 165, 250, 0.06);
            min-height: 100%;
        }
        .pricing-plan-name {
            font-size: 1.15rem;
            font-weight: 700;
            margin-bottom: 0.4rem;
        }
        .pricing-price {
            font-size: 3rem;
            line-height: 1;
            font-weight: 800;
            color: var(--brand-blue);
            margin-bottom: 0.1rem;
        }
        .pricing-period {
            font-size: 1.05rem;
            font-weight: 600;
            margin-bottom: 0.9rem;
        }
        .pricing-credits {
            font-size: 1.05rem;
            font-weight: 700;
            margin-bottom: 0.55rem;
        }
        .pricing-note {
            font-size: 0.95rem;
            color: #cbd5e1;
            margin-bottom: 0.85rem;
        }
        .plan-checkout-link {
            display: block;
            width: 100%;
            text-align: center;
            padding: 0.7rem 0.95rem;
            border-radius: 0.7rem;
            background: var(--brand-blue);
            color: #0f172a !important;
            font-weight: 650;
            text-decoration: none !important;
            border: 1px solid var(--brand-blue);
            margin-top: 0.2rem;
            margin-bottom: 0.95rem;
            font-size: 0.98rem;
            line-height: 1.2;
        }
        .plan-checkout-link:hover {
            background: var(--brand-blue-dark);
            border-color: var(--brand-blue-dark);
        }
        .pricing-feature {
            margin-bottom: 0.35rem;
            font-size: 0.97rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    col1, col2 = st.columns(2)
    for col, plan_key in zip([col1, col2], ["starter", "pro"]):
        plan = PLANS[plan_key]
        with col:
            st.markdown(
                (
                    '<div class="pricing-card">'
                    f'<div class="pricing-plan-name">{escape(plan["name"])}</div>'
                    f'<div class="pricing-price">{escape(plan["monthly_price"])}</div>'
                    '<div class="pricing-period">Per month</div>'
                    f'<div class="pricing-credits">{escape(plan["credit_note"])}</div>'
                    '<div class="pricing-note">1 returned buyer company = 1 credit</div>'
                ),
                unsafe_allow_html=True,
            )
            if stripe_ready() and plan["price_id"]:
                try:
                    url = get_cached_checkout_url(user, plan_key)
                    st.markdown(
                        f'<a class="plan-checkout-link" href="{url}" target="_self">Subscribe to {plan["name"]}</a>',
                        unsafe_allow_html=True,
                    )
                except Exception as exc:
                    st.error(f"Stripe checkout could not be created: {exc}")
            else:
                st.info(f"Set the Stripe price ID for {plan['name']} to enable checkout.")
            for feature in plan["features"]:
                st.markdown(f'<div class="pricing-feature">• {escape(feature)}</div>', unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)

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
    if is_admin_user(current_user()):
        with st.expander("Admin credit controls"):
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
    available_credits = credits()
    result_limit_cap = min(20, max(3, available_credits)) if available_credits else 3
    result_limit_options = list(range(3, result_limit_cap + 1))
    default_result_limit = min(10, result_limit_cap)
    result_limit = st.selectbox(
        "Buyer company result limit",
        options=result_limit_options,
        index=result_limit_options.index(default_result_limit),
        help="You are charged based on the final buyer companies returned, up to this limit.",
    )
    estimated_range, estimate_basis = estimate_search_time(
        len(selected),
        high_volume,
        time_window,
    )
    basis_text = "based on your recent runs" if estimate_basis == "history" else "based on current settings"
    st.info(
        f"Credits remaining: {available_credits}\n\n"
        f"This search will cost between 3 and {result_limit} credits depending on buyer companies returned.\n\n"
        f"Estimated search time: {format_duration_range_text(estimated_range[0], estimated_range[1])} ({basis_text})."
    )
    st.caption("Search time can still vary because live web search depends on outside websites and OpenAI response time.")

    if st.button("Generate and save list", type="primary"):
        if not selected:
            st.error("Please select at least one saved service.")
            return
        current_credits = credits()
        if current_credits < 3:
            st.error("At least 3 credits are required to run a search.")
            return
        if current_credits < result_limit:
            st.error(f"You need at least {result_limit} credits available to run with a result limit of {result_limit}.")
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
            company_df_all = aggregate_companies(evidence_df)
            company_df = company_df_all.head(result_limit).reset_index(drop=True)
            actual_credits_used = max(3, len(company_df))
            duration_seconds = time.time() - start_time
            run_id = save_run(
                run_name=f"{datetime.now().strftime('%Y-%m-%d %H:%M')} | {len(selected)} service(s)",
                services_text="; ".join([options[label]["service_name"] for label in selected]),
                service_count=len(selected),
                location_filter=location_filter,
                time_window=time_window,
                high_volume_mode=high_volume,
                enrichment_enabled=False,
                credits_used=actual_credits_used,
                duration_seconds=duration_seconds,
                company_df=company_df,
                evidence_df=evidence_df,
            )
            remaining = add_credits(-actual_credits_used)
            progress.progress(100, text="List complete.")
            st.success(
                f"Saved list #{run_id} created in {format_duration_text(duration_seconds)}. {len(company_df)} buyer companies returned. Credits used: {actual_credits_used}. Credits remaining: {remaining}"
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
    if master_company_df.empty:
        st.info("Saved lists exist, but the master company list is still empty.")
        return

    service_options = sorted(
        {
            service
            for value in master_company_df["matched_services"].tolist()
            for service in split_service_values(value)
        }
    )
    selected_services = st.multiselect(
        "Filter by Matched Services",
        options=service_options,
        help="Select one or more services to include only matching rows in the master list.",
    )

    filtered_company_df = master_company_df.copy()
    if selected_services:
        selected_set = set(selected_services)
        filtered_company_df = filtered_company_df[
            filtered_company_df["matched_services"].apply(
                lambda value: bool(selected_set.intersection(split_service_values(value)))
            )
        ].reset_index(drop=True)

    if filtered_company_df.empty:
        st.info("No buyer company rows match the selected services.")
        return

    st.dataframe(pretty_df(filtered_company_df), use_container_width=True)
    st.download_button(
        "Download master company list as CSV",
        data=csv_data(filtered_company_df),
        file_name="nextstepsignal_master_company_list.csv",
        mime="text/csv",
        key="master_company_csv",
    )


def page_users():
    st.title("Users")
    user_table = users_df()
    if user_table.empty:
        st.info("No users found yet.")
        return

    display = user_table.copy()
    display["is_admin"] = display["is_admin"].map({1: "Yes", 0: "No"})
    display["subscription_status"] = display["subscription_status"].fillna("inactive").str.title()
    display["plan_name"] = display["plan_name"].fillna("None")
    display["monthly_credit_allowance"] = display["monthly_credit_allowance"].fillna(0).astype(int)
    display["credit_balance"] = display["credit_balance"].fillna(0).astype(int)
    display["created_at"] = display["created_at"].fillna("")
    display = display[
        [
            "full_name",
            "email",
            "credit_balance",
            "subscription_status",
            "plan_name",
            "monthly_credit_allowance",
            "is_admin",
            "created_at",
        ]
    ]
    st.dataframe(pretty_df(display), use_container_width=True)


def page_next_steps():
    st.title("Next Steps")
    master_evidence_df = build_master_evidence_data()
    if master_evidence_df.empty:
        st.info("Generate and save at least one list before using Next Steps.")
        return

    company_priority_df = build_next_steps_company_table(master_evidence_df)
    if company_priority_df.empty:
        st.info("No company priority analysis is available from the current saved evidence.")
        return

    st.markdown(
        """
        <style>
        .nextsteps-wrap {
            max-width: 1080px;
            margin: 0 auto;
        }
        .nextsteps-company-box {
            border: 1px solid var(--brand-border);
            border-radius: 1rem;
            padding: 1.15rem 1.15rem 0.95rem 1.15rem;
            background: linear-gradient(180deg, rgba(96, 165, 250, 0.08), rgba(255,255,255,0.02));
            margin: 0 auto 1.15rem auto;
            box-shadow: 0 14px 34px rgba(15, 23, 42, 0.12);
        }
        .nextsteps-company-title {
            font-size: 1.32rem;
            font-weight: 800;
            color: #eff6ff;
            margin-bottom: 0.45rem;
        }
        .nextsteps-grid {
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 0.85rem;
            margin-bottom: 0.95rem;
        }
        .nextsteps-meta-card {
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: 0.95rem;
            background: rgba(15, 23, 42, 0.45);
            padding: 0.9rem 1rem;
        }
        .nextsteps-meta-label {
            font-size: 0.76rem;
            font-weight: 700;
            letter-spacing: 0.02em;
            text-transform: uppercase;
            color: #93c5fd;
            margin-bottom: 0.28rem;
        }
        .nextsteps-meta-value {
            font-size: 0.98rem;
            line-height: 1.55;
            color: #e5eefb;
        }
        .nextsteps-section-label {
            display: inline-block;
            background: rgba(96, 165, 250, 0.16);
            color: #dbeafe;
            padding: 0.32rem 0.55rem;
            border-radius: 0.45rem;
            font-size: 0.82rem;
            font-weight: 700;
            margin-bottom: 0.5rem;
        }
        .nextsteps-company-copy {
            color: #cbd5e1;
            line-height: 1.55;
            margin-bottom: 0.8rem;
        }
        .nextsteps-summary-grid {
            display: grid;
            grid-template-columns: repeat(4, minmax(0, 1fr));
            gap: 0.8rem;
            margin-bottom: 1rem;
        }
        .nextsteps-summary-card {
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: 0.95rem;
            background: rgba(15, 23, 42, 0.42);
            padding: 0.9rem 1rem;
        }
        .nextsteps-summary-label {
            font-size: 0.76rem;
            font-weight: 700;
            text-transform: uppercase;
            color: #93c5fd;
            margin-bottom: 0.35rem;
        }
        .nextsteps-summary-value {
            font-size: 1.35rem;
            font-weight: 800;
            color: #eff6ff;
            line-height: 1.15;
        }
        .nextsteps-summary-subvalue {
            margin-top: 0.3rem;
            color: #cbd5e1;
            font-size: 0.88rem;
            line-height: 1.45;
        }
        .nextsteps-takeaway-box {
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: 1rem;
            background: rgba(15, 23, 42, 0.35);
            padding: 1rem 1rem 0.55rem 1rem;
            margin-bottom: 1rem;
        }
        .nextsteps-takeaway-item {
            color: #dbeafe;
            line-height: 1.6;
            margin-bottom: 0.55rem;
        }
        @media (max-width: 900px) {
            .nextsteps-grid {
                grid-template-columns: 1fr;
            }
            .nextsteps-summary-grid {
                grid-template-columns: 1fr 1fr;
            }
        }
        @media (max-width: 640px) {
            .nextsteps-summary-grid {
                grid-template-columns: 1fr;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown('<div class="nextsteps-wrap">', unsafe_allow_html=True)
    top_company_count = min(5, len(company_priority_df))
    top_companies_df = company_priority_df.head(top_company_count).copy()
    deep_dive_cache = st.session_state.setdefault("company_deep_dive_cache", {})

    stat1, stat2, stat3, stat4 = st.columns(4)
    stat1.metric("Companies Reviewed", len(company_priority_df))
    stat2.metric("Top Companies Highlighted", top_company_count)
    stat3.metric("Multiple Posting Signals", int((company_priority_df["relevant_posting_count"] >= 2).sum()))
    stat4.metric("Salary Disclosed", int((company_priority_df["salary_signal"] != "Not disclosed").sum()))

    st.download_button(
        "Download top next steps as CSV",
        data=csv_data(
            top_companies_df[
                [
                    "buyer_company",
                    "relevant_posting_count",
                    "most_recent_posted_date",
                    "salary_signal",
                    "matched_services",
                    "likely_buyer_department_general",
                    "why_highlighted",
                    "suggested_next_step",
                    "source_urls",
                ]
            ]
        ),
        file_name="nextstepsignal_next_steps.csv",
        mime="text/csv",
    )

    freshest_date = next(
        (value for value in company_priority_df["most_recent_posted_date"].tolist() if value and value != "Unknown"),
        "Unknown",
    )
    multiple_postings = int((company_priority_df["relevant_posting_count"] >= 2).sum())
    salary_disclosed = int((company_priority_df["salary_signal"] != "Not disclosed").sum())
    matched_service_counts = {}
    for value in company_priority_df["matched_services"].fillna(""):
        for service in split_service_values(value):
            matched_service_counts[service] = matched_service_counts.get(service, 0) + 1
    top_service = (
        sorted(matched_service_counts.items(), key=lambda item: (-item[1], item[0]))[0][0]
        if matched_service_counts
        else "Not enough data"
    )
    highest_signal_company = safe_text(top_companies_df.iloc[0]["buyer_company"]) if not top_companies_df.empty else "Unknown"

    st.subheader("Analysis")
    st.markdown(
        (
            '<div class="nextsteps-summary-grid">'
            f'<div class="nextsteps-summary-card"><div class="nextsteps-summary-label">Companies Reviewed</div><div class="nextsteps-summary-value">{len(company_priority_df)}</div><div class="nextsteps-summary-subvalue">Total buyer companies included in the current review.</div></div>'
            f'<div class="nextsteps-summary-card"><div class="nextsteps-summary-label">Multiple Relevant Postings</div><div class="nextsteps-summary-value">{multiple_postings}</div><div class="nextsteps-summary-subvalue">Companies showing repeated demand rather than a single isolated role.</div></div>'
            f'<div class="nextsteps-summary-card"><div class="nextsteps-summary-label">Salary Disclosed</div><div class="nextsteps-summary-value">{salary_disclosed}</div><div class="nextsteps-summary-subvalue">Companies with at least one explicit base salary in the captured postings.</div></div>'
            f'<div class="nextsteps-summary-card"><div class="nextsteps-summary-label">Freshest Posting</div><div class="nextsteps-summary-value">{escape(freshest_date)}</div><div class="nextsteps-summary-subvalue">Most recent posting date observed in the current saved evidence.</div></div>'
            '</div>'
        ),
        unsafe_allow_html=True,
    )

    priority_table_df = top_companies_df[
        [
            "buyer_company",
            "relevant_posting_count",
            "most_recent_posted_date",
            "salary_signal",
            "why_highlighted",
        ]
    ].copy()
    priority_table_df.columns = [
        "Company",
        "Relevant Postings",
        "Most Recent Date",
        "Salary Disclosed",
        "Why Prioritized",
    ]
    st.markdown("**Top Priority Companies**")
    st.dataframe(pretty_df(priority_table_df), use_container_width=True, hide_index=True)

    st.markdown(
        (
            '<div class="nextsteps-takeaway-box">'
            '<div class="nextsteps-section-label">Key Takeaways</div>'
            + "".join(
                f'<div class="nextsteps-takeaway-item">{escape(line)}</div>'
                for line in build_next_steps_takeaways(top_companies_df, company_priority_df)
            )
            + f'<div class="nextsteps-takeaway-item">Highest-signal company in the current review: {escape(highest_signal_company)}.</div>'
            + f'<div class="nextsteps-takeaway-item">Most common matched service across the current review: {escape(top_service)}.</div>'
            + '</div>'
        ),
        unsafe_allow_html=True,
    )

    st.subheader("Priority Company Reports")
    for _, company_row in top_companies_df.iterrows():
        company_name = company_row["buyer_company"]
        company_evidence_df = ensure_evidence_columns(
            master_evidence_df[master_evidence_df["company_name"] == company_name].copy()
        )
        company_evidence_df = company_evidence_df.sort_values(
            ["posted_date", "match_score"],
            ascending=[False, False],
        ).reset_index(drop=True)

        relevant_jobs_df = company_evidence_df[
            company_evidence_df["match_type"].isin(["Direct", "Peripheral"])
        ].drop_duplicates(subset=["source_url", "job_title"], keep="first")
        other_jobs_df = company_evidence_df[
            company_evidence_df["match_type"].isin(["Weak"])
        ].drop_duplicates(subset=["source_url", "job_title"], keep="first")

        st.markdown('<div class="nextsteps-company-box">', unsafe_allow_html=True)
        st.markdown(
            f'<div class="nextsteps-company-title">Company Name: {escape(company_name)}</div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            f'<div class="nextsteps-grid">'
            f'<div class="nextsteps-meta-card"><div class="nextsteps-meta-label">Company Description</div><div class="nextsteps-meta-value">{escape(build_company_business_description(company_name, company_evidence_df))}</div></div>'
            f'<div class="nextsteps-meta-card"><div class="nextsteps-meta-label">Why It Is Relevant</div><div class="nextsteps-meta-value">{escape(build_company_next_steps_description(company_row, company_evidence_df))}</div></div>'
            f'</div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            f'<div class="nextsteps-grid">'
            f'<div class="nextsteps-meta-card"><div class="nextsteps-meta-label">Suggested Next Step</div><div class="nextsteps-meta-value">{escape(safe_text(company_row["suggested_next_step"], "No next step captured."))}</div></div>'
            f'<div class="nextsteps-meta-card"><div class="nextsteps-meta-label">Likely Buyer Department</div><div class="nextsteps-meta-value">{escape(safe_text(company_row["likely_buyer_department_general"], "Unknown"))}</div></div>'
            f'<div class="nextsteps-meta-card"><div class="nextsteps-meta-label">Relevant Posting Count</div><div class="nextsteps-meta-value">{escape(str(company_row["relevant_posting_count"]))}</div></div>'
            f'<div class="nextsteps-meta-card"><div class="nextsteps-meta-label">Most Recent Posting Date</div><div class="nextsteps-meta-value">{escape(safe_text(company_row["most_recent_posted_date"], "Unknown"))}</div></div>'
            f'</div>',
            unsafe_allow_html=True,
        )

        st.markdown('<div class="nextsteps-section-label">Relevant Job Postings</div>', unsafe_allow_html=True)
        if relevant_jobs_df.empty:
            st.write("No Direct or Peripheral postings available for this company.")
        else:
            for _, job_row in relevant_jobs_df.iterrows():
                render_next_steps_job_block(job_row)

        st.markdown('<div class="nextsteps-section-label">Other Related Job Postings</div>', unsafe_allow_html=True)
        if other_jobs_df.empty:
            st.write("No additional Weak postings available for this company.")
        else:
            for _, job_row in other_jobs_df.iterrows():
                render_next_steps_job_block(job_row)

        st.markdown('<div class="nextsteps-section-label">Company Deep Dive</div>', unsafe_allow_html=True)
        matched_services_text = safe_text(company_row.get("matched_services"))
        company_cache_key = f"{safe_text(company_name)}::{matched_services_text}"
        deep_dive_entry = deep_dive_cache.get(company_cache_key)

        if st.button(
            f"Expand company hiring view for {company_name}",
            key=f"expand_company_view_{company_cache_key}",
        ):
            try:
                with st.spinner(f"Searching for additional public postings from {company_name}..."):
                    api_client = client()
                    raw_json, deep_dive_df = search_company_deep_dive(
                        api_client,
                        company_name,
                        matched_services_text,
                        company_evidence_df,
                    )
                    deep_dive_entry = {
                        "raw_json": raw_json,
                        "records": deep_dive_df.to_dict(orient="records"),
                        "error": None,
                    }
                    deep_dive_cache[company_cache_key] = deep_dive_entry
            except Exception as exc:
                deep_dive_entry = {
                    "raw_json": "",
                    "records": [],
                    "error": str(exc),
                }
                deep_dive_cache[company_cache_key] = deep_dive_entry

        deep_dive_entry = deep_dive_cache.get(company_cache_key)
        if not deep_dive_entry:
            st.caption("Run a company deep dive to search for additional public postings from this company.")
        elif deep_dive_entry.get("error"):
            st.warning(f"Company deep dive could not be completed: {deep_dive_entry['error']}")
        else:
            deep_dive_df = pd.DataFrame(deep_dive_entry.get("records", []))
            if deep_dive_df.empty:
                st.write("No additional public postings were found beyond the ones already captured in this report.")
            else:
                direct_deep_dive_df = deep_dive_df[
                    deep_dive_df["relevance_bucket"] == "Directly relevant"
                ].reset_index(drop=True)
                adjacent_deep_dive_df = deep_dive_df[
                    deep_dive_df["relevance_bucket"] == "Adjacent"
                ].reset_index(drop=True)
                broader_deep_dive_df = deep_dive_df[
                    deep_dive_df["relevance_bucket"] == "Broader company context"
                ].reset_index(drop=True)

                st.markdown("**Additional Directly Relevant Postings**")
                if direct_deep_dive_df.empty:
                    st.write("No additional directly relevant postings were found.")
                else:
                    for _, job_row in direct_deep_dive_df.iterrows():
                        render_company_deep_dive_job_block(job_row)

                st.markdown("**Additional Adjacent Postings**")
                if adjacent_deep_dive_df.empty:
                    st.write("No additional adjacent postings were found.")
                else:
                    for _, job_row in adjacent_deep_dive_df.iterrows():
                        render_company_deep_dive_job_block(job_row)

                st.markdown("**Broader Company Hiring Context**")
                if broader_deep_dive_df.empty:
                    st.write("No broader company-context postings were found.")
                else:
                    for _, job_row in broader_deep_dive_df.iterrows():
                        render_company_deep_dive_job_block(job_row)
        st.markdown("</div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)


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
        st.markdown(f'<div class="sidebar-brand">{APP_NAME}</div>', unsafe_allow_html=True)
        st.markdown(
            (
                '<div class="sidebar-card">'
                f'<div class="sidebar-user-name">{escape(user["full_name"])}</div>'
                f'<div class="sidebar-user-email">{escape(user["email"])}</div>'
                '<div class="sidebar-mini-grid">'
                f'<div class="sidebar-mini-item"><div class="sidebar-mini-label">Credits</div><div class="sidebar-mini-value">{credits(user["id"])}</div></div>'
                f'<div class="sidebar-mini-item"><div class="sidebar-mini-label">Plan</div><div class="sidebar-mini-value">{escape(user.get("plan_name") or "None")}</div></div>'
                f'<div class="sidebar-mini-item"><div class="sidebar-mini-label">Status</div><div class="sidebar-mini-value">{escape(user.get("subscription_status", "inactive").title())}</div></div>'
                '</div>'
                '</div>'
            ),
            unsafe_allow_html=True,
        )
        nav_options = [
            "Dashboard",
            "Plans & Billing",
            "Service Profiles",
            "Generate List",
            "Saved Lists",
            "Next Steps",
            "Potential Expansions",
        ]
        if is_admin_user(user):
            nav_options.append("Users")
        page = st.radio(
            "Navigate",
            nav_options,
            label_visibility="collapsed",
        )
        if st.button("Sign Out", type="secondary"):
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
    elif page == "Next Steps":
        page_next_steps()
    elif page == "Potential Expansions":
        page_potential_expansions()
    elif page == "Users" and is_admin_user(user):
        page_users()
    else:
        page_saved_lists()
