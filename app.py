import io
import json
import os
import re
import sqlite3
import hashlib
import hmac
import secrets
import smtplib
import ssl
import statistics
import time
from datetime import datetime
from email.message import EmailMessage
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
SIGNUP_FREE_CREDITS = int(os.getenv("SIGNUP_FREE_CREDITS", "10"))
TIME_OPTIONS = ["1 week", "2 weeks", "1 month", "2 months", "3 months"]
NEXT_STEPS_REFRESH_COST = 10
COMPANY_DEEP_DIVE_COST = 5
APP_BASE_URL = os.getenv("APP_BASE_URL", "http://localhost:8501")
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY", "")
DISCOVERY_MODEL = os.getenv("OPENAI_DISCOVERY_MODEL", "gpt-5-mini")
SYNTHESIS_MODEL = os.getenv("OPENAI_SYNTHESIS_MODEL", "gpt-5-mini")
PASSWORD_RESET_HOURS = int(os.getenv("PASSWORD_RESET_HOURS", "2"))
SMTP_HOST = os.getenv("SMTP_HOST", "").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USERNAME = os.getenv("SMTP_USERNAME", "").strip()
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
SMTP_FROM_EMAIL = os.getenv("SMTP_FROM_EMAIL", "").strip()
SMTP_USE_TLS = os.getenv("SMTP_USE_TLS", "1").strip() not in {"0", "false", "False"}
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
    "service_description",
    "supporting_signal_count",
    "connected_current_services",
    "companies_showing_interest",
    "sample_job_titles",
    "sample_responsibilities",
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
    "service_category": "Service Category",
    "service_name": "Service Name",
    "service_description": "Service Description",
    "suggested_service": "Suggested Service",
    "supporting_signal_count": "Supporting Signal Count",
    "connected_current_services": "Connected Current Services",
    "companies_showing_interest": "Companies Showing Interest",
    "sample_job_titles": "Sample Job Titles",
    "sample_responsibilities": "Typical Responsibilities Seen",
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
        [data-testid="stHeader"] {
            display: none;
        }
        [data-testid="stAppViewBlockContainer"] {
            padding-top: 0 !important;
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


def landing_marketing_mockup_html(kind="hero"):
    if kind == "list":
        return """
        <div class="landing-mockup-wrap landing-mockup-list">
            <div class="epic-scene epic-scene-list">
                <div class="epic-nebula nebula-a"></div>
                <div class="epic-nebula nebula-b"></div>
                <div class="epic-horizon"></div>
                <div class="epic-cityline"></div>
                <div class="epic-grid-floor"></div>
            </div>
            <div class="epic-panel primary-panel">
                <div class="epic-panel-kicker">Generate List</div>
                <div class="epic-panel-title">Public demand signals</div>
                <div class="epic-panel-copy">Buyer companies surfaced from your saved service scope.</div>
                <div class="epic-chip-row">
                    <div class="epic-chip">3 services</div>
                    <div class="epic-chip">1 month</div>
                    <div class="epic-chip">15 results</div>
                </div>
            </div>
            <div class="epic-panel side-panel top-right-panel">
                <div class="epic-panel-kicker">Top signal</div>
                <div class="epic-panel-title">DEPCOM Power</div>
                <div class="epic-panel-copy">4 relevant postings</div>
            </div>
            <div class="epic-panel side-panel bottom-left-panel">
                <div class="epic-panel-kicker">Export</div>
                <div class="epic-panel-title">CSV + PDF</div>
                <div class="epic-panel-copy">Ready to share</div>
            </div>
        </div>
        """
    if kind == "next":
        return """
        <div class="landing-mockup-wrap landing-mockup-next">
            <div class="epic-scene epic-scene-next">
                <div class="epic-nebula nebula-a"></div>
                <div class="epic-nebula nebula-c"></div>
                <div class="epic-horizon"></div>
                <div class="epic-citadel"></div>
                <div class="epic-glow-trail"></div>
            </div>
            <div class="epic-panel primary-panel">
                <div class="epic-panel-kicker">Next Steps</div>
                <div class="epic-panel-title">Top companies ranked</div>
                <div class="epic-score-card">
                    <div class="epic-rank-chip">#1</div>
                    <div>
                        <div class="epic-score-title">DEPCOM Power</div>
                        <div class="epic-score-copy">5 relevant postings</div>
                    </div>
                </div>
                <div class="epic-mini-grid">
                    <div class="epic-mini-card"><span>Freshest</span><strong>04/03/26</strong></div>
                    <div class="epic-mini-card"><span>Pattern</span><strong>Repeated</strong></div>
                    <div class="epic-mini-card"><span>Focus</span><strong>Field Ops</strong></div>
                </div>
            </div>
            <div class="epic-panel side-panel bottom-left-panel">
                <div class="epic-panel-kicker">Priority view</div>
                <div class="epic-panel-title">Top 5 companies</div>
                <div class="epic-panel-copy">Structured for action</div>
            </div>
        </div>
        """
    if kind == "expansion":
        return """
        <div class="landing-mockup-wrap landing-mockup-expansion">
            <div class="epic-scene epic-scene-expansion">
                <div class="epic-nebula nebula-b"></div>
                <div class="epic-nebula nebula-c"></div>
                <div class="epic-horizon"></div>
                <div class="epic-mountain-left"></div>
                <div class="epic-mountain-right"></div>
                <div class="epic-signal-beam"></div>
            </div>
            <div class="epic-panel primary-panel">
                <div class="epic-panel-kicker">Potential Expansions</div>
                <div class="epic-panel-title">Adjacent scopes showing up</div>
                <div class="epic-stack-card">
                    <div class="epic-stack-row"><span>Commissioning Support</span><strong>12</strong></div>
                    <div class="epic-stack-row"><span>Field Quality Oversight</span><strong>9</strong></div>
                    <div class="epic-stack-row"><span>Grid Integration Studies</span><strong>6</strong></div>
                </div>
            </div>
            <div class="epic-panel side-panel top-right-panel">
                <div class="epic-panel-kicker">Signals</div>
                <div class="epic-panel-title">56 tracked</div>
                <div class="epic-panel-copy">Across saved evidence</div>
            </div>
        </div>
        """
    return """
    <div class="landing-mockup-wrap landing-mockup-hero">
        <div class="hero-clean-shell">
            <div class="hero-clean-orbit orbit-a"></div>
            <div class="hero-clean-orbit orbit-b"></div>
            <div class="hero-clean-browser">
                <div class="hero-clean-bar">
                    <span class="hero-clean-dot"></span>
                    <span class="hero-clean-dot"></span>
                    <span class="hero-clean-dot"></span>
                </div>
                <div class="hero-clean-body">
                    <div class="hero-clean-panel hero-clean-panel-large"></div>
                    <div class="hero-clean-grid">
                        <div class="hero-clean-card tall"></div>
                        <div class="hero-clean-card"></div>
                        <div class="hero-clean-card"></div>
                        <div class="hero-clean-card wide"></div>
                    </div>
                </div>
            </div>
            <div class="hero-clean-float float-top"></div>
            <div class="hero-clean-float float-bottom"></div>
        </div>
    </div>
    """


def render_landing_feature_band(title, copy, bullets, kind, tone="soft"):
    bullet_html = "".join(f"<div class=\"landing-bullet\">{escape(item)}</div>" for item in bullets)
    st.markdown(
        f"""
        <div class="landing-band {tone}">
            <div class="landing-feature-layout">
                <div class="landing-feature-copy-shell">
                    <div class="landing-product-title">{escape(title)}</div>
                    <div class="landing-product-copy">{escape(copy)}</div>
                    <div class="landing-product-bullets">{bullet_html}</div>
                </div>
                <div class="landing-feature-visual-shell">
                    {landing_marketing_mockup_html(kind)}
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
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

Your task is to review a client's current service coverage and recent market evidence, then identify service gaps the market appears to be requesting that are not explicitly covered by the current service set.

Current service profiles:
{{CURRENT_SERVICES}}

Market evidence from recent U.S. solar jobs, filled roles, RFPs, and similar opportunities:
{{MARKET_EVIDENCE}}

Return valid JSON only using this schema:
{
  "expansions": [
    {
      "suggested_service": null,
      "service_description": null,
      "supporting_signal_count": 0,
      "connected_current_services": [],
      "companies_showing_interest": [],
      "sample_job_titles": [],
      "sample_responsibilities": []
    }
  ]
}

Rules:
- Suggest services that are adjacent or related to the current service set
- Do not repeat services already explicitly covered
- Base suggestions only on evidence shown in the market evidence
- Name each suggested service based on the overall hiring pattern across the supporting signals, not just one repeated keyword
- Only use a narrow term like repowering, SCADA, commissioning, controls, QAQC, or grid integration in the suggested service title when that term is clearly supported by multiple postings or repeated responsibility language
- If the evidence is broader or mixed, prefer a more general title that reflects the real pattern of work being requested
- Do not force all suggested service titles into the same wording pattern
- supporting_signal_count must be an integer
- service_description should be a short factual description of the suggested service based on repeated job-posting language
- connected_current_services must be a list of current saved services this suggested expansion most closely connects to
- companies_showing_interest must be a list of companies whose postings suggest demand for the suggested service
- sample_job_titles must be a list
- sample_responsibilities must be a list
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
                    "service_description": {"type": ["string", "null"]},
                    "supporting_signal_count": {"type": "integer", "minimum": 0},
                    "connected_current_services": {"type": "array", "items": {"type": "string"}},
                    "companies_showing_interest": {"type": "array", "items": {"type": "string"}},
                    "sample_job_titles": {"type": "array", "items": {"type": "string"}},
                    "sample_responsibilities": {"type": "array", "items": {"type": "string"}},
                },
                "required": [
                    "suggested_service",
                    "service_description",
                    "supporting_signal_count",
                    "connected_current_services",
                    "companies_showing_interest",
                    "sample_job_titles",
                    "sample_responsibilities",
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
            CREATE TABLE IF NOT EXISTS password_reset_tokens (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                token_hash TEXT NOT NULL,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                used_at TEXT,
                FOREIGN KEY(user_id) REFERENCES users(id)
            )
            """
        )
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS services (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                service_category TEXT NOT NULL DEFAULT 'General',
                service_order INTEGER,
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
            """
            CREATE TABLE IF NOT EXISTS expansion_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                services_text TEXT NOT NULL,
                service_count INTEGER NOT NULL DEFAULT 0,
                used_saved_baseline INTEGER NOT NULL DEFAULT 1,
                broader_validation INTEGER NOT NULL DEFAULT 0,
                high_volume_mode INTEGER NOT NULL DEFAULT 0,
                location_filter TEXT NOT NULL,
                time_window TEXT NOT NULL,
                credits_used INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                evidence_json TEXT NOT NULL,
                expansion_json TEXT NOT NULL
            )
            """
        )
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS deep_dive_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                company_name TEXT NOT NULL,
                matched_services_text TEXT,
                credits_used INTEGER NOT NULL,
                created_at TEXT NOT NULL,
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
        if "service_category" not in service_columns:
            db.execute("ALTER TABLE services ADD COLUMN service_category TEXT NOT NULL DEFAULT 'General'")
        if "service_order" not in service_columns:
            db.execute("ALTER TABLE services ADD COLUMN service_order INTEGER")
        search_columns = [row["name"] for row in db.execute("PRAGMA table_info(searches)").fetchall()]
        if "user_id" not in search_columns:
            db.execute("ALTER TABLE searches ADD COLUMN user_id INTEGER")
        if "service_count" not in search_columns:
            db.execute("ALTER TABLE searches ADD COLUMN service_count INTEGER NOT NULL DEFAULT 0")
        if "enrichment_enabled" not in search_columns:
            db.execute("ALTER TABLE searches ADD COLUMN enrichment_enabled INTEGER NOT NULL DEFAULT 1")
        if "duration_seconds" not in search_columns:
            db.execute("ALTER TABLE searches ADD COLUMN duration_seconds REAL")
        expansion_columns = [row["name"] for row in db.execute("PRAGMA table_info(expansion_runs)").fetchall()]
        if expansion_columns and "user_id" not in expansion_columns:
            db.execute("ALTER TABLE expansion_runs ADD COLUMN user_id INTEGER")
        deep_dive_columns = [row["name"] for row in db.execute("PRAGMA table_info(deep_dive_runs)").fetchall()]
        if deep_dive_columns and "user_id" not in deep_dive_columns:
            db.execute("ALTER TABLE deep_dive_runs ADD COLUMN user_id INTEGER")


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


def hash_reset_token(token):
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def smtp_ready():
    return bool(SMTP_HOST and SMTP_FROM_EMAIL)


def send_email_message(to_email, subject, body_text):
    if not smtp_ready():
        raise ValueError("Password reset email is not configured yet.")
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = SMTP_FROM_EMAIL
    message["To"] = to_email
    message.set_content(body_text)

    if SMTP_USE_TLS:
        context = ssl.create_default_context()
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls(context=context)
            if SMTP_USERNAME:
                server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.send_message(message)
    else:
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=ssl.create_default_context()) as server:
            if SMTP_USERNAME:
                server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.send_message(message)


def create_password_reset_token(email):
    user = get_user_by_email(email)
    if not user:
        return None
    token = secrets.token_urlsafe(32)
    token_hash = hash_reset_token(token)
    created_at = datetime.now()
    expires_at = created_at.timestamp() + (PASSWORD_RESET_HOURS * 3600)
    created_text = created_at.strftime("%Y-%m-%d %H:%M:%S")
    expires_text = datetime.fromtimestamp(expires_at).strftime("%Y-%m-%d %H:%M:%S")
    with conn() as db:
        db.execute(
            "UPDATE password_reset_tokens SET used_at = ? WHERE user_id = ? AND used_at IS NULL",
            (created_text, user["id"]),
        )
        db.execute(
            """
            INSERT INTO password_reset_tokens (
                user_id, token_hash, created_at, expires_at, used_at
            ) VALUES (?, ?, ?, ?, NULL)
            """,
            (user["id"], token_hash, created_text, expires_text),
        )
    return token, user


def get_password_reset_record(token):
    if not token:
        return None
    token_hash = hash_reset_token(token)
    with conn() as db:
        row = db.execute(
            """
            SELECT prt.*, u.email, u.full_name
            FROM password_reset_tokens prt
            JOIN users u ON u.id = prt.user_id
            WHERE prt.token_hash = ?
            ORDER BY prt.id DESC
            LIMIT 1
            """,
            (token_hash,),
        ).fetchone()
    if not row:
        return None
    record = dict(row)
    expires_at = pd.to_datetime(record.get("expires_at"), errors="coerce")
    used_at = pd.to_datetime(record.get("used_at"), errors="coerce")
    if pd.notna(used_at):
        return None
    if pd.isna(expires_at) or expires_at < pd.Timestamp.now():
        return None
    return record


def mark_password_reset_used(reset_id):
    with conn() as db:
        db.execute(
            "UPDATE password_reset_tokens SET used_at = ? WHERE id = ?",
            (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), int(reset_id)),
        )


def update_user_password(user_id, new_password):
    update_user_fields(int(user_id), password_hash=hash_password(new_password))


def password_reset_url(token):
    return f"{APP_BASE_URL}?reset_token={token}"


def send_password_reset_email(email):
    created = create_password_reset_token(email)
    if not created:
        return False
    token, user = created
    reset_link = password_reset_url(token)
    body = (
        f"Hi {safe_text(user.get('full_name'), 'there')},\n\n"
        f"We received a request to reset your {APP_NAME} password.\n\n"
        f"Open this link to set a new password:\n{reset_link}\n\n"
        f"This link expires in {PASSWORD_RESET_HOURS} hour(s). If you did not request a password reset, you can ignore this email.\n"
    )
    send_email_message(user["email"], f"{APP_NAME} password reset", body)
    return True


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
    starting_credits = ADMIN_DEMO_CREDITS if is_admin else SIGNUP_FREE_CREDITS
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
            "SELECT * FROM services WHERE user_id = ? ORDER BY service_category ASC, COALESCE(service_order, 999999) ASC, created_at ASC, id ASC",
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


def save_service(category, name, description, location_filter, user_id=None):
    user = get_user_by_id(user_id) if user_id else current_user()
    if not user:
        raise ValueError("Please sign in to save service profiles.")
    category_name = category.strip() or "General"
    with conn() as db:
        next_order = db.execute(
            "SELECT COALESCE(MAX(service_order), 0) + 1 FROM services WHERE user_id = ? AND service_category = ?",
            (user["id"], category_name),
        ).fetchone()[0]
        db.execute(
            """
            INSERT INTO services (
                user_id, service_category, service_order, service_name, service_description, target_location,
                default_time_window, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user["id"],
                category_name,
                int(next_order or 1),
                name.strip(),
                description.strip(),
                location_filter.strip(),
                "2 months",
                datetime.now().strftime("%Y-%m-%d %H:%M"),
            ),
        )


def update_service_profile(service_id, category, name, description, location_filter, user_id=None):
    user = get_user_by_id(user_id) if user_id else current_user()
    if not user:
        raise ValueError("Please sign in to update service profiles.")
    service_id = int(service_id)
    with conn() as db:
        existing = db.execute(
            "SELECT * FROM services WHERE id = ? AND user_id = ?",
            (service_id, user["id"]),
        ).fetchone()
        if not existing:
            raise ValueError("Service profile not found.")
        existing = dict(existing)
        new_category = category.strip() or "General"
        new_order = existing.get("service_order")
        if safe_text(existing.get("service_category"), "General") != new_category:
            next_order = db.execute(
                "SELECT COALESCE(MAX(service_order), 0) + 1 FROM services WHERE user_id = ? AND service_category = ?",
                (user["id"], new_category),
            ).fetchone()[0]
            new_order = int(next_order or 1)
        db.execute(
            """
            UPDATE services
            SET service_category = ?, service_order = ?, service_name = ?, service_description = ?, target_location = ?
            WHERE id = ? AND user_id = ?
            """,
            (
                new_category,
                int(new_order or 1),
                name.strip(),
                description.strip(),
                location_filter.strip(),
                service_id,
                user["id"],
            ),
        )
    if safe_text(existing.get("service_category"), "General") != new_category:
        resequence_service_category(existing.get("service_category"), user["id"])
    resequence_service_category(new_category, user["id"])


def delete_service(service_id, user_id=None):
    user = get_user_by_id(user_id) if user_id else current_user()
    if not user:
        raise ValueError("Please sign in to delete service profiles.")
    category_name = None
    with conn() as db:
        existing = db.execute(
            "SELECT service_category FROM services WHERE id = ? AND user_id = ?",
            (int(service_id), user["id"]),
        ).fetchone()
        if existing:
            category_name = existing["service_category"]
    if not category_name:
        return
    with conn() as db:
        db.execute(
            "DELETE FROM services WHERE id = ? AND user_id = ?",
            (int(service_id), user["id"]),
        )
    resequence_service_category(category_name, user["id"])


def resequence_service_category(category_name, user_id=None):
    user = get_user_by_id(user_id) if user_id else current_user()
    if not user:
        return
    category_name = safe_text(category_name, "General")
    with conn() as db:
        rows = db.execute(
            """
            SELECT id
            FROM services
            WHERE user_id = ? AND service_category = ?
            ORDER BY COALESCE(service_order, 999999) ASC, created_at ASC, id ASC
            """,
            (user["id"], category_name),
        ).fetchall()
        for index, row in enumerate(rows, start=1):
            db.execute(
                "UPDATE services SET service_order = ? WHERE id = ? AND user_id = ?",
                (index, row["id"], user["id"]),
            )


def ensure_service_orders(user_id=None):
    user = get_user_by_id(user_id) if user_id else current_user()
    if not user:
        return
    svc = services_df(user["id"])
    if svc.empty:
        return
    for category_name in flatten_unique(svc["service_category"].tolist()):
        resequence_service_category(category_name, user["id"])


def move_service_within_category(service_id, direction, user_id=None):
    user = get_user_by_id(user_id) if user_id else current_user()
    if not user:
        raise ValueError("Please sign in to reorder service profiles.")
    service_id = int(service_id)
    ensure_service_orders(user["id"])
    svc = prepare_service_map_df(services_df(user["id"]))
    row_match = svc[svc["id"] == service_id]
    if row_match.empty:
        return
    row = row_match.iloc[0]
    category_name = safe_text(row["service_category"], "General")
    category_df = svc[svc["service_category"] == category_name].copy().reset_index(drop=True)
    current_idx = int(category_df.index[category_df["id"] == service_id][0])
    if direction == "up":
        swap_idx = current_idx - 1
    else:
        swap_idx = current_idx + 1
    if swap_idx < 0 or swap_idx >= len(category_df):
        return
    current_order = int(category_df.iloc[current_idx]["service_order"])
    swap_order = int(category_df.iloc[swap_idx]["service_order"])
    swap_id = int(category_df.iloc[swap_idx]["id"])
    with conn() as db:
        db.execute("UPDATE services SET service_order = ? WHERE id = ? AND user_id = ?", (swap_order, service_id, user["id"]))
        db.execute("UPDATE services SET service_order = ? WHERE id = ? AND user_id = ?", (current_order, swap_id, user["id"]))
    resequence_service_category(category_name, user["id"])


def build_service_option_map(svc_df):
    if svc_df.empty:
        return {}
    working = prepare_service_map_df(svc_df)
    return {
        f"#{int(row['service_number'])} | {safe_text(row.get('service_category'), 'General')} | {safe_text(row['service_name'], 'Untitled Service')}": row
        for _, row in working.iterrows()
    }


def prepare_service_map_df(svc_df):
    if svc_df.empty:
        return pd.DataFrame()
    working = svc_df.copy()
    working["service_category"] = working["service_category"].fillna("General").replace("", "General")
    if "service_order" not in working.columns:
        working["service_order"] = None
    working["_created_at_sort"] = pd.to_datetime(working["created_at"], errors="coerce")
    working["_service_order_sort"] = pd.to_numeric(working["service_order"], errors="coerce")
    working = working.sort_values(
        ["service_category", "_service_order_sort", "_created_at_sort", "id"],
        ascending=[True, True, True, True],
        na_position="last",
    ).reset_index(drop=True)
    working["service_number"] = working.groupby("service_category").cumcount() + 1
    return working


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


def save_expansion_run(
    services_text,
    service_count,
    used_saved_baseline,
    broader_validation,
    high_volume_mode,
    location_filter,
    time_window,
    credits_used,
    evidence_df,
    expansion_df,
    user_id=None,
):
    user = get_user_by_id(user_id) if user_id else current_user()
    if not user:
        raise ValueError("Please sign in to save expansion analysis.")
    with conn() as db:
        cursor = db.execute(
            """
            INSERT INTO expansion_runs (
                user_id, services_text, service_count, used_saved_baseline, broader_validation,
                high_volume_mode, location_filter, time_window, credits_used, created_at,
                evidence_json, expansion_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user["id"],
                services_text,
                int(service_count),
                int(used_saved_baseline),
                int(broader_validation),
                int(high_volume_mode),
                location_filter,
                time_window,
                int(credits_used),
                datetime.now().strftime("%Y-%m-%d %H:%M"),
                evidence_df.to_json(orient="records"),
                expansion_df.to_json(orient="records"),
            ),
        )
    return cursor.lastrowid


def expansion_runs_df(user_id=None):
    user = get_user_by_id(user_id) if user_id else current_user()
    if not user:
        return pd.DataFrame()
    with conn() as db:
        rows = db.execute(
            "SELECT * FROM expansion_runs WHERE user_id = ? ORDER BY id DESC",
            (user["id"],),
        ).fetchall()
    return pd.DataFrame([dict(r) for r in rows])


def save_deep_dive_run(company_name, matched_services_text, credits_used, evidence_df, user_id=None):
    user = get_user_by_id(user_id) if user_id else current_user()
    if not user:
        raise ValueError("Please sign in to save expanded search evidence.")
    with conn() as db:
        cursor = db.execute(
            """
            INSERT INTO deep_dive_runs (
                user_id, company_name, matched_services_text, credits_used, created_at, evidence_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                user["id"],
                safe_text(company_name, "Unknown Company"),
                safe_text(matched_services_text),
                int(credits_used),
                datetime.now().strftime("%Y-%m-%d %H:%M"),
                evidence_df.to_json(orient="records"),
            ),
        )
    return cursor.lastrowid


def deep_dive_runs_df(user_id=None):
    user = get_user_by_id(user_id) if user_id else current_user()
    if not user:
        return pd.DataFrame()
    with conn() as db:
        rows = db.execute(
            "SELECT * FROM deep_dive_runs WHERE user_id = ? ORDER BY id DESC",
            (user["id"],),
        ).fetchall()
    return pd.DataFrame([dict(r) for r in rows])


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


def deep_dive_records_to_evidence_df(records_df, matched_services_text):
    if records_df.empty:
        return pd.DataFrame(columns=EVIDENCE_COLUMNS)
    services = split_service_values(matched_services_text)
    primary_service = services[0] if services else "Expanded Search"
    evidence_rows = []
    for _, row in records_df.iterrows():
        evidence_rows.append(
            {
                "matched_service": primary_service,
                "company_name": safe_text(row.get("company_name")),
                "job_title": safe_text(row.get("job_title")),
                "base_salary": safe_text(row.get("base_salary")) or None,
                "location": safe_text(row.get("location")) or None,
                "country": "United States",
                "source_type": safe_text(row.get("source_type")) or None,
                "opportunity_status": safe_text(row.get("opportunity_status"), "Unknown"),
                "posted_date": safe_text(row.get("posted_date")) or None,
                "match_score": 90 if safe_text(row.get("relevance_bucket")) == "Directly relevant" else (70 if safe_text(row.get("relevance_bucket")) == "Adjacent" else 45),
                "match_type": "Direct" if safe_text(row.get("relevance_bucket")) == "Directly relevant" else ("Peripheral" if safe_text(row.get("relevance_bucket")) == "Adjacent" else "Weak"),
                "likely_service_need": safe_text(row.get("why_it_matters")) or None,
                "why_it_matches": [safe_text(row.get("why_it_matters"))] if safe_text(row.get("why_it_matters")) else [],
                "matching_responsibilities": [],
                "matching_keywords": [],
                "buyer_department": None,
                "outreach_next_step": None,
                "source_url": safe_text(row.get("source_url")) or None,
            }
        )
    return ensure_evidence_columns(pd.DataFrame(evidence_rows))


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
        ["supporting_signal_count", "suggested_service"],
        ascending=[False, True],
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
        "connected_current_services",
        "companies_showing_interest",
        "sample_job_titles",
        "sample_responsibilities",
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


def canonicalize_company_name(company_name):
    text = safe_text(company_name, "Unknown Company").lower()
    text = re.sub(r"\([^)]*\)", " ", text)
    text = re.sub(r"[^a-z0-9& ]+", " ", text)
    text = text.replace("&", " and ")
    removable_suffixes = {
        "inc",
        "incorporated",
        "llc",
        "corp",
        "corporation",
        "co",
        "company",
        "group",
        "holdings",
        "ltd",
        "limited",
        "na",
    }
    parts = [part for part in text.split() if part not in removable_suffixes]
    return " ".join(parts).strip() or "unknown company"


def choose_display_company_name(names):
    cleaned = [safe_text(name) for name in names if safe_text(name)]
    if not cleaned:
        return "Unknown Company"
    counts = {}
    for name in cleaned:
        counts[name] = counts.get(name, 0) + 1
    ranked = sorted(counts.items(), key=lambda item: (-item[1], len(item[0]), item[0].lower()))
    return ranked[0][0]


def keyword_tokens(*values):
    tokens = set()
    for value in values:
        if isinstance(value, list):
            items = value
        else:
            items = [value]
        for item in items:
            text = safe_text(item).lower()
            if not text:
                continue
            for token in re.findall(r"[a-z0-9]+", text):
                if len(token) >= 4:
                    tokens.add(token)
    return tokens


def build_expansion_company_views(expansion_row, evidence_df):
    if evidence_df.empty:
        return []

    working = ensure_evidence_columns(evidence_df).copy()
    working["canonical_company_name"] = working["company_name"].apply(canonicalize_company_name)
    candidate_companies = split_service_values(expansion_row.get("companies_showing_interest"))
    candidate_canonicals = {canonicalize_company_name(name) for name in candidate_companies if safe_text(name)}
    sample_titles = split_service_values(expansion_row.get("sample_job_titles"))
    title_tokens = keyword_tokens(sample_titles)
    expansion_tokens = keyword_tokens(
        expansion_row.get("suggested_service"),
        expansion_row.get("service_description"),
        expansion_row.get("sample_responsibilities"),
    )

    company_views = []
    for candidate in candidate_companies:
        candidate_canonical = canonicalize_company_name(candidate)
        company_df = working[working["canonical_company_name"] == candidate_canonical].copy()
        if company_df.empty:
            continue

        scored_rows = []
        for _, row in company_df.iterrows():
            title = safe_text(row.get("job_title"))
            row_tokens = keyword_tokens(
                row.get("job_title"),
                row.get("likely_service_need"),
                row.get("matching_keywords"),
                row.get("matching_responsibilities"),
            )
            title_match = 1 if safe_text(title) in sample_titles else 0
            token_overlap = len(row_tokens & (title_tokens | expansion_tokens))
            direct_bucket = 2 if safe_text(row.get("match_type")) in {"Direct", "Peripheral"} else 0
            score = (title_match * 4) + token_overlap + direct_bucket
            if score <= 0:
                continue
            scored_rows.append((score, row))

        if not scored_rows:
            scored_rows = [(1, row) for _, row in company_df.iterrows()]

        scored_rows.sort(
            key=lambda item: (
                -item[0],
                safe_text(item[1].get("posted_date"), ""),
                safe_text(item[1].get("job_title"), "").lower(),
            )
        )
        selected_rows = [item[1] for item in scored_rows[:5]]
        selected_df = pd.DataFrame(selected_rows).drop_duplicates(subset=["source_url", "job_title"], keep="first")
        selected_df = selected_df.sort_values(["posted_date", "match_score"], ascending=[False, False]).reset_index(drop=True)
        if selected_df.empty:
            continue

        recent_dates = pd.to_datetime(selected_df["posted_date"], errors="coerce").dropna()
        most_recent = recent_dates.max().strftime("%m/%d/%y") if not recent_dates.empty else "Unknown"
        company_views.append(
            {
                "company_name": choose_display_company_name(selected_df["company_name"].tolist()),
                "posting_count": len(selected_df),
                "most_recent_posted_date": most_recent,
                "jobs": selected_df.to_dict(orient="records"),
            }
        )

    company_views.sort(key=lambda item: (-item["posting_count"], item["most_recent_posted_date"] == "Unknown", item["company_name"].lower()))
    return company_views


def latest_expansion_run_record(user_id=None):
    runs = expansion_runs_df(user_id)
    if runs.empty:
        return None
    return dict(runs.iloc[0])


def render_potential_expansions_report(
    expansion_df,
    evidence_df,
    services_text,
    location_filter,
    time_window,
    mode_text,
    created_at,
    service_count,
    key_suffix="current",
):
    display_expansion_df = format_lists_for_display(expansion_df)
    evidence_df = format_lists_for_display(evidence_df)

    st.markdown(
        """
        <style>
        .expansion-wrap {
            max-width: 1080px;
            margin: 0 auto;
        }
        .expansion-summary-grid {
            display: grid;
            grid-template-columns: repeat(4, minmax(0, 1fr));
            gap: 0.8rem;
            margin-bottom: 1rem;
        }
        .expansion-summary-card {
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: 0.95rem;
            background: rgba(15, 23, 42, 0.42);
            padding: 0.9rem 1rem;
        }
        .expansion-summary-label {
            font-size: 0.76rem;
            font-weight: 700;
            text-transform: uppercase;
            color: #93c5fd;
            margin-bottom: 0.35rem;
        }
        .expansion-summary-value {
            font-size: 1.3rem;
            font-weight: 800;
            color: #eff6ff;
            line-height: 1.15;
        }
        .expansion-summary-subvalue {
            margin-top: 0.3rem;
            color: #cbd5e1;
            font-size: 0.88rem;
            line-height: 1.45;
        }
        .expansion-card {
            border: 1px solid var(--brand-border);
            border-radius: 1rem;
            padding: 1.1rem 1.1rem 0.95rem 1.1rem;
            background: linear-gradient(180deg, rgba(96, 165, 250, 0.08), rgba(255,255,255,0.02));
            box-shadow: 0 14px 34px rgba(15, 23, 42, 0.12);
            margin-bottom: 1rem;
        }
        .expansion-card-title {
            font-size: 1.2rem;
            font-weight: 800;
            color: #eff6ff;
            margin-bottom: 0.7rem;
        }
        .expansion-card-section {
            margin-bottom: 0.85rem;
        }
        .expansion-card-label {
            font-size: 0.76rem;
            font-weight: 700;
            text-transform: uppercase;
            color: #93c5fd;
            margin-bottom: 0.22rem;
        }
        .expansion-card-value {
            color: #dbeafe;
            line-height: 1.58;
        }
        .expansion-company-box {
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: 0.95rem;
            background: rgba(15, 23, 42, 0.36);
            padding: 0.9rem 1rem;
            margin-bottom: 0.8rem;
        }
        .expansion-company-title {
            font-size: 1rem;
            font-weight: 750;
            color: #eff6ff;
            margin-bottom: 0.25rem;
        }
        .expansion-company-meta {
            color: #cbd5e1;
            font-size: 0.9rem;
            line-height: 1.45;
            margin-bottom: 0.55rem;
        }
        .expansion-job-line {
            color: #dbeafe;
            line-height: 1.55;
            margin-bottom: 0.32rem;
            padding-left: 0.15rem;
        }
        @media (max-width: 900px) {
            .expansion-summary-grid {
                grid-template-columns: 1fr 1fr;
            }
        }
        @media (max-width: 640px) {
            .expansion-summary-grid {
                grid-template-columns: 1fr;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    most_repeated_gap = safe_text(display_expansion_df.iloc[0]["suggested_service"]) if not display_expansion_df.empty else "Unknown"
    st.markdown('<div class="expansion-wrap">', unsafe_allow_html=True)
    st.caption(
        f"Showing saved expansion analysis from {created_at} | {mode_text} | Services analyzed: {service_count}"
    )
    st.markdown(
        (
            '<div class="expansion-summary-grid">'
            f'<div class="expansion-summary-card"><div class="expansion-summary-label">Expansion Gaps Found</div><div class="expansion-summary-value">{len(display_expansion_df)}</div><div class="expansion-summary-subvalue">Distinct service gaps identified from current market evidence.</div></div>'
            f'<div class="expansion-summary-card"><div class="expansion-summary-label">Most Repeated Gap</div><div class="expansion-summary-value">{escape(most_repeated_gap)}</div><div class="expansion-summary-subvalue">The expansion idea appearing most often in the reviewed signals.</div></div>'
            f'<div class="expansion-summary-card"><div class="expansion-summary-label">Services Analyzed</div><div class="expansion-summary-value">{int(service_count or 0)}</div><div class="expansion-summary-subvalue">Saved services included in this expansion review.</div></div>'
            f'<div class="expansion-summary-card"><div class="expansion-summary-label">Market Signals Reviewed</div><div class="expansion-summary-value">{len(evidence_df)}</div><div class="expansion-summary-subvalue">Public postings and related signals used to generate the gaps below.</div></div>'
            '</div>'
        ),
        unsafe_allow_html=True,
    )

    ranked_table_df = display_expansion_df[
        [
            "suggested_service",
            "service_description",
            "supporting_signal_count",
            "companies_showing_interest",
        ]
    ].copy()
    ranked_table_df.columns = [
        "Suggested Expansion",
        "Service Description",
        "Frequency",
        "Companies Showing Interest",
    ]
    st.markdown("**Top Expansion Opportunities**")
    st.dataframe(pretty_df(ranked_table_df), use_container_width=True, hide_index=True)

    st.download_button(
        "Download potential expansions as CSV",
        data=csv_data(ranked_table_df),
        file_name="nextstepsignal_potential_expansions.csv",
        mime="text/csv",
        key=f"expansion_csv_{key_suffix}",
    )
    st.download_button(
        "Download potential expansions as PDF",
        data=expansion_pdf_data(
            display_expansion_df,
            {
                "created_at": created_at,
                "services_text": services_text,
                "location_filter": location_filter,
                "time_window": time_window,
                "mode": mode_text,
            },
        ),
        file_name="nextstepsignal_potential_expansions.pdf",
        mime="application/pdf",
        key=f"expansion_pdf_{key_suffix}",
    )

    st.subheader("Expansion Reports")
    for idx, (_, row) in enumerate(display_expansion_df.iterrows(), start=1):
        company_views = build_expansion_company_views(row, evidence_df)
        with st.expander(
            f"#{idx} {safe_text(row['suggested_service'], 'Unknown expansion')} | {safe_text(str(row['supporting_signal_count']), '0')} signals",
            expanded=False,
        ):
            company_cards_html = []
            for company_view in company_views:
                job_lines = []
                for job in company_view["jobs"]:
                    job_title = escape(safe_text(job.get("job_title"), "Unknown job title"))
                    salary = safe_text(job.get("base_salary"))
                    posted = safe_text(job.get("posted_date"), "Unknown")
                    job_line = f"{job_title}"
                    if salary:
                        job_line += f" | {escape(salary)}"
                    job_line += f" | {escape(posted)}"
                    job_lines.append(f'<div class="expansion-job-line">- {job_line}</div>')
                company_cards_html.append(
                    '<div class="expansion-company-box">'
                    f'<div class="expansion-company-title">{escape(company_view["company_name"])}</div>'
                    f'<div class="expansion-company-meta">{company_view["posting_count"]} related posting{"s" if int(company_view["posting_count"]) != 1 else ""} | Most recent: {escape(company_view["most_recent_posted_date"])}</div>'
                    + "".join(job_lines)
                    + '</div>'
                )

            st.markdown(
                (
                    '<div class="expansion-card">'
                    f'<div class="expansion-card-title">{escape(safe_text(row["suggested_service"], "Unknown expansion"))}</div>'
                    f'<div class="expansion-card-section"><div class="expansion-card-label">Service Description</div><div class="expansion-card-value">{escape(safe_text(row["service_description"], "No service description captured."))}</div></div>'
                    f'<div class="expansion-card-section"><div class="expansion-card-label">Companies Showing Interest</div><div class="expansion-card-value">{escape(safe_text(row["companies_showing_interest"], "No companies captured."))}</div></div>'
                    f'<div class="expansion-card-section"><div class="expansion-card-label">Pattern By Company</div><div class="expansion-card-value">{"".join(company_cards_html) if company_cards_html else "No company-specific posting pattern could be mapped from the current evidence."}</div></div>'
                    f'<div class="expansion-card-section"><div class="expansion-card-label">Typical Job Titles / Responsibilities Seen</div><div class="expansion-card-value"><strong>Job Titles:</strong> {escape(safe_text(row["sample_job_titles"], "No job titles captured."))}<br><strong>Responsibilities:</strong> {escape(safe_text(row["sample_responsibilities"], "No responsibilities captured."))}</div></div>'
                    '</div>'
                ),
                unsafe_allow_html=True,
            )

    with st.expander("Supporting evidence used for expansion analysis"):
        st.dataframe(evidence_df, use_container_width=True, hide_index=True)
    st.markdown("</div>", unsafe_allow_html=True)


def build_master_evidence_data():
    runs = runs_df()
    expansion_runs = expansion_runs_df()
    deep_dive_runs = deep_dive_runs_df()
    if runs.empty and expansion_runs.empty and deep_dive_runs.empty:
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
        evidence_df["source_origin"] = "Generate List"
        evidence_frames.append(evidence_df)

    for _, expansion_row in expansion_runs.iterrows():
        evidence_df = ensure_evidence_columns(load_df(expansion_row["evidence_json"]))
        if evidence_df.empty:
            continue
        evidence_df["source_run_id"] = expansion_row["id"]
        evidence_df["source_run_created_at"] = expansion_row["created_at"]
        evidence_df["source_services"] = expansion_row["services_text"]
        evidence_df["source_origin"] = "Potential Expansions"
        evidence_frames.append(evidence_df)

    for _, deep_dive_row in deep_dive_runs.iterrows():
        raw_deep_dive_df = load_df(deep_dive_row["evidence_json"])
        evidence_df = deep_dive_records_to_evidence_df(
            raw_deep_dive_df,
            deep_dive_row.get("matched_services_text"),
        )
        if evidence_df.empty:
            continue
        evidence_df["source_run_id"] = deep_dive_row["id"]
        evidence_df["source_run_created_at"] = deep_dive_row["created_at"]
        evidence_df["source_services"] = deep_dive_row["matched_services_text"]
        evidence_df["source_origin"] = "Expanded Search"
        evidence_frames.append(evidence_df)

    if not evidence_frames:
        return pd.DataFrame(columns=EVIDENCE_COLUMNS)

    master_evidence_df = pd.concat(evidence_frames, ignore_index=True)
    master_evidence_df["_source_created_sort"] = pd.to_datetime(master_evidence_df.get("source_run_created_at"), errors="coerce")
    master_evidence_df = master_evidence_df.sort_values(
        ["_source_created_sort", "match_score", "company_name", "job_title"],
        ascending=[False, False, True, True],
        na_position="last",
    )
    master_evidence_df = master_evidence_df.drop_duplicates(
        subset=["source_url", "company_name", "job_title", "matched_service"],
        keep="first",
    ).reset_index(drop=True)
    if "_source_created_sort" in master_evidence_df.columns:
        master_evidence_df = master_evidence_df.drop(columns=["_source_created_sort"])
    return master_evidence_df


def build_expansion_baseline_evidence(selected_service_names):
    master_evidence_df = build_master_evidence_data()
    if master_evidence_df.empty:
        return pd.DataFrame(columns=EVIDENCE_COLUMNS)

    selected_set = {safe_text(name) for name in selected_service_names if safe_text(name)}
    if not selected_set:
        return pd.DataFrame(columns=EVIDENCE_COLUMNS)

    baseline_df = ensure_evidence_columns(master_evidence_df).copy()
    if "source_services" not in baseline_df.columns:
        baseline_df["source_services"] = ""
    baseline_df = baseline_df[
        baseline_df.apply(
            lambda row: (
                safe_text(row.get("matched_service")) in selected_set
                or bool(selected_set.intersection(split_service_values(row.get("source_services"))))
            ),
            axis=1,
        )
    ].copy()
    if baseline_df.empty:
        return pd.DataFrame(columns=EVIDENCE_COLUMNS)

    return baseline_df.sort_values("match_score", ascending=False).reset_index(drop=True)


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


def build_expansion_company_signal_map():
    runs = expansion_runs_df()
    if runs.empty:
        return {}
    signal_map = {}
    for _, run in runs.iterrows():
        created_at = pd.to_datetime(run.get("created_at"), errors="coerce")
        expansion_df = load_df(run.get("expansion_json"))
        if expansion_df.empty:
            continue
        for _, exp_row in expansion_df.iterrows():
            companies = split_service_values(exp_row.get("companies_showing_interest"))
            signal_count = int(exp_row.get("supporting_signal_count") or 0)
            for company in companies:
                canonical = canonicalize_company_name(company)
                if not canonical:
                    continue
                entry = signal_map.setdefault(
                    canonical,
                    {"company_names": set(), "signal_mentions": 0, "latest_created_at": pd.NaT},
                )
                entry["company_names"].add(safe_text(company, "Unknown Company"))
                entry["signal_mentions"] += max(1, signal_count)
                if pd.notna(created_at):
                    if pd.isna(entry["latest_created_at"]) or created_at > entry["latest_created_at"]:
                        entry["latest_created_at"] = created_at
    return signal_map


def build_next_steps_company_table(evidence_df):
    if evidence_df.empty:
        return pd.DataFrame()

    temp = ensure_evidence_columns(evidence_df).copy()
    temp["canonical_company_name"] = temp["company_name"].apply(canonicalize_company_name)
    temp["posted_date_parsed"] = pd.to_datetime(temp["posted_date"], errors="coerce")
    expansion_signal_map = build_expansion_company_signal_map()
    rows = []
    now_ts = pd.Timestamp.now().normalize()

    for canonical_company, group in temp.groupby("canonical_company_name", dropna=False):
        job_rows = group.drop_duplicates(subset=["source_url", "job_title"], keep="first").copy()
        related_job_rows = job_rows[~job_rows["match_type"].isin(["None"])].copy()
        relevant_job_rows = related_job_rows[related_job_rows["match_type"].isin(["Direct", "Peripheral"])].copy()

        related_posting_count = len(related_job_rows)
        relevant_posting_count = len(relevant_job_rows)
        if related_posting_count == 0:
            continue

        matched_services = flatten_unique(group["matched_service"].tolist())
        display_company_name = choose_display_company_name(group["company_name"].tolist())
        likely_buyer_department = (
            pd.Series([x for x in group["buyer_department"] if pd.notna(x) and str(x).strip()]).mode().iloc[0]
            if any(pd.notna(group["buyer_department"]))
            else None
        )
        source_urls = flatten_unique(group["source_url"].tolist())[:5]
        salary_values = flatten_unique(group["base_salary"].tolist())
        salary_numeric_values = [value for value in [parse_salary_high_value(s) for s in salary_values] if value is not None]
        salary_signal = salary_values[0] if salary_values else None

        recent_dates = related_job_rows["posted_date_parsed"].dropna()
        most_recent_posted = recent_dates.max() if not recent_dates.empty else pd.NaT
        if pd.notna(most_recent_posted):
            age_days = max(0, (now_ts - most_recent_posted.normalize()).days)
            if age_days <= 7:
                recency_rank = 5
            elif age_days <= 14:
                recency_rank = 4
            elif age_days <= 30:
                recency_rank = 3
            elif age_days <= 60:
                recency_rank = 2
            else:
                recency_rank = 1
            most_recent_posted_text = most_recent_posted.strftime("%m/%d/%y")
        else:
            recency_rank = 0
            most_recent_posted_text = "Unknown"

        salary_rank = 0
        if salary_values:
            salary_rank += 1
        highest_salary = max(salary_numeric_values) if salary_numeric_values else None
        if highest_salary is not None:
            if highest_salary >= 150000:
                salary_rank += 4
            elif highest_salary >= 100000:
                salary_rank += 3
            elif highest_salary >= 70000:
                salary_rank += 2
            else:
                salary_rank += 1

        expansion_entry = expansion_signal_map.get(canonical_company, {})
        expansion_signal_count = int(expansion_entry.get("signal_mentions") or 0)
        expansion_rank = 1 if expansion_signal_count else 0
        if expansion_signal_count >= 12:
            expansion_rank = 4
        elif expansion_signal_count >= 6:
            expansion_rank = 3
        elif expansion_signal_count >= 2:
            expansion_rank = 2

        why_parts = [
            f"{relevant_posting_count} relevant posting{'s' if relevant_posting_count != 1 else ''} found",
        ]
        if related_posting_count > relevant_posting_count:
            why_parts.append(
                f"{related_posting_count} total related posting{'s' if related_posting_count != 1 else ''} found"
            )
        if most_recent_posted_text != "Unknown":
            why_parts.append(f"most recent posting dated {most_recent_posted_text}")
        if salary_signal:
            why_parts.append(f"explicit base salary disclosed ({salary_signal})")
        if expansion_signal_count:
            why_parts.append(f"also highlighted in saved expansion analysis ({expansion_signal_count} signal{'s' if expansion_signal_count != 1 else ''})")

        suggested_next_step = (
            f"Prioritize outreach to the {likely_buyer_department} team and reference the matching postings."
            if likely_buyer_department
            else "Prioritize outreach to the team responsible for this function and reference the matching postings."
        )

        rows.append(
            {
                "buyer_company": safe_text(display_company_name, "Unknown Company"),
                "relevant_posting_count": relevant_posting_count,
                "related_posting_count": related_posting_count,
                "most_recent_posted_date": most_recent_posted_text,
                "salary_signal": salary_signal or "Not disclosed",
                "matched_services": "; ".join(matched_services),
                "likely_buyer_department_general": likely_buyer_department,
                "why_highlighted": ". ".join(why_parts) + ".",
                "suggested_next_step": suggested_next_step,
                "source_urls": " | ".join(source_urls),
                "_canonical_company_name": canonical_company,
                "_recency_rank": recency_rank,
                "_salary_rank": salary_rank,
                "_expansion_rank": expansion_rank,
                "_expansion_signal_count": expansion_signal_count,
            }
        )

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows).sort_values(
        ["relevant_posting_count", "related_posting_count", "_expansion_rank", "_expansion_signal_count", "_recency_rank", "_salary_rank", "buyer_company"],
        ascending=[False, False, False, False, False, False, True],
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
                Paragraph(escape(f"Service description: {row['service_description'] or 'Unknown'}"), styles["Normal"]),
                Paragraph(escape(f"Frequency: {row['supporting_signal_count']}"), styles["Normal"]),
                Paragraph(escape(f"Connected current services: {row['connected_current_services']}"), styles["Normal"]),
                Paragraph(escape(f"Companies showing interest: {row['companies_showing_interest']}"), styles["Normal"]),
                Paragraph(escape(f"Typical job titles seen: {row['sample_job_titles']}"), styles["Normal"]),
                Paragraph(escape(f"Typical responsibilities seen: {row['sample_responsibilities']}"), styles["Normal"]),
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


def clear_reset_query_param():
    try:
        if "reset_token" in st.query_params:
            del st.query_params["reset_token"]
    except Exception:
        pass


def render_auth_reset_panel(reset_token):
    reset_record = get_password_reset_record(reset_token)
    if not reset_record:
        st.error("This password reset link is invalid or has expired.")
        if st.button("Back to Sign In", key="back_from_invalid_reset"):
            clear_reset_query_param()
            st.rerun()
        return

    st.markdown("**Reset Password**")
    st.caption(f"Reset password for {safe_text(reset_record.get('email'))}")
    with st.form("reset_password_form"):
        new_password = st.text_input("New Password", type="password", key="reset_password_1")
        confirm_password = st.text_input("Confirm New Password", type="password", key="reset_password_2")
        submitted = st.form_submit_button("Save New Password")
    if submitted:
        if not new_password.strip() or not confirm_password.strip():
            st.error("Please complete both password fields.")
        elif new_password != confirm_password:
            st.error("The passwords do not match.")
        elif len(new_password) < 8:
            st.error("Use a password with at least 8 characters.")
        else:
            update_user_password(reset_record["user_id"], new_password)
            mark_password_reset_used(reset_record["id"])
            clear_reset_query_param()
            st.success("Password updated. You can sign in now.")
            st.rerun()
    if st.button("Back to Sign In", key="back_from_reset"):
        clear_reset_query_param()
        st.rerun()


def render_auth_account_panel():
    auth_mode = st.session_state.get("landing_auth_mode", "Create Account")
    signup_first = auth_mode == "Create Account"
    tab_labels = ["Create Account", "Sign In"] if signup_first else ["Sign In", "Create Account"]
    auth_tabs = st.tabs(tab_labels)

    if signup_first:
        signup_tab = auth_tabs[0]
        login_tab = auth_tabs[1]
    else:
        login_tab = auth_tabs[0]
        signup_tab = auth_tabs[1]

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
        with st.expander("Forgot password?"):
            with st.form("forgot_password_form"):
                reset_email = st.text_input("Email", key="forgot_password_email")
                reset_submit = st.form_submit_button("Send reset link")
            if reset_submit:
                try:
                    send_password_reset_email(reset_email)
                    st.success("If that email is registered, a password reset link has been sent.")
                except ValueError as exc:
                    st.error(str(exc))
                except Exception:
                    st.error("Password reset email could not be sent right now.")

    with signup_tab:
        with st.form("signup_form"):
            full_name = st.text_input("Full name", key="signup_name")
            email = st.text_input(
                "Email",
                value=safe_text(st.session_state.get("landing_signup_email")),
                key="signup_email",
            )
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
                    st.session_state.pop("landing_signup_email", None)
                    set_current_user(user)
                    st.success("Account created. You can use starter demo credits or subscribe below.")
                    st.rerun()
                except ValueError as exc:
                    st.error(str(exc))


def render_landing_signup_capture():
    with st.form("landing_capture_form", border=False):
        email_col, button_col = st.columns([1.8, 0.95], gap="small")
        with email_col:
            landing_email = st.text_input(
                "Start free with your email",
                placeholder="Enter your email address",
                key="landing_capture_email",
                label_visibility="collapsed",
            )
        with button_col:
            start_free = st.form_submit_button(
                "Start Free",
                type="primary",
                use_container_width=True,
            )

    if start_free:
        if not landing_email.strip():
            st.error("Enter your email address to get started.")
        else:
            st.session_state["landing_signup_email"] = landing_email.strip()
            st.session_state["landing_auth_mode"] = "Create Account"
            st.query_params["auth"] = "signup"
            st.rerun()


def page_auth():
    reset_token = st.query_params.get("reset_token")
    auth_view = safe_text(st.query_params.get("auth")).lower()
    signup_email_prefill = safe_text(st.query_params.get("signup_email"))
    if auth_view == "signup" and signup_email_prefill:
        st.session_state["landing_signup_email"] = signup_email_prefill
    st.markdown(
        """
        <style>
        [data-testid="stAppViewBlockContainer"] {
            padding-top: 0.35rem;
        }
        .landing-topbar {
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            z-index: 90;
            width: 100%;
            background: rgba(15, 23, 42, 0.98);
            border-bottom: 1px solid rgba(255,255,255,0.05);
            backdrop-filter: blur(10px);
        }
        .landing-topbar-inner {
            max-width: 1280px;
            margin: 0 auto;
            min-height: 84px;
            padding: 0 1.8rem;
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 1rem;
        }
        .landing-brand {
            font-size: 1.32rem;
            font-weight: 850;
            color: #eff6ff;
            letter-spacing: -0.02em;
        }
        .landing-nav-right {
            display: flex;
            align-items: center;
            gap: 0.85rem;
        }
        .landing-nav-link {
            color: #dbeafe !important;
            text-decoration: none !important;
            font-weight: 650;
        }
        .landing-topbar-cta {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            min-width: 172px;
            height: 54px;
            padding: 0 1.35rem;
            border-radius: 999px;
            text-decoration: none !important;
            background: var(--brand-blue);
            color: #0f172a !important;
            font-weight: 750;
            border: 1px solid var(--brand-blue);
        }
        .auth-hero {
            position: relative;
            min-height: 620px;
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
            .auth-hero {
                min-height: 420px;
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
        .landing-wrap {
            max-width: none;
            margin: 0;
            padding: 0 0 2rem 0;
        }
        .landing-page {
            max-width: 1280px;
            margin: 0 auto;
            padding: 128px 1.8rem 2.5rem 1.8rem;
        }
        .landing-hero-shell {
            display: grid;
            grid-template-columns: minmax(0, 0.9fr) minmax(0, 1.1fr);
            gap: 3rem;
            align-items: start;
            min-height: 640px;
        }
        .landing-copy {
            max-width: 40rem;
            padding-top: 4.4rem;
        }
        .landing-band {
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: 1.2rem;
            padding: 1.2rem 1.15rem;
            margin-bottom: 1rem;
        }
        .landing-band.soft {
            background: rgba(96, 165, 250, 0.08);
        }
        .landing-band.deep {
            background: rgba(15, 23, 42, 0.42);
        }
        .landing-band.clear {
            background: rgba(255,255,255,0.02);
        }
        .landing-kicker {
            display: inline-block;
            margin-bottom: 0.85rem;
            padding: 0.42rem 0.7rem;
            border-radius: 999px;
            background: rgba(96, 165, 250, 0.14);
            color: #dbeafe;
            font-size: 0.8rem;
            font-weight: 700;
            letter-spacing: 0.01em;
        }
        .landing-title {
            font-size: clamp(3.2rem, 5vw, 4.7rem);
            line-height: 1.02;
            font-weight: 560;
            color: #eff6ff;
            margin-bottom: 1.25rem;
            max-width: 10.6ch;
            letter-spacing: -0.04em;
        }
        .landing-subtitle {
            color: #cbd5e1;
            font-size: 1.46rem;
            line-height: 1.42;
            max-width: 26ch;
            margin-bottom: 1.85rem;
            font-weight: 400;
        }
        .landing-proof-grid,
        .landing-work-grid,
        .landing-feature-grid,
        .landing-outcome-grid {
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 0.9rem;
            margin-bottom: 1.1rem;
        }
        .landing-card {
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: 1rem;
            background: rgba(15, 23, 42, 0.40);
            padding: 1rem 1rem 0.95rem 1rem;
            box-shadow: 0 14px 34px rgba(15, 23, 42, 0.10);
        }
        .landing-card-title {
            color: #eff6ff;
            font-size: 1.02rem;
            font-weight: 800;
            margin-bottom: 0.35rem;
        }
        .landing-card-copy {
            color: #cbd5e1;
            line-height: 1.6;
        }
        .landing-section {
            margin-top: 1rem;
            margin-bottom: 1rem;
        }
        .landing-anchor-row {
            display: flex;
            gap: 1rem;
            flex-wrap: wrap;
            margin-bottom: 0.8rem;
        }
        .landing-signup-form {
            display: flex;
            align-items: center;
            gap: 0.8rem;
            width: 100%;
            max-width: 760px;
        }
        .landing-signup-input-wrap {
            flex: 1 1 auto;
            display: flex;
            align-items: center;
            min-height: 64px;
            padding: 0 1.25rem;
            border-radius: 999px;
            background: rgba(255,255,255,0.98);
            border: 1px solid rgba(226, 232, 240, 0.84);
            box-shadow: 0 16px 40px rgba(2, 6, 23, 0.12);
        }
        .landing-signup-input {
            width: 100%;
            border: 0;
            background: transparent;
            color: #0f172a;
            font-size: 1rem;
            outline: none;
        }
        .landing-signup-input::placeholder {
            color: #64748b;
        }
        .landing-signup-button {
            flex: 0 0 auto;
            min-width: 190px;
            height: 64px;
            padding: 0 1.4rem;
            border-radius: 999px;
            border: 0;
            background: var(--brand-blue);
            color: #0f172a;
            font-size: 1rem;
            font-weight: 760;
            cursor: pointer;
            box-shadow: 0 16px 36px rgba(96, 165, 250, 0.22);
        }
        .landing-anchor-button {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            min-width: 220px;
            padding: 0.95rem 1.25rem;
            border-radius: 0.95rem;
            text-decoration: none !important;
            font-weight: 700;
            font-size: 1rem;
            transition: all 0.15s ease;
        }
        .landing-anchor-button.primary {
            background: var(--brand-blue);
            color: #0f172a !important;
            border: 1px solid var(--brand-blue);
        }
        .landing-anchor-button.secondary {
            background: transparent;
            color: #dbeafe !important;
            border: 1px solid var(--brand-border);
        }
        .landing-anchor-button:hover {
            transform: translateY(-1px);
        }
        .landing-section-title {
            font-size: 1.5rem;
            font-weight: 850;
            color: #eff6ff;
            margin-bottom: 0.25rem;
        }
        .landing-section-copy {
            color: #cbd5e1;
            line-height: 1.55;
            margin-bottom: 0.75rem;
            max-width: 58ch;
        }
        .landing-product {
            border: 1px solid var(--brand-border);
            border-radius: 1.15rem;
            background: linear-gradient(180deg, rgba(96, 165, 250, 0.10), rgba(255,255,255,0.02));
            padding: 1.15rem 1.1rem 1rem 1.1rem;
            margin-bottom: 1rem;
        }
        .landing-product-title {
            font-size: 1.2rem;
            font-weight: 850;
            color: #eff6ff;
            margin-bottom: 0.35rem;
        }
        .landing-product-copy {
            color: #dbeafe;
            line-height: 1.6;
            margin-bottom: 0.75rem;
            max-width: 70ch;
        }
        .landing-product-bullets {
            color: #cbd5e1;
            line-height: 1.7;
        }
        .landing-bullet {
            margin-bottom: 0.32rem;
        }
        .landing-feature-layout {
            display: grid;
            grid-template-columns: 0.9fr 1.1fr;
            gap: 1rem;
            align-items: center;
        }
        .landing-feature-copy-shell {
            min-width: 0;
        }
        .landing-feature-visual-shell {
            min-width: 0;
        }
        .landing-mockup-wrap {
            position: relative;
            min-height: 620px;
            padding: 0;
            overflow: hidden;
            display: flex;
            align-items: flex-start;
        }
        .landing-hero-visual {
            min-height: 620px;
            display: flex;
            align-items: flex-start;
            justify-content: flex-start;
            padding-top: 1.15rem;
        }
        .hero-clean-shell {
            position: relative;
            min-height: 620px;
            width: 100%;
            border-radius: 2rem;
            background:
                radial-gradient(circle at 24% 18%, rgba(125, 211, 252, 0.28) 0%, rgba(125, 211, 252, 0.00) 34%),
                radial-gradient(circle at 78% 22%, rgba(96, 165, 250, 0.24) 0%, rgba(96, 165, 250, 0.00) 30%),
                linear-gradient(180deg, rgba(30, 41, 59, 0.10), rgba(15, 23, 42, 0.00)),
                #0b1220;
            border: 1px solid rgba(255,255,255,0.08);
            box-shadow: inset 0 1px 0 rgba(255,255,255,0.03);
            overflow: hidden;
        }
        .hero-clean-orbit {
            position: absolute;
            border: 1px solid rgba(148, 163, 184, 0.18);
            border-radius: 999px;
            pointer-events: none;
        }
        .hero-clean-orbit.orbit-a {
            width: 86%;
            height: 62%;
            left: 6%;
            top: 11%;
        }
        .hero-clean-orbit.orbit-b {
            width: 66%;
            height: 46%;
            left: 19%;
            bottom: 9%;
        }
        .hero-clean-browser {
            position: absolute;
            inset: 3.25rem 2.5rem 3.1rem 2.5rem;
            border-radius: 1.7rem;
            background: rgba(255,255,255,0.95);
            box-shadow: 0 36px 90px rgba(2, 6, 23, 0.34);
            overflow: hidden;
        }
        .hero-clean-bar {
            height: 3rem;
            display: flex;
            align-items: center;
            gap: 0.45rem;
            padding: 0 1rem;
            background: rgba(248, 250, 252, 0.96);
            border-bottom: 1px solid rgba(148, 163, 184, 0.18);
        }
        .hero-clean-dot {
            width: 0.55rem;
            height: 0.55rem;
            border-radius: 999px;
            background: #d4dbe6;
        }
        .hero-clean-body {
            position: relative;
            height: calc(100% - 3rem);
            padding: 1.3rem;
            background:
                linear-gradient(180deg, #f8fbff 0%, #eef5ff 100%);
        }
        .hero-clean-panel {
            border-radius: 1.35rem;
            background: linear-gradient(135deg, #1d4ed8 0%, #60a5fa 100%);
            box-shadow: 0 20px 50px rgba(37, 99, 235, 0.24);
        }
        .hero-clean-panel-large {
            height: 34%;
            margin-bottom: 1rem;
        }
        .hero-clean-grid {
            display: grid;
            grid-template-columns: 1.05fr 1fr;
            gap: 0.95rem;
            height: calc(66% - 1rem);
        }
        .hero-clean-card {
            border-radius: 1.15rem;
            background: rgba(255,255,255,0.92);
            border: 1px solid rgba(148, 163, 184, 0.20);
            box-shadow: 0 16px 40px rgba(148, 163, 184, 0.16);
        }
        .hero-clean-card.tall {
            grid-row: span 2;
            background:
                linear-gradient(180deg, rgba(96, 165, 250, 0.16), rgba(96, 165, 250, 0.04)),
                rgba(255,255,255,0.94);
        }
        .hero-clean-card.wide {
            background:
                linear-gradient(135deg, rgba(29, 78, 216, 0.12), rgba(125, 211, 252, 0.04)),
                rgba(255,255,255,0.95);
        }
        .hero-clean-float {
            position: absolute;
            width: 12rem;
            height: 9rem;
            border-radius: 1.3rem;
            background: rgba(255,255,255,0.92);
            border: 1px solid rgba(148, 163, 184, 0.14);
            box-shadow: 0 22px 60px rgba(2, 6, 23, 0.18);
        }
        .hero-clean-float.float-top {
            top: 6.5rem;
            right: 0.6rem;
        }
        .hero-clean-float.float-bottom {
            left: 0.8rem;
            bottom: 1.2rem;
            width: 10.5rem;
            height: 7.6rem;
        }
        .epic-scene {
            position: absolute;
            inset: 0;
            border-radius: 1.4rem;
            overflow: hidden;
            background:
                radial-gradient(circle at 50% 78%, rgba(96, 165, 250, 0.40) 0%, rgba(96, 165, 250, 0.00) 26%),
                linear-gradient(180deg, #040814 0%, #09122a 34%, #11234a 62%, #0d1b37 100%);
            box-shadow: inset 0 0 140px rgba(15, 23, 42, 0.55);
        }
        .epic-nebula {
            position: absolute;
            border-radius: 999px;
            filter: blur(26px);
            opacity: 0.9;
            mix-blend-mode: screen;
        }
        .epic-nebula.nebula-a {
            width: 56%;
            height: 34%;
            top: 5%;
            left: 7%;
            background: radial-gradient(circle, rgba(96, 165, 250, 0.85) 0%, rgba(96, 165, 250, 0.00) 75%);
        }
        .epic-nebula.nebula-b {
            width: 52%;
            height: 30%;
            top: 0%;
            right: 4%;
            background: radial-gradient(circle, rgba(29, 78, 216, 0.75) 0%, rgba(29, 78, 216, 0.00) 78%);
        }
        .epic-nebula.nebula-c {
            width: 38%;
            height: 24%;
            bottom: 18%;
            right: 12%;
            background: radial-gradient(circle, rgba(125, 211, 252, 0.55) 0%, rgba(125, 211, 252, 0.00) 80%);
        }
        .epic-horizon {
            position: absolute;
            left: 0;
            right: 0;
            bottom: 24%;
            height: 22%;
            background: radial-gradient(circle at 50% 100%, rgba(96, 165, 250, 0.55) 0%, rgba(96, 165, 250, 0.0) 70%);
            filter: blur(20px);
        }
        .epic-cityline {
            position: absolute;
            left: 7%;
            right: 7%;
            bottom: 19%;
            height: 18%;
            background:
                linear-gradient(90deg, transparent 0 5%, #071120 5% 8%, transparent 8% 13%, #071120 13% 16%, transparent 16% 20%, #071120 20% 24%, transparent 24% 32%, #071120 32% 38%, transparent 38% 43%, #071120 43% 49%, transparent 49% 56%, #071120 56% 61%, transparent 61% 70%, #071120 70% 76%, transparent 76% 83%, #071120 83% 88%, transparent 88% 100%);
            clip-path: polygon(0 100%, 0 62%, 5% 62%, 5% 44%, 9% 44%, 9% 70%, 13% 70%, 13% 36%, 18% 36%, 18% 66%, 24% 66%, 24% 40%, 30% 40%, 30% 74%, 38% 74%, 38% 30%, 44% 30%, 44% 68%, 52% 68%, 52% 38%, 59% 38%, 59% 74%, 68% 74%, 68% 46%, 74% 46%, 74% 64%, 82% 64%, 82% 34%, 88% 34%, 88% 70%, 94% 70%, 94% 52%, 100% 52%, 100% 100%);
            opacity: 0.92;
        }
        .epic-citadel {
            position: absolute;
            left: 50%;
            transform: translateX(-50%);
            bottom: 23%;
            width: 24%;
            height: 34%;
            background: linear-gradient(180deg, rgba(15, 23, 42, 0.10), rgba(3, 8, 20, 0.94));
            clip-path: polygon(0 100%, 6% 52%, 17% 52%, 17% 28%, 24% 28%, 24% 10%, 31% 10%, 31% 36%, 38% 36%, 38% 0, 49% 0, 49% 30%, 56% 30%, 56% 12%, 64% 12%, 64% 44%, 73% 44%, 73% 20%, 82% 20%, 82% 56%, 94% 56%, 100% 100%);
            opacity: 0.96;
        }
        .epic-citadel.large {
            width: 28%;
            height: 38%;
        }
        .epic-grid-floor {
            position: absolute;
            left: -10%;
            right: -10%;
            bottom: -2%;
            height: 28%;
            background:
                linear-gradient(180deg, rgba(96, 165, 250, 0.00), rgba(96, 165, 250, 0.20)),
                linear-gradient(90deg, rgba(96, 165, 250, 0.14) 1px, transparent 1px),
                linear-gradient(180deg, rgba(96, 165, 250, 0.12) 1px, transparent 1px);
            background-size: 100% 100%, 38px 100%, 100% 28px;
            transform: perspective(700px) rotateX(78deg);
            transform-origin: center bottom;
            opacity: 0.46;
        }
        .epic-mountain-left,
        .epic-mountain-right {
            position: absolute;
            bottom: 18%;
            width: 28%;
            height: 30%;
            background: linear-gradient(180deg, rgba(30, 41, 59, 0.20), rgba(2, 6, 23, 0.92));
            opacity: 0.92;
        }
        .epic-mountain-left {
            left: 0;
            clip-path: polygon(0 100%, 0 48%, 26% 56%, 42% 28%, 58% 48%, 74% 16%, 100% 60%, 100% 100%);
        }
        .epic-mountain-right {
            right: 0;
            clip-path: polygon(0 100%, 0 60%, 24% 22%, 43% 48%, 61% 18%, 78% 50%, 100% 42%, 100% 100%);
        }
        .epic-signal-beam {
            position: absolute;
            left: 50%;
            transform: translateX(-50%);
            bottom: 28%;
            width: 16px;
            height: 48%;
            background: linear-gradient(180deg, rgba(125, 211, 252, 0.00) 0%, rgba(125, 211, 252, 0.85) 42%, rgba(191, 219, 254, 0.10) 100%);
            filter: blur(3px);
            opacity: 0.9;
        }
        .epic-signal-beam.hero-beam {
            height: 58%;
        }
        .epic-glow-trail {
            position: absolute;
            left: 18%;
            top: 16%;
            width: 54%;
            height: 2px;
            background: linear-gradient(90deg, rgba(125, 211, 252, 0.0), rgba(125, 211, 252, 0.85), rgba(125, 211, 252, 0.0));
            transform: rotate(-18deg);
            box-shadow: 0 0 18px rgba(125, 211, 252, 0.5);
        }
        .epic-panel {
            position: absolute;
            z-index: 3;
            border: 1px solid rgba(191, 219, 254, 0.18);
            background: linear-gradient(180deg, rgba(7, 14, 28, 0.90), rgba(13, 27, 55, 0.82));
            backdrop-filter: blur(8px);
            box-shadow: 0 22px 44px rgba(2, 6, 23, 0.35);
            color: #eff6ff;
        }
        .epic-panel.primary-panel,
        .epic-panel.hero-core-panel {
            left: 6%;
            top: 9%;
            width: 62%;
            border-radius: 1.3rem;
            padding: 1rem 1rem 0.95rem;
        }
        .epic-panel.hero-core-panel {
            width: 60%;
            top: 13%;
        }
        .epic-panel.side-panel {
            min-width: 180px;
            border-radius: 1rem;
            padding: 0.82rem 0.9rem;
        }
        .epic-panel.top-right-panel {
            top: 1.4rem;
            right: 0.5rem;
        }
        .epic-panel.bottom-left-panel {
            left: 0.5rem;
            bottom: 0.8rem;
        }
        .epic-panel-kicker {
            color: #93c5fd;
            font-size: 0.72rem;
            font-weight: 800;
            text-transform: uppercase;
            letter-spacing: 0.04em;
            margin-bottom: 0.24rem;
        }
        .epic-panel-title,
        .epic-score-title {
            color: #eff6ff;
            font-size: 1.05rem;
            font-weight: 800;
            line-height: 1.2;
            margin-bottom: 0.2rem;
        }
        .epic-panel-copy,
        .epic-score-copy {
            color: #cbd5e1;
            font-size: 0.84rem;
            line-height: 1.5;
        }
        .epic-chip-row {
            display: flex;
            flex-wrap: wrap;
            gap: 0.55rem;
            margin-top: 0.72rem;
        }
        .epic-chip {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            padding: 0.42rem 0.7rem;
            border-radius: 999px;
            background: rgba(96, 165, 250, 0.14);
            border: 1px solid rgba(96, 165, 250, 0.18);
            color: #dbeafe;
            font-size: 0.74rem;
            font-weight: 700;
        }
        .epic-mini-grid {
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 0.6rem;
            margin-top: 0.72rem;
        }
        .epic-mini-card {
            border-radius: 0.95rem;
            padding: 0.72rem 0.8rem;
            background: rgba(255,255,255,0.06);
            border: 1px solid rgba(191, 219, 254, 0.12);
        }
        .epic-mini-card span {
            display: block;
            color: #93c5fd;
            font-size: 0.68rem;
            font-weight: 800;
            text-transform: uppercase;
            letter-spacing: 0.04em;
            margin-bottom: 0.22rem;
        }
        .epic-mini-card strong {
            color: #eff6ff;
            font-size: 0.95rem;
            font-weight: 800;
        }
        .epic-score-card {
            display: flex;
            align-items: center;
            gap: 0.8rem;
            margin-top: 0.3rem;
        }
        .epic-rank-chip {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            width: 2.5rem;
            height: 2.5rem;
            border-radius: 999px;
            background: linear-gradient(135deg, #1d4ed8 0%, #7dd3fc 100%);
            color: #eff6ff;
            font-weight: 900;
            box-shadow: 0 0 28px rgba(96, 165, 250, 0.32);
        }
        .epic-stack-card {
            margin-top: 0.72rem;
            border-radius: 1rem;
            overflow: hidden;
            border: 1px solid rgba(191, 219, 254, 0.12);
            background: rgba(255,255,255,0.05);
        }
        .epic-stack-row {
            display: grid;
            grid-template-columns: 1fr 0.3fr;
            gap: 0.6rem;
            align-items: center;
            padding: 0.72rem 0.82rem;
            color: #eff6ff;
            border-top: 1px solid rgba(191, 219, 254, 0.08);
            font-size: 0.84rem;
        }
        .epic-stack-row:first-child {
            border-top: 0;
        }
        .epic-stack-row strong {
            text-align: right;
            color: #93c5fd;
            font-size: 0.94rem;
        }
        .landing-mockup-hero .epic-scene,
        .landing-mockup-next .epic-scene,
        .landing-mockup-list .epic-scene,
        .landing-mockup-expansion .epic-scene {
            border: 1px solid rgba(255,255,255,0.08);
        }
        @media (max-width: 1100px) {
            .epic-panel.primary-panel,
            .epic-panel.hero-core-panel {
                width: 72%;
            }
            .epic-mini-grid {
                grid-template-columns: 1fr;
            }
        }
        @media (max-width: 760px) {
            .epic-panel.primary-panel,
            .epic-panel.hero-core-panel {
                position: relative;
                left: auto;
                top: auto;
                width: auto;
                margin: 1rem 0.55rem 0;
            }
            .epic-panel.top-right-panel,
            .epic-panel.bottom-left-panel {
                position: relative;
                top: auto;
                right: auto;
                left: auto;
                bottom: auto;
                margin: 0.75rem 0.55rem 0;
            }
            .landing-mockup-wrap {
                min-height: 420px;
            }
        }
        .landing-auth-shell {
            border: 1px solid var(--brand-border);
            border-radius: 1.15rem;
            background: linear-gradient(180deg, rgba(96, 165, 250, 0.10), rgba(255,255,255,0.02));
            padding: 1.15rem 1.1rem 1rem 1.1rem;
            margin-top: 1.1rem;
            scroll-margin-top: 1rem;
        }
        .landing-auth-title {
            font-size: 1.45rem;
            font-weight: 850;
            color: #eff6ff;
            margin-bottom: 0.25rem;
        }
        .landing-auth-copy {
            color: #cbd5e1;
            line-height: 1.6;
            margin-bottom: 0.85rem;
        }
        .auth-page-shell {
            max-width: 1120px;
            margin: 0 auto;
        }
        .auth-page-topbar {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 1rem;
            margin-bottom: 1rem;
        }
        .auth-page-back {
            color: #dbeafe !important;
            text-decoration: none !important;
            font-weight: 650;
        }
        @media (max-width: 1100px) {
            .landing-page {
                padding-top: 108px;
            }
            .landing-hero-shell {
                grid-template-columns: 1fr;
                min-height: auto;
                gap: 1.75rem;
            }
            .landing-copy {
                max-width: 46rem;
                padding-top: 0.25rem;
            }
            .landing-proof-grid,
            .landing-work-grid,
            .landing-feature-grid,
            .landing-outcome-grid {
                grid-template-columns: 1fr;
            }
            .landing-feature-layout {
                grid-template-columns: 1fr;
            }
            .landing-title {
                max-width: 11.2ch;
            }
            .landing-subtitle {
                max-width: 29ch;
            }
            .landing-mockup-wrap,
            .hero-clean-shell,
            .landing-hero-visual {
                min-height: 420px;
            }
            .hero-clean-browser {
                inset: 2rem 1.35rem 2.1rem 1.35rem;
            }
            .hero-clean-float.float-top {
                right: 0.5rem;
                width: 10rem;
                height: 7.4rem;
            }
            .hero-clean-float.float-bottom {
                width: 8.8rem;
                height: 6.5rem;
            }
        }
        @media (max-width: 760px) {
            .landing-topbar-inner {
                min-height: 74px;
                padding: 0 0.95rem;
            }
            .landing-brand {
                font-size: 1.16rem;
            }
            .landing-topbar-cta {
                min-width: 136px;
                padding: 0.78rem 1rem;
            }
            .landing-page {
                padding: 98px 0.95rem 1.8rem 0.95rem;
            }
            .landing-title {
                font-size: clamp(2.65rem, 12vw, 3.9rem);
                max-width: 10.2ch;
            }
            .landing-subtitle {
                font-size: 1.1rem;
                max-width: 24ch;
            }
            .landing-signup-form {
                flex-direction: column;
                align-items: stretch;
                gap: 0.7rem;
            }
            .landing-signup-button {
                width: 100%;
                min-width: 0;
            }
            .hero-clean-browser {
                inset: 1.25rem 0.9rem 1.35rem 0.9rem;
            }
            .hero-clean-grid {
                gap: 0.7rem;
            }
            .hero-clean-float.float-top {
                top: 5.4rem;
                width: 8rem;
                height: 5.8rem;
            }
            .hero-clean-float.float-bottom {
                width: 7rem;
                height: 5rem;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.markdown('<div class="landing-wrap">', unsafe_allow_html=True)
    if reset_token:
        left, right = st.columns([1.02, 1.18], gap="large")
        with left:
            st.markdown(f'<div class="landing-title" style="font-size:2.8rem; max-width:10ch;">Reset your password</div>', unsafe_allow_html=True)
            st.markdown('<div class="landing-subtitle">Use the secure link from your email to set a new password and get back into your account.</div>', unsafe_allow_html=True)
            st.markdown('<div class="landing-auth-shell">', unsafe_allow_html=True)
            render_auth_reset_panel(reset_token)
            st.markdown("</div>", unsafe_allow_html=True)
        with right:
            st.markdown(auth_space_scene_html(), unsafe_allow_html=True)
    elif auth_view in {"signup", "signin"}:
        st.session_state["landing_auth_mode"] = "Create Account" if auth_view == "signup" else "Sign In"
        st.markdown('<div class="auth-page-shell">', unsafe_allow_html=True)
        st.markdown(
            f"""
            <div class="auth-page-topbar">
                <div class="landing-brand">{APP_NAME}</div>
                <a class="auth-page-back" href="?">Back to site</a>
            </div>
            """,
            unsafe_allow_html=True,
        )
        left, right = st.columns([0.98, 1.12], gap="large")
        with left:
            page_title = "Create your account" if auth_view == "signup" else "Sign in to your account"
            page_copy = (
                "Use your email to set up your account and start building a cleaner market view."
                if auth_view == "signup"
                else "Sign in to continue working with your saved services, lists, and market analysis."
            )
            st.markdown(f'<div class="landing-title" style="font-size:2.8rem; max-width:11ch;">{page_title}</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="landing-subtitle">{page_copy}</div>', unsafe_allow_html=True)
            st.markdown('<div class="landing-auth-shell">', unsafe_allow_html=True)
            render_auth_account_panel()
            st.markdown("</div>", unsafe_allow_html=True)
        with right:
            st.markdown(auth_space_scene_html(), unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)
    else:
        st.markdown(
            f"""
            <div class="landing-topbar">
                <div class="landing-topbar-inner">
                    <div class="landing-brand">{APP_NAME}</div>
                    <div class="landing-nav-right">
                        <a class="landing-topbar-cta" href="?auth=signup">Start Free</a>
                    </div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.markdown(
            f"""
            <section class="landing-page">
                <div class="landing-hero-shell">
                    <div class="landing-copy">
                        <div class="landing-title">Turn public market signals into your next opportunities</div>
                        <div class="landing-subtitle">Find buyer demand. Rank opportunities. Spot service gaps.</div>
                        <form class="landing-signup-form" method="get">
                            <input type="hidden" name="auth" value="signup" />
                            <div class="landing-signup-input-wrap">
                                <input
                                    class="landing-signup-input"
                                    type="email"
                                    name="signup_email"
                                    placeholder="Enter your email address"
                                    aria-label="Enter your email address"
                                />
                            </div>
                            <button class="landing-signup-button" type="submit">Start Free</button>
                        </form>
                    </div>
                    <div class="landing-hero-visual">
                        {landing_marketing_mockup_html("hero")}
                    </div>
                </div>
            </section>
            """,
            unsafe_allow_html=True,
        )

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
            if st.button("Refresh Billing Status", type="primary"):
                updated = sync_user_billing(user)
                set_current_user(updated)
                st.success("Billing status refreshed.")
                st.rerun()
        with right:
            if st.button("Open Billing Portal", type="primary"):
                try:
                    url = billing_portal_url(user)
                    st.markdown(f"[Open Stripe Billing Portal]({url})")
                except Exception as exc:
                    st.error(f"Billing portal is not available yet: {exc}")
    else:
        st.info("To enable Stripe, set `STRIPE_SECRET_KEY`, `STRIPE_PRICE_ID_STARTER`, `STRIPE_PRICE_ID_PRO`, and `APP_BASE_URL`.")


def page_dashboard():
    st.title(APP_NAME)
    st.subheader("Simple market view and next actions")
    user = current_user()
    svc = services_df()
    runs = runs_df()
    master_evidence_df = build_master_evidence_data()
    next_steps_df = build_next_steps_company_table(master_evidence_df) if not master_evidence_df.empty else pd.DataFrame()

    top_buyer_company = safe_text(next_steps_df.iloc[0]["buyer_company"], "None yet") if not next_steps_df.empty else "None yet"
    multiple_posting_signals = int((next_steps_df["relevant_posting_count"] > 1).sum()) if not next_steps_df.empty else 0
    freshest_signal = "None yet"
    if not master_evidence_df.empty:
        freshest_dates = pd.to_datetime(master_evidence_df["posted_date"], errors="coerce").dropna()
        if not freshest_dates.empty:
            freshest_signal = freshest_dates.max().strftime("%m/%d/%y")
    most_recent_list = (
        format_short_date(runs.iloc[0]["created_at"])
        if not runs.empty
        else "None yet"
    )

    st.markdown(
        """
        <style>
        .dashboard-wrap {
            max-width: 1120px;
            margin: 0 auto;
        }
        .dashboard-grid {
            display: grid;
            grid-template-columns: repeat(4, minmax(0, 1fr));
            gap: 0.85rem;
            margin-bottom: 1rem;
        }
        .dashboard-card {
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: 0.95rem;
            background: rgba(15, 23, 42, 0.42);
            padding: 0.95rem 1rem;
        }
        .dashboard-label {
            font-size: 0.76rem;
            font-weight: 700;
            text-transform: uppercase;
            color: #93c5fd;
            margin-bottom: 0.35rem;
        }
        .dashboard-value {
            font-size: 1.35rem;
            font-weight: 800;
            color: #eff6ff;
            line-height: 1.15;
        }
        .dashboard-subvalue {
            margin-top: 0.3rem;
            color: #cbd5e1;
            font-size: 0.9rem;
            line-height: 1.45;
        }
        .dashboard-action-shell {
            border: 1px solid var(--brand-border);
            border-radius: 1rem;
            background: linear-gradient(180deg, rgba(96, 165, 250, 0.08), rgba(255,255,255,0.02));
            padding: 1rem 1rem 0.2rem 1rem;
            margin-bottom: 1rem;
        }
        .dashboard-action-title {
            font-size: 1rem;
            font-weight: 750;
            color: #eff6ff;
            margin-bottom: 0.25rem;
        }
        .dashboard-action-copy {
            color: #cbd5e1;
            margin-bottom: 0.85rem;
            line-height: 1.5;
        }
        @media (max-width: 980px) {
            .dashboard-grid {
                grid-template-columns: 1fr 1fr;
            }
        }
        @media (max-width: 640px) {
            .dashboard-grid {
                grid-template-columns: 1fr;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown('<div class="dashboard-wrap">', unsafe_allow_html=True)
    st.markdown(
        (
            '<div class="dashboard-grid">'
            f'<div class="dashboard-card"><div class="dashboard-label">Credits Remaining</div><div class="dashboard-value">{credits()}</div><div class="dashboard-subvalue">Available for list generation, refreshes, and expansions.</div></div>'
            f'<div class="dashboard-card"><div class="dashboard-label">Saved Services</div><div class="dashboard-value">{len(svc)}</div><div class="dashboard-subvalue">Service profiles currently saved in your account.</div></div>'
            f'<div class="dashboard-card"><div class="dashboard-label">Saved Lists</div><div class="dashboard-value">{len(runs)}</div><div class="dashboard-subvalue">Buyer-company lists already generated and stored.</div></div>'
            f'<div class="dashboard-card"><div class="dashboard-label">Current Plan</div><div class="dashboard-value">{escape(safe_text(user.get("plan_name"), "None"))}</div><div class="dashboard-subvalue">{escape(safe_text(user.get("subscription_status"), "inactive").title())}</div></div>'
            '</div>'
        ),
        unsafe_allow_html=True,
    )

    st.markdown(
        (
            '<div class="dashboard-grid">'
            f'<div class="dashboard-card"><div class="dashboard-label">Top Buyer Company</div><div class="dashboard-value">{escape(top_buyer_company)}</div><div class="dashboard-subvalue">Highest current signal based on saved evidence.</div></div>'
            f'<div class="dashboard-card"><div class="dashboard-label">Multiple Posting Signals</div><div class="dashboard-value">{multiple_posting_signals}</div><div class="dashboard-subvalue">Buyer companies with more than one relevant posting.</div></div>'
            f'<div class="dashboard-card"><div class="dashboard-label">Freshest Signal</div><div class="dashboard-value">{escape(freshest_signal)}</div><div class="dashboard-subvalue">Most recent posting date captured in saved evidence.</div></div>'
            f'<div class="dashboard-card"><div class="dashboard-label">Most Recent List</div><div class="dashboard-value">{escape(most_recent_list)}</div><div class="dashboard-subvalue">Latest saved list generation date.</div></div>'
            '</div>'
        ),
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="dashboard-action-shell">
            <div class="dashboard-action-title">Open the next part of the workflow</div>
            <div class="dashboard-action-copy">Generate a new list, review the strongest current buyer-company signals, or look for adjacent service gaps.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    a1, a2, a3 = st.columns(3)
    if a1.button("Generate New List", type="primary", use_container_width=True):
        st.session_state["nav_page"] = "Generate List"
        st.rerun()
    if a2.button("Review Next Steps", type="primary", use_container_width=True):
        st.session_state["nav_page"] = "Next Steps"
        st.rerun()
    if a3.button("Review Potential Expansions", type="primary", use_container_width=True):
        st.session_state["nav_page"] = "Potential Expansions"
        st.rerun()

    if is_admin_user(user):
        with st.expander("Admin credit controls"):
            amount = st.number_input("Add demo credits", min_value=1, max_value=500, value=10, step=1)
            if st.button("Add credits"):
                st.success(f"Credits updated to {add_credits(int(amount))}.")
                st.rerun()

    st.markdown("**Recent Activity**")
    if runs.empty:
        st.info("No saved lists yet.")
    else:
        activity_df = runs[
            [
                "created_at",
                "services_text",
                "location_filter",
                "time_window",
                "credits_used",
            ]
        ].copy().head(5)
        activity_df["created_at"] = activity_df["created_at"].apply(format_short_date)
        activity_df.columns = [
            "Date",
            "Services",
            "Location",
            "Time Window",
            "Credits Used",
        ]
        st.dataframe(activity_df, use_container_width=True, hide_index=True)
    st.markdown("</div>", unsafe_allow_html=True)


def page_services():
    st.title("Service Profiles")
    with st.form("service_form"):
        category = st.text_input("Service Category", placeholder="Solar, Energy Storage, EV Charging")
        name = st.text_input("Service")
        description = st.text_area("Service description", height=180, placeholder="Describe the service, scope, titles, and keywords.")
        location_filter = st.text_input("Default target location", value="Any U.S. location")
        submit = st.form_submit_button("Save service profile")
    if submit:
        if not category.strip() or not name.strip() or not description.strip():
            st.error("Please enter a service category, a service, and a service description.")
        else:
            save_service(category, name, description, location_filter)
            st.success("Service profile saved.")
            st.rerun()
    ensure_service_orders()
    svc = services_df()
    if svc.empty:
        st.info("No service profiles saved yet.")
    else:
        st.markdown(
            """
            <style>
            .service-map-wrap {
                max-width: 1120px;
                margin: 0 auto;
            }
            .service-map-intro {
                color: #cbd5e1;
                margin: 0.2rem 0 1rem 0;
                line-height: 1.55;
            }
            .service-category-header {
                margin: 0.4rem 0 0.9rem 0;
                padding: 0.9rem 1rem;
                border: 1px solid rgba(255,255,255,0.08);
                border-radius: 0.95rem;
                background: rgba(15, 23, 42, 0.34);
            }
            .service-category-title {
                font-size: 1.08rem;
                font-weight: 800;
                color: #eff6ff;
                margin-bottom: 0.2rem;
            }
            .service-category-subtitle {
                color: #cbd5e1;
                line-height: 1.45;
            }
            .service-quick-grid {
                display: grid;
                grid-template-columns: repeat(3, minmax(0, 1fr));
                gap: 0.8rem;
                margin-bottom: 1.05rem;
            }
            .service-quick-tile {
                border: 1px solid rgba(255,255,255,0.08);
                border-radius: 0.9rem;
                background: rgba(15, 23, 42, 0.34);
                padding: 0.8rem 0.85rem;
            }
            .service-quick-tile.active {
                border-color: var(--brand-blue);
                background: rgba(96, 165, 250, 0.10);
                box-shadow: 0 0 0 1px rgba(96, 165, 250, 0.20) inset;
            }
            .service-quick-label {
                color: #eff6ff;
                font-weight: 800;
                line-height: 1.45;
                margin-bottom: 0.65rem;
                word-break: break-word;
            }
            .service-chip-grid {
                display: grid;
                grid-template-columns: repeat(3, minmax(0, 1fr));
                gap: 0.9rem;
                margin-bottom: 1rem;
            }
            .service-chip-tile {
                border: 1px solid var(--brand-border);
                border-radius: 0.95rem;
                padding: 0.9rem 0.95rem;
                background: linear-gradient(180deg, rgba(96, 165, 250, 0.08), rgba(255,255,255,0.02));
                box-shadow: 0 12px 28px rgba(15, 23, 42, 0.10);
                min-height: 168px;
                display: flex;
                flex-direction: column;
                justify-content: space-between;
            }
            .service-chip-tile.active {
                border-color: var(--brand-blue);
                background: linear-gradient(180deg, rgba(96, 165, 250, 0.14), rgba(255,255,255,0.03));
                box-shadow: 0 0 0 1px rgba(96, 165, 250, 0.18) inset, 0 12px 28px rgba(15, 23, 42, 0.12);
            }
            .service-chip-label {
                font-size: 1rem;
                font-weight: 800;
                color: #eff6ff;
                line-height: 1.45;
                margin-bottom: 0.7rem;
                word-break: break-word;
            }
            .service-chip-meta {
                color: #cbd5e1;
                font-size: 0.86rem;
                line-height: 1.5;
            }
            .service-chip-description {
                color: #dbeafe;
                font-size: 0.9rem;
                line-height: 1.55;
                margin: 0.7rem 0 0.75rem 0;
                min-height: 58px;
            }
            @media (max-width: 960px) {
                .service-quick-grid,
                .service-chip-grid {
                    grid-template-columns: 1fr 1fr;
                }
            }
            @media (max-width: 640px) {
                .service-quick-grid,
                .service-chip-grid {
                    grid-template-columns: 1fr;
                }
            }
            </style>
            """,
            unsafe_allow_html=True,
        )
        st.markdown('<div class="service-map-wrap">', unsafe_allow_html=True)
        st.markdown("**Saved Service Map**")
        st.markdown(
            '<div class="service-map-intro">Services are shown in the order they were created so you can track your service library and its later variations over time.</div>',
            unsafe_allow_html=True,
        )

        svc = prepare_service_map_df(svc)

        rename_id = st.session_state.get("service_rename_id")
        delete_id = st.session_state.get("service_delete_id")
        focus_id = st.session_state.get("service_focus_id")
        for category_name, category_df in svc.groupby("service_category", sort=False):
            st.markdown(
                (
                    '<div class="service-category-header">'
                    f'<div class="service-category-title">{escape(safe_text(category_name, "General"))}</div>'
                    f'<div class="service-category-subtitle">{len(category_df)} service{"s" if len(category_df) != 1 else ""} in this category.</div>'
                    '</div>'
                ),
                unsafe_allow_html=True,
            )
            st.markdown('<div class="service-quick-grid">', unsafe_allow_html=True)
            quick_columns = st.columns(3)
            for quick_idx, (_, row) in enumerate(category_df.iterrows()):
                service_id = int(row["id"])
                service_number = int(row["service_number"])
                with quick_columns[quick_idx % 3]:
                    active_class = " active" if focus_id == service_id else ""
                    st.markdown(
                        (
                            f'<div class="service-quick-tile{active_class}">'
                            f'<div class="service-quick-label">#{service_number} | {escape(safe_text(row["service_category"], "General"))} | {escape(safe_text(row["service_name"], "Untitled Service"))}</div>'
                            '</div>'
                        ),
                        unsafe_allow_html=True,
                    )
                    mini1, mini2, mini3 = st.columns(3)
                    if mini1.button("Edit", key=f"quick_edit_{service_id}", use_container_width=True):
                        st.session_state["service_rename_id"] = service_id
                        st.session_state["service_focus_id"] = service_id
                        st.session_state.pop("service_delete_id", None)
                        st.rerun()
                    if mini2.button("↑", key=f"quick_up_{service_id}", use_container_width=True):
                        move_service_within_category(service_id, "up")
                        st.session_state["service_focus_id"] = service_id
                        st.rerun()
                    if mini3.button("↓", key=f"quick_down_{service_id}", use_container_width=True):
                        move_service_within_category(service_id, "down")
                        st.session_state["service_focus_id"] = service_id
                        st.rerun()
            st.markdown("</div>", unsafe_allow_html=True)

            st.markdown('<div class="service-chip-grid">', unsafe_allow_html=True)
            tile_columns = st.columns(3)

            for idx, (_, row) in enumerate(category_df.iterrows()):
                service_id = int(row["id"])
                service_number = int(row["service_number"])
                description_preview = safe_text(row["service_description"])
                if len(description_preview) > 120:
                    description_preview = description_preview[:117].rstrip() + "..."
                created_text = format_short_date(row["created_at"]) or safe_text(row["created_at"], "")

                with tile_columns[idx % 3]:
                    active_class = " active" if focus_id == service_id else ""
                    st.markdown(
                        (
                            f'<div class="service-chip-tile{active_class}">'
                            f'<div class="service-chip-label">#{service_number} | {escape(safe_text(row["service_category"], "General"))} | {escape(safe_text(row["service_name"], "Untitled Service"))}</div>'
                            f'<div class="service-chip-description">{escape(description_preview or "No description available.")}</div>'
                            '<div class="service-chip-meta">'
                            f'Target location: {escape(safe_text(row["target_location"], "Any U.S. location"))}<br>'
                            f'Created: {escape(created_text or "Unknown")}'
                            '</div>'
                            '</div>'
                        ),
                        unsafe_allow_html=True,
                    )

                    action_col1, action_col2 = st.columns(2)
                    if action_col1.button("Edit Service", key=f"edit_service_{service_id}", use_container_width=True):
                        st.session_state["service_rename_id"] = service_id
                        st.session_state["service_focus_id"] = service_id
                        st.session_state.pop("service_delete_id", None)
                        st.rerun()
                    if action_col2.button("Delete", key=f"delete_service_{service_id}", use_container_width=True):
                        st.session_state["service_delete_id"] = service_id
                        st.session_state["service_focus_id"] = service_id
                        st.session_state.pop("service_rename_id", None)
                        st.rerun()

                    if rename_id == service_id:
                        with st.form(f"rename_service_form_{service_id}"):
                            new_category = st.text_input(
                                "Service Category",
                                value=safe_text(row["service_category"], "General"),
                                key=f"rename_category_{service_id}",
                            )
                            new_title = st.text_input(
                                "Service",
                                value=safe_text(row["service_name"]),
                                key=f"rename_title_{service_id}",
                            )
                            new_description = st.text_area(
                                "Service Description",
                                value=safe_text(row["service_description"]),
                                height=160,
                                key=f"rename_description_{service_id}",
                            )
                            new_location = st.text_input(
                                "Target Location",
                                value=safe_text(row["target_location"], "Any U.S. location"),
                                key=f"rename_location_{service_id}",
                            )
                            form_col1, form_col2 = st.columns(2)
                            save_rename = form_col1.form_submit_button("Save")
                            cancel_rename = form_col2.form_submit_button("Cancel")
                        if save_rename:
                            if not new_category.strip() or not new_title.strip() or not new_description.strip():
                                st.error("Please enter a service category, service, and service description.")
                            else:
                                update_service_profile(
                                    service_id,
                                    new_category,
                                    new_title,
                                    new_description,
                                    new_location,
                                )
                                st.session_state.pop("service_rename_id", None)
                                st.success("Service profile updated.")
                                st.rerun()
                        if cancel_rename:
                            st.session_state.pop("service_rename_id", None)
                            st.rerun()

                    if delete_id == service_id:
                        st.warning("Delete this service profile?")
                        with st.form(f"delete_service_form_{service_id}"):
                            confirm_col1, confirm_col2 = st.columns(2)
                            confirm_delete = confirm_col1.form_submit_button("Confirm Delete", use_container_width=True)
                            cancel_delete = confirm_col2.form_submit_button("Cancel", use_container_width=True)
                        if confirm_delete:
                            delete_service(service_id)
                            st.session_state.pop("service_delete_id", None)
                            st.success("Service profile deleted.")
                            st.rerun()
                        if cancel_delete:
                            st.session_state.pop("service_delete_id", None)
                            st.rerun()
            st.markdown("</div>", unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)


def page_generate():
    st.title("Generate List")
    svc = services_df()
    if svc.empty:
        st.info("Create a service profile first.")
        return

    options = build_service_option_map(svc)
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

    generate_list_label = f"Generate and Save List (3-{result_limit} credits)"
    if st.button(generate_list_label, type="primary"):
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
                services_text="; ".join(
                    [
                        f"{safe_text(options[label].get('service_category'), 'General')} | {options[label]['service_name']}"
                        for label in selected
                    ]
                ),
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
    current = current_user()
    deep_dive_cache = st.session_state.setdefault("company_deep_dive_cache", {})
    refresh_key = f"next_steps_last_refresh_user_{current['id']}" if current else "next_steps_last_refresh"
    master_evidence_df = build_master_evidence_data()
    if master_evidence_df.empty:
        st.info("Generate and save at least one list before using Next Steps.")
        return

    company_priority_df = build_next_steps_company_table(master_evidence_df)
    if company_priority_df.empty:
        st.info("No company priority analysis is available from the current saved evidence.")
        return
    master_evidence_df = ensure_evidence_columns(master_evidence_df.copy())
    master_evidence_df["canonical_company_name"] = master_evidence_df["company_name"].apply(canonicalize_company_name)

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

    refresh_left, refresh_right = st.columns([1, 2])
    with refresh_left:
        if st.button(f"Refresh analysis ({NEXT_STEPS_REFRESH_COST} credits)", key="next_steps_refresh_button"):
            current_balance = credits()
            if current_balance < NEXT_STEPS_REFRESH_COST:
                st.error(f"You need at least {NEXT_STEPS_REFRESH_COST} credits to refresh the Next Steps analysis.")
            else:
                remaining = add_credits(-NEXT_STEPS_REFRESH_COST)
                deep_dive_cache.clear()
                st.session_state[refresh_key] = datetime.now().strftime("%m/%d/%y %I:%M %p")
                st.success(
                    f"Next Steps analysis refreshed. {NEXT_STEPS_REFRESH_COST} credits used. Credits remaining: {remaining}."
                )
                st.rerun()
    with refresh_right:
        last_refresh = st.session_state.get(refresh_key)
        if last_refresh:
            st.caption(
                f"Last refreshed: {last_refresh} | Use refresh after new searches to re-rank the top 1 to 5 companies."
            )
        else:
            st.caption(
                f"Use refresh after new searches to re-rank the top 1 to 5 companies. Each refresh costs {NEXT_STEPS_REFRESH_COST} credits."
            )

    st.markdown('<div class="nextsteps-wrap">', unsafe_allow_html=True)
    top_company_count = min(5, len(company_priority_df))
    top_companies_df = company_priority_df.head(top_company_count).copy()

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

    st.subheader("Priority Company Reports")
    for idx, (_, company_row) in enumerate(top_companies_df.iterrows(), start=1):
        company_name = company_row["buyer_company"]
        canonical_company_name = safe_text(company_row.get("_canonical_company_name"))
        company_evidence_df = ensure_evidence_columns(
            master_evidence_df[master_evidence_df["canonical_company_name"] == canonical_company_name].copy()
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

        expander_label = (
            f"#{idx} {company_name} | "
            f"{company_row['relevant_posting_count']} relevant posting"
            f"{'s' if int(company_row['relevant_posting_count']) != 1 else ''} | "
            f"{safe_text(company_row['most_recent_posted_date'], 'Unknown date')}"
        )
        with st.expander(expander_label, expanded=False):
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

            st.markdown('<div class="nextsteps-section-label">Expanded Search</div>', unsafe_allow_html=True)
            matched_services_text = safe_text(company_row.get("matched_services"))
            company_cache_key = f"{safe_text(company_name)}::{matched_services_text}"
            deep_dive_entry = deep_dive_cache.get(company_cache_key)

            if st.button(
                f"Expand company hiring view for {company_name} ({COMPANY_DEEP_DIVE_COST} credits)",
                key=f"expand_company_view_{company_cache_key}",
                type="primary",
            ):
                current_balance = credits()
                if current_balance < COMPANY_DEEP_DIVE_COST:
                    st.error(f"You need at least {COMPANY_DEEP_DIVE_COST} credits to run an expanded search.")
                else:
                    try:
                        with st.spinner(f"Searching for additional public postings from {company_name}..."):
                            api_client = client()
                            raw_json, deep_dive_df = search_company_deep_dive(
                                api_client,
                                company_name,
                                matched_services_text,
                                company_evidence_df,
                            )
                            remaining = add_credits(-COMPANY_DEEP_DIVE_COST)
                            if not deep_dive_df.empty:
                                save_deep_dive_run(
                                    company_name=company_name,
                                    matched_services_text=matched_services_text,
                                    credits_used=COMPANY_DEEP_DIVE_COST,
                                    evidence_df=deep_dive_df,
                                )
                            deep_dive_entry = {
                                "raw_json": raw_json,
                                "records": deep_dive_df.to_dict(orient="records"),
                                "error": None,
                            }
                            deep_dive_cache[company_cache_key] = deep_dive_entry
                            st.success(
                                f"Expanded search complete. {COMPANY_DEEP_DIVE_COST} credits used. Credits remaining: {remaining}."
                            )
                    except Exception as exc:
                        deep_dive_entry = {
                            "raw_json": "",
                            "records": [],
                            "error": str(exc),
                        }
                        deep_dive_cache[company_cache_key] = deep_dive_entry

            deep_dive_entry = deep_dive_cache.get(company_cache_key)
            if not deep_dive_entry:
                st.caption("Run an expanded search to pull additional public postings from this company. These results are shown below and are not automatically added to the saved master list.")
            elif deep_dive_entry.get("error"):
                st.warning(f"Expanded search could not be completed: {deep_dive_entry['error']}")
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

                    st.markdown("**Expanded Search: Directly Relevant Postings**")
                    if direct_deep_dive_df.empty:
                        st.write("No additional directly relevant postings were found.")
                    else:
                        for _, job_row in direct_deep_dive_df.iterrows():
                            render_company_deep_dive_job_block(job_row)

                    st.markdown("**Expanded Search: Adjacent Postings**")
                    if adjacent_deep_dive_df.empty:
                        st.write("No additional adjacent postings were found.")
                    else:
                        for _, job_row in adjacent_deep_dive_df.iterrows():
                            render_company_deep_dive_job_block(job_row)

                    st.markdown("**Expanded Search: Broader Company Hiring Context**")
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
        "Select 3 or more saved services to identify market-requested service gaps that are not explicitly covered in the current service set."
    )
    svc = services_df()
    if svc.empty:
        st.info("Create service profiles first.")
        return

    svc = prepare_service_map_df(svc)
    options = build_service_option_map(svc)
    selected = st.multiselect("Select 3 or more services", list(options.keys()))
    if selected:
        st.markdown(
            """
            <style>
            .exp-service-map-wrap {
                max-width: 1080px;
                margin: 0 auto 1rem auto;
            }
            .exp-service-category-box {
                border: 1px solid rgba(255,255,255,0.08);
                border-radius: 0.95rem;
                background: rgba(15, 23, 42, 0.34);
                padding: 0.9rem 1rem 1rem 1rem;
                margin-bottom: 0.9rem;
            }
            .exp-service-category-title {
                font-size: 1rem;
                font-weight: 800;
                color: #eff6ff;
                margin-bottom: 0.2rem;
            }
            .exp-service-category-subtitle {
                color: #cbd5e1;
                margin-bottom: 0.8rem;
                line-height: 1.45;
            }
            .exp-service-chip-grid {
                display: grid;
                grid-template-columns: repeat(3, minmax(0, 1fr));
                gap: 0.7rem;
            }
            .exp-service-chip {
                border: 1px solid var(--brand-border);
                border-radius: 0.85rem;
                background: rgba(96, 165, 250, 0.08);
                color: #dbeafe;
                padding: 0.72rem 0.85rem;
                font-weight: 700;
                line-height: 1.4;
                min-height: 60px;
                display: flex;
                align-items: center;
            }
            @media (max-width: 900px) {
                .exp-service-chip-grid {
                    grid-template-columns: 1fr 1fr;
                }
            }
            @media (max-width: 640px) {
                .exp-service-chip-grid {
                    grid-template-columns: 1fr;
                }
            }
            </style>
            """,
            unsafe_allow_html=True,
        )
        selected_rows_preview = pd.DataFrame([options[label] for label in selected]).copy()
        selected_rows_preview = prepare_service_map_df(selected_rows_preview)
        st.markdown('<div class="exp-service-map-wrap">', unsafe_allow_html=True)
        st.markdown("**Selected Service Map**")
        for category_name, category_df in selected_rows_preview.groupby("service_category", sort=False):
            chips_html = "".join(
                [
                    f'<div class="exp-service-chip">#{int(row["service_number"])} | {escape(safe_text(row["service_category"], "General"))} | {escape(safe_text(row["service_name"], "Untitled Service"))}</div>'
                    for _, row in category_df.iterrows()
                ]
            )
            st.markdown(
                (
                    '<div class="exp-service-category-box">'
                    f'<div class="exp-service-category-title">{escape(safe_text(category_name, "General"))}</div>'
                    f'<div class="exp-service-category-subtitle">{len(category_df)} selected service{"s" if len(category_df) != 1 else ""} in this category.</div>'
                    f'<div class="exp-service-chip-grid">{chips_html}</div>'
                    '</div>'
                ),
                unsafe_allow_html=True,
            )
        st.markdown("</div>", unsafe_allow_html=True)
    st.caption("Default mode uses the saved evidence already collected in your account. This is faster and more tailored to your service set.")
    run_broader_validation = st.checkbox(
        "Broaden with fresh market validation",
        value=False,
        key="exp_broader_validation",
    )
    location_filter = "Any U.S. location"
    time_window = "1 month"
    high_volume = False
    if run_broader_validation:
        location_filter = st.text_input("Location filter", value="Any U.S. location", key="exp_location")
        time_window = st.selectbox("Time window", TIME_OPTIONS, index=2, key="exp_time")
        high_volume = st.checkbox(
            "High volume mode (broader search, more opportunity signals)",
            value=True,
            key="exp_high_volume",
        )
    credits_needed = (len(selected) * (2 if high_volume else 1) if run_broader_validation else 0) + (1 if selected else 0)
    st.caption(
        (
            f"Credits needed: {credits_needed} | Includes 1 expansion analysis credit"
            + (" plus fresh market validation" if run_broader_validation else " using your saved evidence baseline")
            + f" | Credits remaining: {credits()}"
        )
    )

    generate_expansions_label = f"Generate Expansion Ideas ({credits_needed} credits)"
    rendered_report = False
    if st.button(generate_expansions_label, type="primary"):
        if len(selected) < 3:
            st.error("Please select at least 3 saved services.")
            return
        if credits() < credits_needed:
            st.error("Not enough credits. Add more on the Dashboard.")
            return
        try:
            api_client = client()
            selected_rows = pd.DataFrame([options[label] for label in selected])
            selected_service_names = selected_rows["service_name"].tolist()
            baseline_evidence_df = build_expansion_baseline_evidence(selected_service_names)
            all_records = baseline_evidence_df.to_dict(orient="records") if not baseline_evidence_df.empty else []

            with st.spinner("Analyzing service gaps from saved market evidence..."):
                if run_broader_validation:
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
                    if run_broader_validation:
                        st.info(f"No matching U.S. results from the last {time_window} were found.")
                    else:
                        st.info("No saved evidence is available yet for the selected services. Run Generate List first or use broader market validation.")
                    return

                evidence_df = ensure_evidence_columns(evidence_df).drop_duplicates(
                    subset=["source_url", "company_name", "job_title", "matched_service"],
                    keep="first",
                ).sort_values("match_score", ascending=False).reset_index(drop=True)
                raw_json, expansion_df = analyze_expansions(
                    api_client,
                    selected_rows,
                    evidence_df,
                )

            if expansion_df.empty:
                st.info("No clear expansion ideas were found from the current evidence.")
                return

            services_text = "; ".join(selected)
            save_expansion_run(
                services_text=services_text,
                service_count=len(selected_rows),
                used_saved_baseline=not run_broader_validation,
                broader_validation=run_broader_validation,
                high_volume_mode=high_volume,
                location_filter=location_filter if run_broader_validation else "Saved evidence baseline",
                time_window=time_window if run_broader_validation else "Saved evidence baseline",
                credits_used=credits_needed,
                evidence_df=evidence_df,
                expansion_df=expansion_df,
            )
            remaining = add_credits(-credits_needed)
            st.success(f"Expansion analysis complete. Credits remaining: {remaining}")
            render_potential_expansions_report(
                expansion_df=expansion_df,
                evidence_df=evidence_df,
                services_text=services_text,
                location_filter=location_filter if run_broader_validation else "Saved evidence baseline",
                time_window=time_window if run_broader_validation else "Saved evidence baseline",
                mode_text="High volume" if run_broader_validation and high_volume else ("Focused" if run_broader_validation else "Saved evidence baseline"),
                created_at=datetime.now().strftime("%Y-%m-%d %H:%M"),
                service_count=len(selected_rows),
                key_suffix="current",
            )
            rendered_report = True

        except ValueError as exc:
            st.error(str(exc))
        except Exception as exc:
            st.error(f"Something went wrong while generating potential expansions: {exc}")

    if not rendered_report:
        latest_run = latest_expansion_run_record()
        if latest_run:
            saved_expansion_df = load_df(latest_run.get("expansion_json"))
            saved_evidence_df = ensure_evidence_columns(load_df(latest_run.get("evidence_json")))
            if not saved_expansion_df.empty and not saved_evidence_df.empty:
                render_potential_expansions_report(
                    expansion_df=saved_expansion_df,
                    evidence_df=saved_evidence_df,
                    services_text=safe_text(latest_run.get("services_text")),
                    location_filter=safe_text(latest_run.get("location_filter"), "Saved evidence baseline"),
                    time_window=safe_text(latest_run.get("time_window"), "Saved evidence baseline"),
                    mode_text="High volume"
                    if int(latest_run.get("broader_validation") or 0) and int(latest_run.get("high_volume_mode") or 0)
                    else ("Focused" if int(latest_run.get("broader_validation") or 0) else "Saved evidence baseline"),
                    created_at=safe_text(latest_run.get("created_at"), "Unknown"),
                    service_count=int(latest_run.get("service_count") or 0),
                    key_suffix=f"saved_{int(latest_run.get('id') or 0)}",
                )


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
            "Potential Expansions",
            "Next Steps",
        ]
        if is_admin_user(user):
            nav_options.append("Users")
        if st.session_state.get("nav_page") not in nav_options:
            st.session_state["nav_page"] = "Dashboard"
        page = st.radio(
            "Navigate",
            nav_options,
            label_visibility="collapsed",
            key="nav_page",
        )
        if st.button("Sign Out", type="secondary"):
            set_current_user(None)
            st.session_state.pop("nav_page", None)
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
