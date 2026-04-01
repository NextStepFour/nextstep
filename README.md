# NextStep

Simple Streamlit portal for generating and saving solar service prospect lists with user accounts and Stripe billing support.

## What it does

1. Save reusable service profiles
2. Generate prospect lists using credits
3. Search by service, location, time window, and search mode
4. Show an estimated search time before each run
5. Optionally skip company contact research for faster list generation
6. Group results into company-level opportunities
7. Surface likely buyer contacts, contact info, contact priority, and outreach next steps
8. Suggest potential service expansions based on repeated adjacent market signals
9. Let users create accounts and sign in
10. Support Stripe subscription checkout and billing status refresh
11. Retain saved lists inside the portal
12. Export company opportunities as CSV or PDF
13. Export supporting evidence as CSV

## Portal sections

- Dashboard
- Plans & Billing
- Service Profiles
- Generate List
- Saved Lists
- Potential Expansions

## Credits

- Focused mode uses 1 credit per selected service
- High volume mode uses 2 credits per selected service
- Potential Expansions uses the search credits above plus 1 extra synthesis credit
- New users start with a small demo credit balance
- Active paid plans refresh monthly credits based on the Stripe plan

## Install dependencies

```bash
pip install -r requirements.txt
```

## Set the OpenAI API key

### PowerShell

```powershell
$env:OPENAI_API_KEY="your_api_key_here"
$env:APP_BASE_URL="http://localhost:8501"
$env:STRIPE_SECRET_KEY="your_stripe_secret_key"
$env:STRIPE_PRICE_ID_STARTER="price_..."
$env:STRIPE_PRICE_ID_PRO="price_..."
```

### macOS/Linux

```bash
export OPENAI_API_KEY="your_api_key_here"
export APP_BASE_URL="http://localhost:8501"
export STRIPE_SECRET_KEY="your_stripe_secret_key"
export STRIPE_PRICE_ID_STARTER="price_..."
export STRIPE_PRICE_ID_PRO="price_..."
```

## Run the app locally with Streamlit

```bash
python -m streamlit run app.py
```

Then open the local Streamlit URL shown in your terminal.

## Deploy publicly

The simplest production path is:

1. Push the project to GitHub
2. Deploy it on Render or Railway
3. Set the environment variables above in the hosting dashboard
4. Use a persistent disk if you keep SQLite
5. Attach your custom domain in the hosting dashboard
6. Update `APP_BASE_URL` to your public domain

Example public URL:

```text
https://app.yourdomain.com
```

## Stripe setup

1. Create products and recurring prices in Stripe for Starter and Pro
2. Copy the Stripe secret key and price IDs into environment variables
3. Set `APP_BASE_URL` to your deployed site URL
4. Sign in to the app and use the Plans & Billing page to create a checkout session

## Notes

- The current app keeps data in `nextstep_portal.db`
- For a low-volume MVP, this can work on a host with a persistent disk
- For a larger production rollout, move from SQLite to a hosted Postgres database

## What To Do Next

1. Install Git on your computer
2. Create a GitHub repo and push this project
3. Create a Render account and deploy from the repo
4. Set the environment variables from `.env.example` in Render
5. Buy a domain and connect it to Render
6. Create Stripe products and recurring prices for Starter and Pro
7. Paste the Stripe values into Render and update `APP_BASE_URL` to the final public domain
