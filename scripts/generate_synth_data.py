#!/usr/bin/env python3
"""Generate synthetic banking data for Verifiable Banking Analytics demo."""

import os
import time
import numpy as np
import pandas as pd

SEED = 42
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

BANKS = [
    "JPMorgan Chase", "Bank of America", "Wells Fargo", "Citibank",
    "U.S. Bancorp", "Truist Financial", "PNC Financial", "Goldman Sachs",
    "Morgan Stanley", "TD Bank", "Capital One", "Charles Schwab",
    "HSBC North America", "American Express", "Ally Financial",
    "Citizens Financial", "Fifth Third Bank", "KeyBank",
    "Huntington Bancshares", "Regions Financial", "M&T Bank",
    "Discover Financial", "Synchrony Financial", "BMO Harris",
    "Northern Trust", "Comerica", "Zions Bancorporation",
    "Webster Bank", "East West Bancorp", "Valley National Bank",
]

STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC",
]

PRODUCTS = [
    "Credit card", "Mortgage", "Student loan", "Vehicle loan",
    "Checking/savings", "Debt collection", "Credit reporting",
    "Money transfer", "Personal loan", "Payday loan",
]

SUB_PRODUCTS = {
    "Credit card": ["General-purpose credit card", "Store credit card"],
    "Mortgage": ["Conventional home mortgage", "FHA mortgage", "VA mortgage", "Home equity loan"],
    "Student loan": ["Federal student loan", "Private student loan"],
    "Vehicle loan": ["Loan", "Lease"],
    "Checking/savings": ["Checking account", "Savings account", "CD", "Other banking product"],
    "Debt collection": ["Credit card debt", "Medical debt", "Auto debt", "Mortgage debt"],
    "Credit reporting": ["Credit reporting", "Credit monitoring"],
    "Money transfer": ["Domestic money transfer", "International money transfer", "Mobile wallet"],
    "Personal loan": ["Installment loan", "Personal line of credit"],
    "Payday loan": ["Payday loan", "Title loan"],
}

ISSUES_BY_PRODUCT = {
    "Credit card": [
        "Billing disputes", "Fees or interest", "Closing/cancelling account",
        "Fraud or scam", "Problem with a purchase", "Advertising and marketing",
    ],
    "Mortgage": [
        "Applying for a mortgage", "Closing on a mortgage", "Trouble during payment process",
        "Struggling to pay mortgage", "Problem with escrow",
    ],
    "Student loan": [
        "Dealing with your lender", "Struggling to repay", "Getting a loan",
        "Problem with fees", "Incorrect information on your report",
    ],
    "Vehicle loan": [
        "Managing the loan", "Getting a loan", "Problems at end of loan",
        "Problem with fraud alerts",
    ],
    "Checking/savings": [
        "Managing an account", "Opening an account", "Closing an account",
        "Problem with a lender", "Overdraft fees",
    ],
    "Debt collection": [
        "Attempts to collect debt not owed", "Written notification about debt",
        "False statements", "Threatened negative credit reporting",
    ],
    "Credit reporting": [
        "Incorrect information on your report", "Problem with a credit reporting agency",
        "Improper use of your report", "Unable to get your credit report",
    ],
    "Money transfer": [
        "Fraud or scam", "Wrong amount charged", "Money was not available when promised",
        "Other transaction problem",
    ],
    "Personal loan": [
        "Getting a loan", "Managing the loan", "Charged fees or interest unexpectedly",
        "Problem with the payoff process",
    ],
    "Payday loan": [
        "Charged unexpected fees", "Received a loan never applied for",
        "Can't stop withdrawals from bank account", "Problem with the payoff process",
    ],
}

SUB_ISSUES = [
    "Information is incorrect", "Account status incorrect", "Billing statement issues",
    "Unexpected fees charged", "Difficulty contacting company", "Not given enough notice",
    "Company closed account without explanation", "Still being contacted",
    "Debt was paid", "Problem using the card", "Funds not received",
]

CHANNELS = ["Web", "Phone", "Referral", "Fax", "Mail", "Email"]
CHANNEL_WEIGHTS = [0.50, 0.25, 0.10, 0.02, 0.08, 0.05]

RESPONSES = [
    "Closed with explanation", "Closed with monetary relief",
    "Closed with non-monetary relief", "Closed without relief",
    "In progress", "Untimely response",
]
RESPONSE_WEIGHTS = [0.45, 0.10, 0.15, 0.20, 0.07, 0.03]

NARRATIVES = [
    "SYNTHETIC NARRATIVE: Customer reported issue with account.",
    "SYNTHETIC NARRATIVE: Dispute regarding charges on statement.",
    "SYNTHETIC NARRATIVE: Requested resolution for billing error.",
    "SYNTHETIC NARRATIVE: Complaint about service delay.",
    "SYNTHETIC NARRATIVE: Concern over unauthorized transaction.",
    "SYNTHETIC NARRATIVE: Issue with loan terms and conditions.",
    "SYNTHETIC NARRATIVE: Problem accessing account online.",
    "SYNTHETIC NARRATIVE: Difficulty reaching customer support.",
    "SYNTHETIC NARRATIVE: Unexpected fee applied to account.",
    "SYNTHETIC NARRATIVE: Request for information correction.",
]

# State population-weighted sampling (approximate)
STATE_WEIGHTS = np.array([
    1.5, 0.2, 2.3, 1.0, 12.0, 1.8, 1.1, 0.3, 6.8, 3.4,
    0.4, 0.6, 4.0, 2.1, 1.0, 0.9, 1.4, 1.5, 0.4, 1.9,
    2.2, 3.2, 1.8, 0.9, 1.9, 0.3, 0.6, 1.0, 0.4, 2.9,
    0.7, 6.2, 3.3, 0.2, 3.7, 1.3, 1.3, 4.1, 0.3, 1.6,
    0.3, 2.2, 9.2, 1.0, 0.2, 2.7, 2.4, 0.6, 1.8, 0.2, 0.2,
])
STATE_WEIGHTS = STATE_WEIGHTS / STATE_WEIGHTS.sum()


def generate_complaints(n: int, rng: np.random.Generator) -> pd.DataFrame:
    """Generate synthetic CFPB-style complaints using vectorized operations."""
    print(f"  Generating {n:,} complaints...")
    t0 = time.time()

    # Date range: 2020-01-01 to 2025-12-31
    start = np.datetime64("2020-01-01")
    end = np.datetime64("2025-12-31")
    days_range = (end - start).astype(int) + 1
    dates = start + rng.integers(0, days_range, size=n).astype("timedelta64[D]")

    products = rng.choice(PRODUCTS, size=n)

    # Vectorized sub-product and issue selection
    sub_products = np.empty(n, dtype=object)
    issues = np.empty(n, dtype=object)
    for prod in PRODUCTS:
        mask = products == prod
        count = mask.sum()
        if count == 0:
            continue
        sub_products[mask] = rng.choice(SUB_PRODUCTS[prod], size=count)
        issues[mask] = rng.choice(ISSUES_BY_PRODUCT[prod], size=count)

    sub_issues = rng.choice(SUB_ISSUES, size=n)
    companies = rng.choice(BANKS, size=n)
    states = rng.choice(STATES, size=n, p=STATE_WEIGHTS)
    zip_codes = rng.integers(10000, 99999, size=n, endpoint=True).astype(str)
    channels = rng.choice(CHANNELS, size=n, p=CHANNEL_WEIGHTS)
    responses = rng.choice(RESPONSES, size=n, p=RESPONSE_WEIGHTS)
    timely = rng.choice(["Yes", "No"], size=n, p=[0.92, 0.08])
    disputed = rng.choice(["Yes", "No", "NA"], size=n, p=[0.15, 0.55, 0.30])
    narratives = rng.choice(NARRATIVES, size=n)

    print(f"  Arrays built in {time.time() - t0:.1f}s, assembling DataFrame...")

    df = pd.DataFrame({
        "complaint_id": np.arange(1, n + 1),
        "date_received": dates,
        "product": products,
        "sub_product": sub_products,
        "issue": issues,
        "sub_issue": sub_issues,
        "company": companies,
        "state": states,
        "zip_code": zip_codes,
        "channel": channels,
        "company_response": responses,
        "timely_response": timely,
        "consumer_disputed": disputed,
        "consumer_narrative": narratives,
    })
    print(f"  Complaints generated in {time.time() - t0:.1f}s")
    return df


def generate_call_reports(rng: np.random.Generator) -> pd.DataFrame:
    """Generate synthetic FDIC/FFIEC-style quarterly bank financials."""
    print("  Generating call reports...")
    t0 = time.time()

    quarters = [f"Q{q} {y}" for y in range(2020, 2026) for q in range(1, 5)]
    n_quarters = len(quarters)
    n_banks = len(BANKS)
    n_rows = n_quarters * n_banks

    # Deterministic bank IDs
    bank_ids = {bank: 100000 + i for i, bank in enumerate(BANKS)}

    # Tile quarters and banks
    quarter_col = np.repeat(quarters, n_banks)
    bank_col = np.tile(BANKS, n_quarters)
    bank_id_col = np.array([bank_ids[b] for b in bank_col])

    # Base assets per bank (log-uniform between $1B and $3T)
    base_log_assets = rng.uniform(np.log10(1e9), np.log10(3e12), size=n_banks)
    base_assets = 10 ** base_log_assets

    # Repeat base for all quarters with quarterly variation (+/- 5%)
    assets_variation = 1 + rng.normal(0, 0.025, size=(n_quarters, n_banks))
    total_assets = (base_assets[np.newaxis, :] * assets_variation).flatten()

    # Deposits: 70-85% of assets
    deposit_ratio = rng.uniform(0.70, 0.85, size=n_rows)
    total_deposits = total_assets * deposit_ratio

    # Net income: ~0.5-2% of assets annually, so ~0.125-0.5% quarterly
    # Add outliers: ~5% chance of negative income or big swings
    net_income_ratio = rng.normal(0.003, 0.001, size=n_rows)
    outlier_mask = rng.random(size=n_rows) < 0.05
    net_income_ratio[outlier_mask] = rng.uniform(-0.01, 0.015, size=outlier_mask.sum())
    net_income = total_assets * net_income_ratio

    # Non-performing assets: 0.5-5% of assets
    npa_ratio = rng.uniform(0.005, 0.05, size=n_rows)
    non_performing_assets = total_assets * npa_ratio

    # Tier 1 capital ratio: 8-18%
    tier1 = rng.uniform(8.0, 18.0, size=n_rows)

    df = pd.DataFrame({
        "quarter": quarter_col,
        "bank_name": bank_col,
        "bank_id": bank_id_col,
        "total_assets": np.round(total_assets, 2),
        "total_deposits": np.round(total_deposits, 2),
        "net_income": np.round(net_income, 2),
        "non_performing_assets": np.round(non_performing_assets, 2),
        "tier1_capital_ratio": np.round(tier1, 2),
    })
    print(f"  Call reports generated in {time.time() - t0:.1f}s ({n_rows:,} rows)")
    return df


def main():
    small_mode = os.environ.get("SMALL_MODE", "0") == "1"
    n_complaints = 50_000 if small_mode else 1_000_000
    mode_label = "SMALL" if small_mode else "FULL"

    print(f"=== Synthetic Banking Data Generator ({mode_label} mode) ===")
    print(f"  Seed: {SEED}")
    print(f"  Complaints: {n_complaints:,}")
    print(f"  Output dir: {DATA_DIR}")

    os.makedirs(DATA_DIR, exist_ok=True)
    rng = np.random.default_rng(SEED)

    t_start = time.time()

    # Generate complaints
    complaints = generate_complaints(n_complaints, rng)
    complaints_path = os.path.join(DATA_DIR, "complaints.csv")
    print(f"  Writing {complaints_path} ...")
    complaints.to_csv(complaints_path, index=False)
    print(f"  Saved complaints ({os.path.getsize(complaints_path) / 1e6:.1f} MB)")

    # Generate call reports
    call_reports = generate_call_reports(rng)
    call_reports_path = os.path.join(DATA_DIR, "call_reports.csv")
    print(f"  Writing {call_reports_path} ...")
    call_reports.to_csv(call_reports_path, index=False)
    print(f"  Saved call_reports ({os.path.getsize(call_reports_path) / 1e6:.1f} MB)")

    print(f"=== Done in {time.time() - t_start:.1f}s ===")


if __name__ == "__main__":
    main()
