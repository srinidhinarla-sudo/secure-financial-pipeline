"""
Generate a synthetic creditcard.csv that matches the Kaggle dataset schema exactly.

Real dataset stats reproduced:
- 284,807 rows  (284,315 legitimate + 492 fraud)
- Time: 0 .. 172792 (seconds, two days of transactions)
- V1-V28: unit-normal PCA features (slightly different distributions for fraud)
- Amount: log-normal, capped at ~25,000
- Class: 0 = legit, 1 = fraud  (0.172% fraud rate)
"""

import numpy as np
import pandas as pd

RNG = np.random.default_rng(42)

N_TOTAL = 284_807
N_FRAUD = 492
N_LEGIT = N_TOTAL - N_FRAUD

print(f"Generating {N_TOTAL:,} rows ({N_FRAUD} fraud, {N_LEGIT:,} legitimate)...")

# ── Time ──────────────────────────────────────────────────────────────────────
time_legit = np.sort(RNG.uniform(0, 172_792, N_LEGIT))
time_fraud = RNG.uniform(0, 172_792, N_FRAUD)

# ── V1-V28 (PCA features) ──────────────────────────────────────────────────────
# Fraud rows have slightly higher variance on several components — matches
# the real dataset's statistical signature without needing the real data.
v_legit = RNG.standard_normal((N_LEGIT, 28))
v_fraud = RNG.standard_normal((N_FRAUD, 28)) * 1.8 - 0.5  # shifted mean, higher var

# ── Amount ────────────────────────────────────────────────────────────────────
amount_legit = np.clip(RNG.lognormal(mean=3.5, sigma=1.8, size=N_LEGIT), 0, 25_000)
amount_fraud = np.clip(RNG.lognormal(mean=2.0, sigma=1.5, size=N_FRAUD), 0, 5_000)

# ── Assemble DataFrames ───────────────────────────────────────────────────────
cols = [f"V{i}" for i in range(1, 29)]

df_legit = pd.DataFrame(v_legit, columns=cols)
df_legit.insert(0, "Time", time_legit)
df_legit["Amount"] = amount_legit
df_legit["Class"] = 0

df_fraud = pd.DataFrame(v_fraud, columns=cols)
df_fraud.insert(0, "Time", time_fraud)
df_fraud["Amount"] = amount_fraud
df_fraud["Class"] = 1

df = pd.concat([df_legit, df_fraud], ignore_index=True)

# Sort by Time (matches original dataset layout)
df = df.sort_values("Time").reset_index(drop=True)

out = "data/creditcard.csv"
df.to_csv(out, index=False)
print(f"Written {len(df):,} rows to {out}")
print(f"Fraud rate: {df['Class'].mean()*100:.3f}%")
print(f"Amount range: ${df['Amount'].min():.2f} – ${df['Amount'].max():.2f}")
print(f"Time range:   {df['Time'].min():.0f}s – {df['Time'].max():.0f}s")
print("Columns:", list(df.columns))
