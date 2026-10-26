import os
import pandas as pd
import matplotlib
matplotlib.use("Agg")                      # draw to files, no screen needed in WSL
import matplotlib.pyplot as plt
import seaborn as sns

os.makedirs("charts", exist_ok=True)

# ---- 1. Risk vs return ----
r = pd.read_csv("results_10y.csv")
r["vol_pct"] = r["ann_vol"] * 100
fig, ax = plt.subplots(figsize=(10, 7))
ax.scatter(r["vol_pct"], r["cagr_pct"], s=40)
for _, row in r.iterrows():
    ax.annotate(row["Stock"], (row["vol_pct"], row["cagr_pct"]),
                fontsize=7, xytext=(3, 3), textcoords="offset points")
ax.axhline(0, color="grey", lw=0.8)
ax.set_xlabel("Annualized volatility (%)")
ax.set_ylabel("10-year CAGR (%)")
ax.set_title("Risk vs return, 49 NIFTY-50 stocks (May 2011 to Apr 2021)")
fig.tight_layout()
fig.savefig("charts/1_risk_return.png", dpi=150)
plt.close(fig)

# ---- 2. Correlation heatmap, stocks grouped by sector ----
cm = pd.read_csv("corr_matrix.csv", index_col=0)
meta = pd.read_csv("data/stock_metadata.csv")
meta["Stock"] = meta["Symbol"].str.replace("&", "", regex=False).str.replace("-", "_", regex=False)
sector_of = dict(zip(meta["Stock"], meta["Industry"]))
order = sorted(cm.columns, key=lambda s: (sector_of.get(s, "ZZZ"), s))
fig, ax = plt.subplots(figsize=(13, 11))
sns.heatmap(cm.loc[order, order], cmap="RdYlGn", vmin=0, vmax=1, ax=ax,
            xticklabels=True, yticklabels=True, cbar_kws={"label": "Correlation of daily returns"})
ax.tick_params(labelsize=7)
ax.set_title("Correlation of daily returns (stocks ordered by sector)")
fig.tight_layout()
fig.savefig("charts/2_correlation.png", dpi=150)
plt.close(fig)

# ---- 3. Sector yearly returns ----
hy = pd.read_csv("sector_yearly.csv").set_index("Industry").sort_index()
fig, ax = plt.subplots(figsize=(11, 6))
sns.heatmap(hy, annot=True, fmt=".0f", cmap="RdYlGn", center=0, ax=ax,
            cbar_kws={"label": "Average yearly return (%)"})
ax.set_ylabel("")
ax.set_title("Average yearly return by sector (%)")
fig.tight_layout()
fig.savefig("charts/3_sector_yearly.png", dpi=150)
plt.close(fig)

# ---- 4. Effect of the split/bonus adjustment (Infosys) ----
p = pd.read_parquet("data/clean_stocks.parquet",
                    filters=[("Stock", "==", "INFY")],
                    columns=["Date", "Close", "AdjClose"]).sort_values("Date")
fig, axes = plt.subplots(2, 1, figsize=(10, 7), sharex=True)
axes[0].plot(p["Date"], p["Close"])
axes[0].set_title("Infosys raw close price (drops at every split and bonus issue)")
axes[1].plot(p["Date"], p["AdjClose"], color="tab:green")
axes[1].set_title("Infosys adjusted close price (continuous)")
axes[1].set_xlabel("Date")
fig.tight_layout()
fig.savefig("charts/4_infy_adjustment.png", dpi=150)
plt.close(fig)

print("Saved:", sorted(os.listdir("charts")))

