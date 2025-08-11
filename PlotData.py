# ブロック14: ダッシュボード（HTML）を保存
import pandas as pd, plotly.express as px, plotly.io as pio
from pathlib import Path

CSV_PATH = "//CSVパス//sample_sales_data.csv"
OUT_HTML = "//任意の書き出しパス//quick_dashboard.html"

df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")
df["出荷日"] = pd.to_datetime(df["出荷日"], errors="coerce")
df["year_month"] = df["出荷日"].dt.to_period("M").astype(str)

ts = df.groupby("year_month", as_index=False)["合計出荷金額"].sum()
fig = px.line(ts, x="year_month", y="合計出荷金額", title="月次 NetSales")
pio.write_html(fig, OUT_HTML, auto_open=False)
print(f"[保存] {OUT_HTML}")
