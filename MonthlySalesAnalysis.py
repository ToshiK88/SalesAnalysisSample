# ブロック7: 月次サマリ
import pandas as pd
import numpy as np

CSV_PATH = "/Users/tk/SALES _ANALYSIS _EXPR4/sample_sales_data.csv"
df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")
df["出荷日"] = pd.to_datetime(df["出荷日"], errors="coerce")
df["year_month"] = df["出荷日"].dt.to_period("M").astype(str)

paid = df.loc[df["合計出荷金額"]>0].groupby("year_month")["合計出荷金額"].sum()
net = df.groupby("year_month")["合計出荷金額"].sum()
ret = df.loc[df["返品フラグ"]==1].groupby("year_month")["合計出荷金額"].apply(lambda s: s.abs().sum())

out = pd.DataFrame({
    "year_month": net.index,
    "NetSales": net.values,
    "PaidSales": paid.reindex(net.index, fill_value=0).values,
    "Returns": ret.reindex(net.index, fill_value=0).values,
})
out["ReturnRate"] = out["Returns"] / out["PaidSales"].replace(0, np.nan)

# MoM/YoY（NetSalesで例示）
out["NetSales_MoM"] = out["NetSales"].pct_change()
out["NetSales_YoY"] = out["NetSales"].pct_change(12)

print(out.head(12))
