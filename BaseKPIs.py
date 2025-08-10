# ブロック6: KPIの算出（Net/Paid/Returns/Rate/AvgPrice）
import pandas as pd
import numpy as np

CSV_PATH = "//"ここにファイルパスを記載"//sample_sales_data.csv"
df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")

paid_sales = df.loc[df["合計出荷金額"]>0, "合計出荷金額"].sum()
net_sales = df["合計出荷金額"].sum()
returns = df.loc[df["返品フラグ"]==1, "合計出荷金額"].abs().sum()
qty_paid = df.loc[df["合計出荷金額"]>0, "個数"].sum()

return_rate = (returns / paid_sales) if paid_sales != 0 else float("nan")
avg_price = (paid_sales / qty_paid) if qty_paid != 0 else float("nan")

print(f"NetSales(純額): {net_sales:,.0f}")
print(f"PaidSales(有償売上): {paid_sales:,.0f}")
print(f"Returns(返品額の絶対値): {returns:,.0f}")
print(f"ReturnRate(返品率=Returns/PaidSales): {return_rate:.3%}" if pd.notna(return_rate) else "ReturnRate: N/A")
print(f"AvgPrice(平均単価=PaidSales/有償数量): {avg_price:,.2f}" if pd.notna(avg_price) else "AvgPrice: N/A")
