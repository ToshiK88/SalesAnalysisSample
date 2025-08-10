# ブロック10: 地域（都道府県）サマリ
import pandas as pd

CSV_PATH = "//こちらにファイルパスを記載//sample_sales_data.csv"
df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")

if {"所在都道府県","合計出荷金額"}.issubset(df.columns):
    pref = df.groupby("所在都道府県", as_index=False).agg(
        NetSales=("合計出荷金額","sum"),
        Stores=("出荷先顧客店舗ID","nunique"))
    print("[prefecture_summary 上位10]")
    print(pref.sort_values("NetSales", ascending=False).head(50))
