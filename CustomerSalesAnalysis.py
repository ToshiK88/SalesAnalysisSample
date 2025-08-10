# ブロック8: 顧客サマリ（法人グループ/店舗）　上位１０件の顧客グループと店舗を分析
import pandas as pd

CSV_PATH = "//ここにファイルパスを記載//sample_sales_data.csv"
df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")

# 法人グループ
if {"請求先顧客法人グループID","請求先顧客法人グループ法人名","合計出荷金額"}.issubset(df.columns):
    cg = df.groupby(["請求先顧客法人グループID","請求先顧客法人グループ法人名"], as_index=False) \
           .agg(NetSales=("合計出荷金額","sum"),
                Stores=("出荷先顧客店舗ID","nunique"),
                Lines=("伝票番号","size") if "伝票番号" in df.columns else ("出荷先顧客店舗ID","size"))
    cg = cg.sort_values("NetSales", ascending=False)
    print("[customer_group_summary]")
    print(cg.head(10))

# 店舗
if {"出荷先顧客店舗ID","出荷先顧客店舗名","合計出荷金額"}.issubset(df.columns):
    st = df.groupby(["出荷先顧客店舗ID","出荷先顧客店舗名"], as_index=False) \
           .agg(NetSales=("合計出荷金額","sum"),
                Lines=("伝票番号","size") if "伝票番号" in df.columns else ("合計出荷金額","size"))
    st = st.sort_values("NetSales", ascending=False)
    print("[store_summary]")
    print(st.head(10))
