# ブロック11: 担当者サマリ（顧客/自社）
import pandas as pd

CSV_PATH = "//"ここにファイルパスを貼り付け"//sample_sales_data.csv"
df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")

def format_decimal_full(number: float) -> str:
    """指数表記を避け、十進数で全桁表示する。末尾の不要なゼロと小数点は削除する。"""
    if pd.isna(number):
        return ""
    try:
        # 整数値なら整数として表示
        if float(number).is_integer():
            return str(int(float(number)))
        # 小数がある場合は固定小数で表現しつつ末尾ゼロを除去
        text = format(float(number), 'f')
        return text.rstrip('0').rstrip('.')
    except Exception as e:
        print(f"[DEBUG] format_decimal_full error: {e}, input={number}")
        return str(number)

# 顧客担当者
if {"顧客担当者ID","顧客担当者名","合計出荷金額"}.issubset(df.columns):
    reps_c = df.groupby(["顧客担当者ID","顧客担当者名"], as_index=False).agg(
        NetSales=("合計出荷金額","sum"),
        Customers=("請求先顧客法人グループID","nunique"))
    print("[reps_customer_summary 上位5]")
    reps_c_top = reps_c.sort_values("NetSales", ascending=False).head(5).copy()
    if "NetSales" in reps_c_top.columns:
        reps_c_top["NetSales"] = reps_c_top["NetSales"].apply(format_decimal_full)
    print(reps_c_top.to_string(index=False))

# 自社担当者
if {"自社担当者ID","出荷時自社担当者名","合計出荷金額"}.issubset(df.columns):
    reps_s = df.groupby(["自社担当者ID","出荷時自社担当者名"], as_index=False).agg(
        NetSales=("合計出荷金額","sum"),
        Accounts=("請求先顧客法人グループID","nunique"))
    print("[reps_company_summary 上位5]")
    reps_s_top = reps_s.sort_values("NetSales", ascending=False).head(5).copy()
    if "NetSales" in reps_s_top.columns:
        reps_s_top["NetSales"] = reps_s_top["NetSales"].apply(format_decimal_full)
    print(reps_s_top.to_string(index=False))
