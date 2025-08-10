# ブロック9: 製品サマリ（グループ/サブカテゴリ/製品）
import pandas as pd


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
    except Exception:
        return str(number)

CSV_PATH = "//"こちらにファイルパスを記載"//sample_sales_data.csv"
df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")

# グループ
if {"製品グループ名","合計出荷金額"}.issubset(df.columns):
    g = df.groupby("製品グループ名", as_index=False).agg(
        NetSales=("合計出荷金額","sum"),
        Items=("製品名称","nunique"),
        Lines=("伝票番号","size") if "伝票番号" in df.columns else ("合計出荷金額","size"))
    print("[product_summary: グループ 上位5]")
    g_top = g.sort_values("NetSales", ascending=False).head(5).copy()
    if "NetSales" in g_top.columns:
        g_top["NetSales"] = g_top["NetSales"].apply(format_decimal_full)
    print(g_top.to_string(index=False))

# サブカテゴリ
if {"製品サブカテゴリ名","合計出荷金額"}.issubset(df.columns):
    sc = df.groupby("製品サブカテゴリ名", as_index=False).agg(
        NetSales=("合計出荷金額","sum"),
        Items=("製品名称","nunique"))
    print("[product_summary: サブカテゴリ 上位5]")
    sc_top = sc.sort_values("NetSales", ascending=False).head(5).copy()
    if "NetSales" in sc_top.columns:
        sc_top["NetSales"] = sc_top["NetSales"].apply(format_decimal_full)
    print(sc_top.to_string(index=False))

# 製品
if {"製品名称","合計出荷金額"}.issubset(df.columns):
    it = df.groupby("製品名称", as_index=False).agg(
        NetSales=("合計出荷金額","sum"))
    print("[product_summary: 製品 上位5]")
    it_top = it.sort_values("NetSales", ascending=False).head(5).copy()
    if "NetSales" in it_top.columns:
        it_top["NetSales"] = it_top["NetSales"].apply(format_decimal_full)
    print(it_top.to_string(index=False))
