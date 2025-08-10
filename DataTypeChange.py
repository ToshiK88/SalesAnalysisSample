# ブロック1: 厳密な型付け + 全カラムの定義表を表示（初心者向けシンプル版）
import pandas as pd
import numpy as np

CSV_PATH = "//"ファイルパスをここに記載"//sample_sales_data.csv"

# 明示的に“こう扱いたい”列（不足は後述の自動推測に任せる）
ID_COLS = [
    "請求先顧客法人グループID", "出荷先顧客店舗ID", "顧客担当者ID",
    "自社担当者ID", "出荷時自社担当者テリトリコード", "プロモーションID"
]
NAME_COLS = [
    "請求先顧客法人グループ法人名", "出荷先顧客店舗名", "顧客担当者名",
    "出荷時自社担当者名", "製品グループ名", "製品サブカテゴリ名",
    "製品名称", "プロモーション名", "所在都道府県"
]
FLAG_COLS = ["返品フラグ", "無償出荷フラグ"]
DATE_COL = "出荷日"
QTY_COL = "個数"
PRICE_COL = "単価"
AMOUNT_COL = "合計出荷金額"

def load_and_cast(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, encoding="utf-8-sig")

    # 1) まずは明示的に決めている列を変換
    for c in ID_COLS + NAME_COLS:
        if c in df.columns:
            df[c] = df[c].astype("string").str.strip()

    if DATE_COL in df.columns:
        df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="coerce")

    for c in FLAG_COLS:
        if c in df.columns:
            # 0/1/True/False/文字列を幅広く受けて nullable boolean に
            v = pd.to_numeric(df[c], errors="coerce")
            df[c] = v.astype("Int64").astype("boolean")

    if QTY_COL in df.columns:
        df[QTY_COL] = pd.to_numeric(df[QTY_COL], errors="coerce").astype("Int64")  # 返品は負もOK

    if PRICE_COL in df.columns:
        df[PRICE_COL] = pd.to_numeric(df[PRICE_COL], errors="coerce").astype("float64")
    if AMOUNT_COL in df.columns:
        df[AMOUNT_COL] = pd.to_numeric(df[AMOUNT_COL], errors="coerce").astype("float64")

    # 2) year_month は常に YYYY-MM の文字列
    if DATE_COL in df.columns:
        df["year_month"] = df[DATE_COL].dt.strftime("%Y-%m").astype("string")

    # 3) 追加の自動推測（名前にヒントがある場合のみ）
    for c in df.columns:
        if c not in ID_COLS + NAME_COLS + FLAG_COLS + [DATE_COL, QTY_COL, PRICE_COL, AMOUNT_COL, "year_month"]:
            if ("ID" in c) or ("コード" in c):
                df[c] = df[c].astype("string").str.strip()
            elif ("フラグ" in c) or (c.lower().endswith("_flag")):
                v = pd.to_numeric(df[c], errors="coerce")
                df[c] = v.astype("Int64").astype("boolean")
            # それ以外は既定の推測に任せる（初心者向けに過度に触らない）

    return df

def human_dtype(s: pd.Series) -> str:
    # 読者にわかりやすい“論理型”の名前
    if pd.api.types.is_bool_dtype(s): return "bool(真偽: 欠損可)"
    if pd.api.types.is_integer_dtype(s): return "int(整数: 欠損可)" if str(s.dtype).startswith("Int") else "int"
    if pd.api.types.is_float_dtype(s): return "float(小数)"
    if pd.api.types.is_datetime64_any_dtype(s): return "datetime(日付時刻)"
    if pd.api.types.is_string_dtype(s): return "string(文字列)"
    if pd.api.types.is_categorical_dtype(s): return "category"
    return str(s.dtype)

def build_data_dictionary(df: pd.DataFrame, sample_n: int = 5) -> pd.DataFrame:
    rows = []
    for col in df.columns:
        s = df[col]
        logical = human_dtype(s)
        pandas_dtype = str(s.dtype)
        nonnull = int(s.notna().sum())
        nulls = int(s.isna().sum())
        unique = int(s.nunique(dropna=True))
        # min/max（数値・日付のみ）
        mm = ""
        if pd.api.types.is_numeric_dtype(s) or pd.api.types.is_datetime64_any_dtype(s):
            ss = s.dropna()
            if not ss.empty:
                mm = f"{ss.min()} / {ss.max()}"
        # 例（先頭から最大N件）
        try:
            ex = [str(v) for v in s.dropna().unique()[:sample_n]]
            examples = ", ".join(ex)
        except Exception:
            examples = ""
        rows.append({
            "列名": col,
            "論理型": logical,
            "pandas_dtype": pandas_dtype,
            "非NULL": nonnull,
            "NULL": nulls,
            "一意数": unique,
            "min/max": mm,
            f"例(最大{sample_n})": examples
        })
    return pd.DataFrame(rows)

# 実行
df = load_and_cast(CSV_PATH)
dic = build_data_dictionary(df, sample_n=5)

print("=== 型変換後のプレビュー(先頭3行) ===")
preview_cols = [c for c in [
    "請求先顧客法人グループID","請求先顧客法人グループ法人名",
    "出荷先顧客店舗ID","出荷先顧客店舗名",
    "製品グループ名","製品サブカテゴリ名","製品名称",
    "単価","個数","合計出荷金額","返品フラグ","無償出荷フラグ",
    "出荷日","year_month"
] if c in df.columns]
print(df[preview_cols].head(3))

print("\n=== データ定義表（全カラム） ===")
print(dic.to_string(index=False))
