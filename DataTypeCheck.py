# ブロック2: データ定義表（dtype/NULL/一意数/min-max/例値）
import pandas as pd
import numpy as np

CSV_PATH = "/Users/tk/SALES _ANALYSIS _EXPR4/sample_sales_data.csv"
df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")
if "出荷日" in df.columns:
    df["出荷日"] = pd.to_datetime(df["出荷日"], errors="coerce")

def fmt_dtype(s: pd.Series) -> str:
    t = s.dtype
    if pd.api.types.is_categorical_dtype(t): return "category"
    if pd.api.types.is_datetime64_any_dtype(t): return "datetime64[ns]"
    if pd.api.types.is_integer_dtype(t): return "int"
    if pd.api.types.is_float_dtype(t): return "float"
    if pd.api.types.is_bool_dtype(t): return "bool"
    return str(t)

print("列名\tdtype\t非NULL\tNULL\t一意数\tmin/max\t例(最大5)")
for col in df.columns:
    s = df[col]
    dtype = fmt_dtype(s)
    nonnull, nulls = int(s.notna().sum()), int(s.isna().sum())
    nunique = int(s.nunique(dropna=True))
    minmax = ""
    if pd.api.types.is_numeric_dtype(s) or pd.api.types.is_datetime64_any_dtype(s):
        ss = s.dropna()
        if len(ss) > 0:
            minmax = f"{ss.min()} / {ss.max()}"
    examples = ", ".join([str(v) for v in s.dropna().unique()[:5]])
    print("\t".join([col, dtype, str(nonnull), str(nulls), str(nunique), minmax, examples]))
