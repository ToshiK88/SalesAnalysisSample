# ブロック1: 厳密な型付け + 全カラムの定義表を表示（初心者向けシンプル版）
import pandas as pd

CSV_PATH = "//"ファイルパスをここに記載"//sample_sales_data.csv"

def maybe_bool(s: pd.Series) -> bool:
    true  = {"1","true","t","y","yes","はい","有"}
    false = {"0","false","f","n","no","いいえ","無"}
    vals = pd.Series(s.dropna().astype(str).str.strip().str.lower().unique())
    return len(vals) > 0 and set(vals).issubset(true | false)

def to_boolean_nullable(s: pd.Series) -> pd.Series:
    m = s.astype(str).str.strip().str.lower()
    true  = {"1","true","t","y","yes","はい","有"}
    false = {"0","false","f","n","no","いいえ","無"}
    out = pd.Series(pd.NA, index=s.index, dtype="boolean")
    out = out.mask(m.isin(true), True).mask(m.isin(false), False)
    return out

def length_class(s: pd.Series) -> str:
    if s.dropna().empty: return "N/A"
    l = s.dropna().astype(str).str.len()
    return f"固定長({int(l.min())})" if l.min()==l.max() else f"可変長({int(l.min())}-{int(l.max())})"

df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")

# ここで“実際に”型変換を適用（最小限）
if "出荷日" in df.columns:
    df["出荷日"] = pd.to_datetime(df["出荷日"], errors="coerce")
for col in df.columns:
    if maybe_bool(df[col]):
        df[col] = to_boolean_nullable(df[col])

print("列名\tデータ型\t長さ区分")
for col in df.columns:
    s = df[col]
    if pd.api.types.is_datetime64_any_dtype(s): t = "datetime"
    elif pd.api.types.is_bool_dtype(s): t = "bool"
    elif pd.api.types.is_integer_dtype(s): t = "int"
    elif pd.api.types.is_float_dtype(s): t = "float"
    else: t = "string"
    print(f"{col}\t{t}\t{length_class(s)}")
