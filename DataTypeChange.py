# ブロック1: 厳密な型付け + 全カラムの定義表を表示（初心者向けシンプル版）
import pandas as pd

CSV_PATH = "//"ファイルパスをここに記載"//sample_sales_data.csv"

def simple_dtype(s: pd.Series) -> str:
    # 最小限の型名に統一
    if pd.api.types.is_datetime64_any_dtype(s): return "datetime"
    if pd.api.types.is_bool_dtype(s): return "bool"
    if pd.api.types.is_integer_dtype(s): return "int"
    if pd.api.types.is_float_dtype(s): return "float"
    return "string"

def maybe_bool(s: pd.Series) -> bool:
    # 0/1/true/false/yes/no/はい/いいえ/有/無 のみなら bool とみなす（簡易判定）
    allowed_true  = {"1","true","t","y","yes","はい","有"}
    allowed_false = {"0","false","f","n","no","いいえ","無"}
    vals = pd.Series(s.dropna().astype(str).str.strip().str.lower().unique())
    if len(vals) == 0: 
        return False
    return set(vals).issubset(allowed_true | allowed_false)

def length_class(s: pd.Series) -> str:
    # 非NULLの文字列表現の長さで判定（数値/日付も文字列化して共通処理）
    if s.dropna().empty:
        return "N/A"
    lens = s.dropna().astype(str).str.len()
    mn, mx = int(lens.min()), int(lens.max())
    return f"固定長({mn})" if mn == mx else f"可変長({mn}-{mx})"

df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")

print("列名\tデータ型\t長さ区分")
for col in df.columns:
    s = df[col]
    # 日付は先に解釈（存在すれば）
    if col == "出荷日":
        s = pd.to_datetime(s, errors="coerce")
        dtype_name = "datetime"
    else:
        # 簡易bool判定 → それ以外はpandas dtypeベースで丸める
        dtype_name = "bool" if maybe_bool(s) else simple_dtype(s)
    print(f"{col}\t{dtype_name}\t{length_class(s)}")
