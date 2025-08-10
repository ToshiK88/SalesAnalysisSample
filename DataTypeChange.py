# ブロック1: 読み込みと前処理（型変換・カテゴリ化）
import pandas as pd

CSV_PATH = "//"ファイルパスをここに記載"//sample_sales_data.csv"

def load_sales_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, encoding="utf-8-sig")
    if "出荷日" in df.columns:
        df["出荷日"] = pd.to_datetime(df["出荷日"], errors="coerce")
        df["year_month"] = df["出荷日"].dt.to_period("M").astype(str)
    categorical_cols = [
        "請求先顧客法人グループID","請求先顧客法人グループ法人名",
        "出荷先顧客店舗ID","出荷先顧客店舗名",
        "所在都道府県",
        "顧客担当者ID","顧客担当者名",
        "自社担当者ID","出荷時自社担当者名","出荷時自社担当者テリトリコード",
        "製品グループ名","製品サブカテゴリ名","製品名称",
        "プロモーションID","プロモーション名"
    ]
    for c in categorical_cols:
        if c in df.columns:
            df[c] = df[c].astype("category")
    return df

df = load_sales_csv(CSV_PATH)
print("[情報] 行数×列数:", df.shape)
print("[情報] dtypes:")
print(df.dtypes.apply(lambda t: "category" if "CategoricalDtype" in str(t) else str(t)).to_string())
print("[プレビュー]")
print(df.head(3))
