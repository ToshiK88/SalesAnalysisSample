# ブロック5: 品質チェック（無償/返品/金額一致）
import pandas as pd

CSV_PATH = "//"ここにファイルパスを記載"//sample_sales_data.csv"
df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")
tol = 0.5  # 円未満の誤差を吸収

issues = []

if {"無償出荷フラグ","合計出荷金額"}.issubset(df.columns):
    m = (df["無償出荷フラグ"]==1) & (df["合計出荷金額"].round(2)!=0)
    if m.any():
        x = df.loc[m].copy(); x["issue"]="FREE_NONZERO_AMOUNT"; issues.append(x)

if {"返品フラグ","個数","合計出荷金額"}.issubset(df.columns):
    m = (df["返品フラグ"]==1) & ((df["個数"]>=0) | (df["合計出荷金額"]>=0))
    if m.any():
        x = df.loc[m].copy(); x["issue"]="RETURN_SIGN_MISMATCH"; issues.append(x)

if {"無償出荷フラグ","返品フラグ","合計出荷金額","単価","個数"}.issubset(df.columns):
    mask_paid = (df["無償出荷フラグ"] == 0) & (df["返品フラグ"] == 0) & (pd.to_numeric(df["合計出荷金額"], errors="coerce") > 0)

    expected = pd.to_numeric(df.loc[mask_paid, "単価"], errors="coerce") * pd.to_numeric(df.loc[mask_paid, "個数"], errors="coerce")
    actual   = pd.to_numeric(df.loc[mask_paid, "合計出荷金額"], errors="coerce")

    diff = expected - actual
    mask_mismatch = diff.abs() > tol

    if mask_mismatch.any():
        tmp = df.loc[mask_paid].loc[mask_mismatch].copy()
        tmp["calc_diff"] = diff.loc[mask_mismatch]
        tmp["expected_round0"] = expected.round(0).loc[mask_mismatch]
        tmp["actual_round0"]   = actual.round(0).loc[mask_mismatch]
        tmp["issue"] = "PRICE_QTY_MISMATCH"
        issues.append(tmp)

out = pd.concat(issues, ignore_index=True) if issues else pd.DataFrame(columns=list(df.columns)+["issue"])
print(f"[品質チェック] 問題件数={len(out)}")
print(out[["issue"]].value_counts().rename("count"))
print("[サンプル表示（最大5行）]")
print(out.head(5))
