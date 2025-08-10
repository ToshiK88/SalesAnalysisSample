# ブロック4: 製品の階層構造（グループ→サブカテゴリ→製品）。カラム名は処理するデータにより適宜変更すること。
import pandas as pd

CSV_PATH = "//"ここにファイルパスを記載"//sample_sales_data.csv"
df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")
need = ["製品グループ名","製品サブカテゴリ名","製品名称"]
missing = [c for c in need if c not in df.columns]
if missing:
    print("[不足列]", missing)
else:
    top = df.groupby("製品グループ名", as_index=False).agg(
        rows=("伝票番号","size") if "伝票番号" in df.columns else ("製品名称","size"),
        subcats=("製品サブカテゴリ名","nunique"),
        items=("製品名称","nunique"))
    top = top.sort_values("rows", ascending=False).head(20)
    for _, g in top.iterrows():
        gname = g["製品グループ名"]
        print(f"{gname} - サブカテゴリ数={int(g['subcats'])} / 製品数={int(g['items'])} / 明細数={int(g['rows'])}")
        sub_df = df[df["製品グループ名"] == gname]
        subs = sub_df.groupby("製品サブカテゴリ名", as_index=False).agg(
            rows=("伝票番号","size") if "伝票番号" in df.columns else ("製品名称","size"),
            items=("製品名称","nunique"))
        subs = subs.sort_values("rows", ascending=False).head(15)
        for j, s in subs.iterrows():
            twig = "└─" if j == subs.index[:len(subs)].max() else "├─"
            print(f"  {twig} {s['製品サブカテゴリ名']} - 製品数={int(s['items'])} / 明細数={int(s['rows'])}")
            leaf = sub_df[sub_df["製品サブカテゴリ名"]==s["製品サブカテゴリ名"]]
            leaves = leaf.groupby("製品名称", as_index=False).agg(
                rows=("伝票番号","size") if "伝票番号" in df.columns else ("製品名称","size"))
            leaves = leaves.sort_values("rows", ascending=False).head(15)
            for k, p in leaves.iterrows():
                twig2 = "└─" if k == leaves.index[:len(leaves)].max() else "├─"
                print(f"    {twig2} {p['製品名称']} - 明細数={int(p['rows'])}")
