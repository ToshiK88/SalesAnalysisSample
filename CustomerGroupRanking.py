# ブロック3: 顧客の階層構造（法人グループ→店舗）
import pandas as pd
import numpy as np

CSV_PATH = "//"ここにファイルパスを記載"//sample_sales_data.csv"
df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")
need = ["請求先顧客法人グループID","請求先顧客法人グループ法人名","出荷先顧客店舗ID","出荷先顧客店舗名","所在都道府県"]
missing = [c for c in need if c not in df.columns]
if missing:
    print("[不足列]", missing)
else:
    grp = df.groupby(["請求先顧客法人グループID","請求先顧客法人グループ法人名"], as_index=False) \
            .agg(rows=("伝票番号","size") if "伝票番号" in df.columns else ("出荷先顧客店舗ID","size"),
                 stores=("出荷先顧客店舗ID","nunique"))
    grp = grp.sort_values("rows", ascending=False).head(30)
    for _, r in grp.iterrows():
        gid, gname, rows, stores = r["請求先顧客法人グループID"], r["請求先顧客法人グループ法人名"], int(r["rows"]), int(r["stores"])
        print(f"{gname} ({gid}) - 店舗数={stores} / 明細数={rows}")
        sub = df[(df["請求先顧客法人グループID"]==gid) & (df["請求先顧客法人グループ法人名"]==gname)]
        st = sub.groupby(["出荷先顧客店舗ID","出荷先顧客店舗名"], as_index=False) \
                .agg(rows=("伝票番号","size") if "伝票番号" in df.columns else ("出荷先顧客店舗ID","size"),
                     pref=("所在都道府県", lambda x: x.mode().iat[0] if not x.mode().empty else ""))
        st = st.sort_values("rows", ascending=False).head(15)
        for i, s in st.iterrows():
            twig = "└─" if i == st.index[:len(st)].max() else "├─"
            print(f"  {twig} {s['出荷先顧客店舗名']} ({s['出荷先顧客店舗ID']}) [{s['pref']}] - 明細数={int(s['rows'])}")
