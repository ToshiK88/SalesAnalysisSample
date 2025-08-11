# ブロック12: 価格×数量ビン（数量帯別の単価）
import pandas as pd
import numpy as np

CSV_PATH = "//こちらにファイルパスを記載//sample_sales_data.csv"
df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")


paid = df[df["合計出荷金額"] > 0].copy()
paid["bin_qty"] = pd.cut(
    paid["個数"],
    bins=[0, 1, 3, 5, 10, 20, 50, 100, 1e9],
    right=True,
    labels=["1", "2-3", "4-5", "6-10", "11-20", "21-50", "51-100", "100+"]
)
agg = paid.groupby("bin_qty", dropna=False).agg(
    Lines=("合計出荷金額", "size"),
    Qty=("個数", "sum"),
    PaidSales=("合計出荷金額", "sum")
)
agg["AvgPrice"] = agg["PaidSales"] / agg["Qty"].replace(0, np.nan)

# ①指数表記を避けて実数表示、②小数点第二位まで
agg["PaidSales"] = agg["PaidSales"].apply(lambda x: "{:.2f}".format(x))
agg["AvgPrice"] = agg["AvgPrice"].apply(lambda x: "{:.2f}".format(x) if pd.notnull(x) else "")

print("[price_quantity_bins]")
print(agg)
