# -*- coding: utf-8 -*-
# 文字エンコーディングをUTF-8として明示しています。

"""
売上データ分析・可視化スクリプト（単体で実行できるCLIツール）

このファイルは、手元にある売上CSVを読み込み、以下を一括で実施します：
1) データ品質チェックとクリーニング（論理一貫性の検証と異常抽出）
2) MECEに基づく基礎集計の生成（期間、顧客、製品、地域、担当者、価格×数量）
3) 処理済み・集計済みCSVの出力
4) PlotlyによるインタラクティブなグラフをHTMLに出力

ユースケース（想定する運用像）
- 実行者の手元にすでに解析対象の売上トランザクションCSVがあり、ターミナル/コマンドプロンプトでPythonが利用可能になっている状態のユーザーが対象です。
- 依存ライブラリは `requirements.txt` で一括インストール可能です。
- 出力は指定ディレクトリに、CSVとHTML（インタラクティブ可視化）として保存されます。

数学的定義（分析で一貫して用いるKPI定義）
- 正味売上 NetSales = Σ 合計出荷金額（返品は負、無償は0を含む純額）
- 有償売上 PaidSales = Σ 合計出荷金額（合計出荷金額 > 0 のみ）
- 返品金額 Returns = Σ |合計出荷金額|（返品フラグ=1 のレコードのみ、絶対値で損失規模を把握）
- 返品率 ReturnRate = Returns / PaidSales（分母は正の売上に限定し、純額の大小に左右されない率）
- 無償件数/率 FreeCount/FreeRate = 無償フラグ=1 の件数 / 総件数（数量ベース指標も併用可）
- 平均単価 AvgPrice = Σ 合計出荷金額（有償） / Σ 個数（有償）

品質ルール（論理整合性）
- 無償出荷フラグ=1 ⇒ 合計出荷金額=0
- 返品フラグ=1 ⇒ 個数<0 かつ 合計出荷金額<0
- 通常有償出荷 ⇒ 合計出荷金額 ≈ 単価×個数（丸め誤差は許容：±0.01）

使い方（例）
  python analyze_sales.py \
      --input sample_sales_data.csv \
      --outdir output \
      --start 2022-01-01 --end 2023-12-31 \
      --open-html

出力物（主なCSV）
- monthly_summary.csv, customer_group_summary.csv, store_summary.csv,
  product_summary.csv, prefecture_summary.csv, reps_customer_summary.csv,
  reps_company_summary.csv, price_quantity_bins.csv, quality_issues.csv

出力物（HTML）
- sales_dashboard.html（時系列、顧客ツリーマップ、ランキング、価格×数量、地域、担当者など）

注意：列名は日本語（UTF-8）を前提にしています。本READMEとコードのコメントは、学習・運用の双方の目的で冗長に記述しています。
"""

# ここでは機能の全体像と設計意図を読めるように長めの説明を書いています。

from __future__ import annotations
# 将来のアノテーション評価（前方参照）を有効にして型表現を柔軟にしています。

import argparse
# コマンドライン引数を扱うための標準ライブラリです。
from dataclasses import dataclass
# 設定を表すデータ構造にデータクラスを使います。
from pathlib import Path
# パス操作を高可読にするPathオブジェクトを使います。
from typing import Dict, List, Optional, Tuple
# 型ヒントのために汎用コレクション型を取り込みます。

import numpy as np
# 数値計算やNaN処理のためにNumPyを使います。
import pandas as pd
# データ操作・集計のためにPandasを使います。
import plotly.express as px
# 高レベルAPIでグラフを簡便に作るためにPlotly Expressを使います。
import plotly.graph_objects as go
# 低レベルAPI（柔軟なレイアウト制御）のためにGraph Objectsを使います。
import plotly.io as pio
# 図をHTML文字列に変換するための入出力ユーティリティを使います。
import warnings
# 将来の仕様変更に関する警告を抑制するためにwarningsを使います。

# 将来のpandas/plotlyのobserved既定値変更に関する警告を非表示にします。
warnings.filterwarnings("ignore", category=FutureWarning, module=r".*pandas.*")
warnings.filterwarnings("ignore", category=FutureWarning, module=r".*plotly\.express\._core.*")


# =========================
# 設定用データクラス
# =========================


@dataclass
class AnalysisConfig:
    input_csv: Path
    # 入力CSVのファイルパスです。

    output_dir: Path
    # 生成物（CSV/HTML）の出力ディレクトリです。

    start_date: Optional[pd.Timestamp] = None
    # 期間フィルタの開始日です（省略可）です。

    end_date: Optional[pd.Timestamp] = None
    # 期間フィルタの終了日です（省略可）です。

    price_multiplication_tolerance: float = 0.01  # 単価×個数 との乖離許容
    # 単価×個数と合計金額の乖離を許容するしきい値です。

    open_html_after_save: bool = False
    # 出力後にHTMLを既定ブラウザで開くかどうかのフラグです。


# =========================
# 入出力と前処理
# =========================


def load_sales_csv(csv_path: Path) -> pd.DataFrame:
    """CSVを読み込み、日付型変換と型の軽量化を行います。

    想定列：
    - 伝票番号, 出荷日, 請求先顧客法人グループID, 請求先顧客法人グループ法人名,
      出荷先顧客店舗ID, 出荷先顧客店舗名, 所在都道府県,
      顧客担当者ID, 顧客担当者名, 自社担当者ID, 出荷時自社担当者名,
      出荷時自社担当者テリトリコード, 製品グループ名, 製品サブカテゴリ名, 製品名称,
      単価, 個数, 合計出荷金額, 返品フラグ, 無償出荷フラグ
    """
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    # UTF-8（BOMつき）でCSVを読み込みます。Excel出力にも対応します。

    # 基本型変換
    df["出荷日"] = pd.to_datetime(df["出荷日"])  # 時系列集計のキー
    # 出荷日を日付型に変換して時系列集計に使います。

    # カテゴリ化（メモリ節約と処理高速化）
    category_cols = [
        "請求先顧客法人グループID",
        "請求先顧客法人グループ法人名",
        "出荷先顧客店舗ID",
        "出荷先顧客店舗名",
        "所在都道府県",
        "顧客担当者ID",
        "顧客担当者名",
        "自社担当者ID",
        "出荷時自社担当者名",
        "出荷時自社担当者テリトリコード",
        "製品グループ名",
        "製品サブカテゴリ名",
        "製品名称",
    ]
    for c in category_cols:
        if c in df.columns:
            df[c] = df[c].astype("category")
            # 存在する列だけ安全にカテゴリ型へ変換します。

    # 補助列（利便性）
    df["year_month"] = df["出荷日"].dt.to_period("M").astype(str)
    # 年月の文字列を作って月次集計のキーにします。
    df["weekday"] = df["出荷日"].dt.weekday  # Monday=0
    # 曜日番号（0=月,6=日）を付与します。
    df["weekday_name"] = df["出荷日"].dt.day_name(locale="ja_JP") if hasattr(df["出荷日"].dt, "day_name") else df["出荷日"].dt.day_name()
    # 互換性のため、日本語ロケールがあれば日本語曜日名を取得します。
    # 日本語の曜日ラベル（月〜日）を安定順序で付与
    weekday_map = {0: "月", 1: "火", 2: "水", 3: "木", 4: "金", 5: "土", 6: "日"}
    ordered_labels = ["月", "火", "水", "木", "金", "土", "日"]
    df["weekday_jp"] = pd.Categorical(df["weekday"].map(weekday_map), categories=ordered_labels, ordered=True)
    # 日本語曜日ラベルを安定順序のカテゴリとして付与します。

    return df
    # 前処理済みのDataFrameを返します。


def filter_by_date(df: pd.DataFrame, start: Optional[pd.Timestamp], end: Optional[pd.Timestamp]) -> pd.DataFrame:
    """開始日・終了日でデータをフィルタします（境界含む）です。"""
    if start is not None:
        df = df[df["出荷日"] >= start]
        # 開始日以降の行だけに絞り込みます。
    if end is not None:
        df = df[df["出荷日"] <= end]
        # 終了日以前の行だけに絞り込みます。
    return df
    # 期間で絞った結果を返します。


# =========================
# 品質チェック（論理整合性）
# =========================


def run_quality_checks(df: pd.DataFrame, tolerance: float) -> pd.DataFrame:
    """主要な論理整合性チェックを行い、問題レコードを返します。

    付与するissue例：
      - FREE_NONZERO_AMOUNT: 無償なのに金額!=0
      - RETURN_SIGN_MISMATCH: 返品なのに数量/金額の符号が負でない
      - PRICE_QTY_MISMATCH: 通常有償だが 合計出荷金額 ≠ 単価×個数（許容超）
    """
    issues: List[pd.DataFrame] = []
    # 問題行を種類ごとに一時保持するリストです。

    # 無償なのに金額が0でない
    mask_free_nonzero = (df["無償出荷フラグ"] == 1) & (df["合計出荷金額"].round(2) != 0)
    # 無償なのに金額が0でない行を検出します。
    if mask_free_nonzero.any():
        tmp = df.loc[mask_free_nonzero].copy()
        tmp["issue"] = "FREE_NONZERO_AMOUNT"
        issues.append(tmp)
        # 該当行にラベルを付けて収集します。

    # 返品の符号不整合（数量または金額が非負）
    mask_return_bad = (df["返品フラグ"] == 1) & ((df["個数"] >= 0) | (df["合計出荷金額"] >= 0))
    # 返品なのに数量または金額が非負の行を検出します。
    if mask_return_bad.any():
        tmp = df.loc[mask_return_bad].copy()
        tmp["issue"] = "RETURN_SIGN_MISMATCH"
        issues.append(tmp)
        # 該当行にラベルを付けて収集します。

    # 通常有償（無償0・返品0・金額>0）の金額一致チェック
    mask_paid = (df["無償出荷フラグ"] == 0) & (df["返品フラグ"] == 0) & (df["合計出荷金額"] > 0)
    # 通常有償（無償/返品でなく、金額が正）の行を取り出します。
    # 許容差分
    diff = (df.loc[mask_paid, "単価"] * df.loc[mask_paid, "個数"]) - df.loc[mask_paid, "合計出荷金額"]
    # 単価×個数と合計金額の差分を計算します。
    mask_mismatch = diff.abs() > tolerance
    # 許容差を超える不一致行を特定します。
    if mask_mismatch.any():
        tmp = df.loc[mask_paid].loc[mask_mismatch].copy()
        tmp["calc_diff"] = diff.loc[mask_mismatch]
        tmp["issue"] = "PRICE_QTY_MISMATCH"
        issues.append(tmp)
        # 差分列とラベルを付けて収集します。

    if issues:
        return pd.concat(issues, axis=0, ignore_index=True)
        # 問題行を連結して返します。
    else:
        # 空のDataFrame（同じ列構成に合わせる）
        out = df.head(0).copy()
        out["issue"] = out.get("issue", pd.Series(dtype="object"))
        return out
        # 問題がなければ空の同型DataFrameを返します。


# =========================
# 集計関数群（MECEの各軸）
# =========================


def compute_core_kpis(df: pd.DataFrame) -> Dict[str, float]:
    paid_sales = df.loc[df["合計出荷金額"] > 0, "合計出荷金額"].sum()
    # 有償売上合計です。
    net_sales = df["合計出荷金額"].sum()
    # 正味売上（返品は負、無償は0を含む純額）です。
    returns = df.loc[df["返品フラグ"] == 1, "合計出荷金額"].abs().sum()
    # 返品金額の絶対値合計です。
    return_rate = (returns / paid_sales) if paid_sales > 0 else np.nan
    # 返品率は分母を有償売上に限定します。
    free_count = int((df["無償出荷フラグ"] == 1).sum())
    # 無償出荷の件数です。
    total_rows = int(len(df))
    # 全レコード件数です。
    free_rate = free_count / total_rows if total_rows > 0 else np.nan
    # 無償率（件数ベース）です。

    qty_paid = df.loc[df["合計出荷金額"] > 0, "個数"].sum()
    # 有償の数量合計です。
    avg_price = (paid_sales / qty_paid) if qty_paid > 0 else np.nan
    # 有償の平均単価です。

    return {
        "NetSales": float(net_sales),
        "PaidSales": float(paid_sales),
        "Returns": float(returns),
        "ReturnRate": float(return_rate) if pd.notna(return_rate) else np.nan,
        "FreeCount": free_count,
        "FreeRate": float(free_rate) if pd.notna(free_rate) else np.nan,
        "Transactions": total_rows,
        "AvgPricePaid": float(avg_price) if pd.notna(avg_price) else np.nan,
    }
    # KPIを辞書で返します。


def monthly_summary(df: pd.DataFrame) -> pd.DataFrame:
    # 基本集計
    base = df.groupby("year_month", observed=False).agg(
        NetSales=("合計出荷金額", "sum"),
        Quantity=("個数", "sum"),
        Transactions=("伝票番号", "count"),
        FreeCount=("無償出荷フラグ", "sum"),
    ).reset_index()
    # 月ごとの基本指標を集計します。
    # 有償売上と返品の別集計
    paid = df[df["合計出荷金額"] > 0].groupby("year_month", observed=False)["合計出荷金額"].sum()
    ret = df[df["返品フラグ"] == 1].groupby("year_month", observed=False)["合計出荷金額"].apply(lambda s: s.abs().sum())
    g = base.merge(paid.rename("PaidSales"), on="year_month", how="left").merge(ret.rename("Returns"), on="year_month", how="left")
    g[["PaidSales", "Returns"]] = g[["PaidSales", "Returns"]].fillna(0.0)
    g["ReturnRate"] = g.apply(lambda r: (r["Returns"] / r["PaidSales"]) if r["PaidSales"] > 0 else np.nan, axis=1)
    g["FreeRate"] = g["FreeCount"] / g["Transactions"].replace({0: np.nan})

    # MoM（前月比）とYoY（前年同月比）
    g = g.sort_values("year_month").reset_index(drop=True)
    g["MoM_NetSales"] = g["NetSales"].pct_change()
    # 前月比（率）を計算します。
    # YoYは期間が2年以上ある場合のみ意味を持つ
    g["YoY_NetSales"] = g["NetSales"].pct_change(periods=12)
    # 前年同月比（率）を計算します。
    return g


def customer_hierarchy_summaries(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    cg_keys = ["請求先顧客法人グループID", "請求先顧客法人グループ法人名"]
    st_keys = cg_keys + ["出荷先顧客店舗ID", "出荷先顧客店舗名"]

    # 基本集計
    cg_base = df.groupby(cg_keys, observed=False).agg(
        NetSales=("合計出荷金額", "sum"),
        Quantity=("個数", "sum"),
        Transactions=("伝票番号", "count"),
        FreeCount=("無償出荷フラグ", "sum"),
    ).reset_index()
    # 法人グループ単位の基本集計です。
    st_base = df.groupby(st_keys, observed=False).agg(
        NetSales=("合計出荷金額", "sum"),
        Quantity=("個数", "sum"),
        Transactions=("伝票番号", "count"),
        FreeCount=("無償出荷フラグ", "sum"),
    ).reset_index()
    # 店舗単位の基本集計です。

    # 返品額と有償売上
    cg_ret = df[df["返品フラグ"] == 1].groupby(cg_keys, observed=False)["合計出荷金額"].apply(lambda s: s.abs().sum()).rename("Returns")
    st_ret = df[df["返品フラグ"] == 1].groupby(st_keys, observed=False)["合計出荷金額"].apply(lambda s: s.abs().sum()).rename("Returns")
    cg_paid = df[df["合計出荷金額"] > 0].groupby(cg_keys, observed=False)["合計出荷金額"].sum().rename("PaidSales")
    # 結合
    cg = cg_base.merge(cg_ret, on=cg_keys, how="left").merge(cg_paid, on=cg_keys, how="left")
    cg[["Returns", "PaidSales"]] = cg[["Returns", "PaidSales"]].fillna(0.0)
    cg["ReturnRate"] = cg.apply(lambda r: (r["Returns"] / r["PaidSales"]) if r["PaidSales"] > 0 else np.nan, axis=1)
    # 法人グループの返品率を計算します。

    st_paid = df[df["合計出荷金額"] > 0].groupby(st_keys, observed=False)["合計出荷金額"].sum().rename("PaidSales")
    store = st_base.merge(st_ret, on=st_keys, how="left").merge(st_paid, on=st_keys, how="left")
    store[["Returns", "PaidSales"]] = store[["Returns", "PaidSales"]].fillna(0.0)
    store["ReturnRate"] = store.apply(lambda r: (r["Returns"] / r["PaidSales"]) if r["PaidSales"] > 0 else np.nan, axis=1)
    # 店舗の返品率を計算します。

    return cg.sort_values("NetSales", ascending=False), store.sort_values("NetSales", ascending=False)
    # 売上降順で並べた結果を返します。


def product_summaries(df: pd.DataFrame) -> pd.DataFrame:
    keys = ["製品グループ名", "製品サブカテゴリ名", "製品名称"]
    base = df.groupby(keys, observed=False).agg(
        NetSales=("合計出荷金額", "sum"),
        Quantity=("個数", "sum"),
        Transactions=("伝票番号", "count"),
        FreeCount=("無償出荷フラグ", "sum"),
    ).reset_index()
    # 基本集計を行います。
    ret = df[df["返品フラグ"] == 1].groupby(keys, observed=False)["合計出荷金額"].apply(lambda s: s.abs().sum()).rename("Returns")
    paid_sales = df[df["合計出荷金額"] > 0].groupby(keys, observed=False)["合計出荷金額"].sum().rename("PaidSales")
    paid_qty = df[df["合計出荷金額"] > 0].groupby(keys, observed=False)["個数"].sum().rename("PaidQty")
    g = base.merge(ret, on=keys, how="left").merge(paid_sales, on=keys, how="left").merge(paid_qty, on=keys, how="left")
    for c in ["Returns", "PaidSales", "PaidQty"]:
        g[c] = g[c].fillna(0.0)
        # 欠損を0で補います。
    g["AvgPricePaid"] = g.apply(lambda r: (r["PaidSales"] / r["PaidQty"]) if r["PaidQty"] > 0 else np.nan, axis=1)
    return g.sort_values("NetSales", ascending=False)
    # 売上降順で返します。


def prefecture_summary(df: pd.DataFrame) -> pd.DataFrame:
    key = ["所在都道府県"]
    base = df.groupby(key, observed=False).agg(
        NetSales=("合計出荷金額", "sum"),
        Quantity=("個数", "sum"),
        Transactions=("伝票番号", "count"),
        FreeCount=("無償出荷フラグ", "sum"),
        Stores=("出荷先顧客店舗ID", "nunique"),
    ).reset_index()
    # 一意店舗数（Stores）を含む基本集計です。
    ret = df[df["返品フラグ"] == 1].groupby(key, observed=False)["合計出荷金額"].apply(lambda s: s.abs().sum()).rename("Returns")
    paid = df[df["合計出荷金額"] > 0].groupby(key, observed=False)["合計出荷金額"].sum().rename("PaidSales")
    g = base.merge(ret, on=key, how="left").merge(paid, on=key, how="left")
    g[["Returns", "PaidSales"]] = g[["Returns", "PaidSales"]].fillna(0.0)
    g["ReturnRate"] = g.apply(lambda r: (r["Returns"] / r["PaidSales"]) if r["PaidSales"] > 0 else np.nan, axis=1)
    g["FreeRate"] = g["FreeCount"] / g["Transactions"].replace({0: np.nan})
    return g.sort_values("NetSales", ascending=False)
    # 売上降順で返します。


def reps_summaries(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    cust_keys = ["顧客担当者ID", "顧客担当者名"]
    comp_keys = ["自社担当者ID", "出荷時自社担当者名", "出荷時自社担当者テリトリコード"]

    cust_base = df.groupby(cust_keys, observed=False).agg(
        NetSales=("合計出荷金額", "sum"),
        Quantity=("個数", "sum"),
        Transactions=("伝票番号", "count"),
        FreeCount=("無償出荷フラグ", "sum"),
    ).reset_index()
    comp_base = df.groupby(comp_keys, observed=False).agg(
        NetSales=("合計出荷金額", "sum"),
        Quantity=("個数", "sum"),
        Transactions=("伝票番号", "count"),
        FreeCount=("無償出荷フラグ", "sum"),
    ).reset_index()

    cust_ret = df[df["返品フラグ"] == 1].groupby(cust_keys, observed=False)["合計出荷金額"].apply(lambda s: s.abs().sum()).rename("Returns")
    comp_ret = df[df["返品フラグ"] == 1].groupby(comp_keys, observed=False)["合計出荷金額"].apply(lambda s: s.abs().sum()).rename("Returns")

    cust = cust_base.merge(cust_ret, on=cust_keys, how="left").fillna({"Returns": 0.0})
    comp = comp_base.merge(comp_ret, on=comp_keys, how="left").fillna({"Returns": 0.0})

    return cust.sort_values("NetSales", ascending=False), comp.sort_values("NetSales", ascending=False)
    # 売上降順で返します。


def price_quantity_bins(df: pd.DataFrame) -> pd.DataFrame:
    # 有償のみ対象
    d = df[(df["合計出荷金額"] > 0)].copy()
    if d.empty:
        return d
        # データがなければ空のまま返します。
    d["qty_bin"] = pd.cut(d["個数"], bins=[0, 10, 20, 50, 100, np.inf], labels=["<=10", "11-20", "21-50", "51-100", ">100"], right=True)
    out = d.groupby("qty_bin", observed=False).agg(
        PaidSales=("合計出荷金額", "sum"),
        PaidQty=("個数", "sum"),
        Transactions=("伝票番号", "count"),
    ).reset_index()
    out["AvgPrice"] = out["PaidSales"] / out["PaidQty"].replace({0: np.nan})
    # 帯ごとの平均単価を計算します。
    return out


# =========================
# 可視化（Plotly）
# =========================


def build_figures(
    df: pd.DataFrame,
    mon: pd.DataFrame,
    cg: pd.DataFrame,
    store: pd.DataFrame,
    prod: pd.DataFrame,
    pref: pd.DataFrame,
    reps_c: pd.DataFrame,
    reps_comp: pd.DataFrame,
) -> List[str]:
    """各図をHTML片として返す（後段で結合して1ファイルに保存）。"""
    html_snippets: List[str] = []
    # 生成した各図のHTML片をこのリストに追加します。

    # 1) 月次売上推移（NetSales）
    mon_sorted = mon.sort_values("year_month")
    # 月次を時系列順に並べ替えます。
    fig1 = go.Figure()
    # 売上を棒、返品を折線に
    fig1.add_trace(go.Bar(x=mon_sorted["year_month"], y=mon_sorted["NetSales"], name="NetSales", marker_color="#1f77b4", opacity=0.85))
    fig1.add_trace(go.Scatter(x=mon_sorted["year_month"], y=mon_sorted["Returns"], mode="lines+markers", name="Returns (abs)", yaxis="y2", line=dict(color="#EF553B")))
    fig1.update_layout(
        title="月次推移: 正味売上（棒）と返品（折線・絶対額）",
        xaxis_title="年月",
        yaxis=dict(title="売上金額"),
        yaxis2=dict(title="返品額", overlaying="y", side="right", showgrid=False),
        legend_title="指標",
        barmode="group",
    )
    # 1つ目の図で Plotly 本体をインライン埋め込み（外部CDN依存を解消してPagesでも空白にならないように）
    html_snippets.append(pio.to_html(fig1, include_plotlyjs="inline", full_html=False))

    # 2) 法人→店舗 ツリーマップ（売上寄与）
    # Treemap は weights（values列）がゼロ合計だとエラーになるため、有償売上>0のみを対象にする
    top_store = store[store.get("PaidSales", 0) > 0].copy().sort_values("PaidSales", ascending=False).head(300)
    # ツリーマップ用に売上上位の店舗を抽出します。
    fig2 = px.treemap(
        top_store,
        path=[px.Constant("全体"), "請求先顧客法人グループ法人名", "出荷先顧客店舗名"],
        values="PaidSales",
        color="NetSales",
        color_continuous_scale="Blues",
        title="顧客階層（法人→店舗）寄与ツリーマップ（累計売上）",
        labels={"PaidSales": "累計売上", "NetSales": "正味売上"},
    )
    html_snippets.append(pio.to_html(fig2, include_plotlyjs=False, full_html=False))
    # 2つ目以降はPlotly本体を外部参照にしてHTMLを軽量化します。

    # 2b) 法人→店舗 積み上げ帯グラフ（TopN店舗＋その他、PaidSales）
    band_src = store[["請求先顧客法人グループ法人名", "出荷先顧客店舗名", "PaidSales"]].copy()
    band_src = band_src[band_src["PaidSales"] > 0]
    # 積み上げ帯グラフ用にデータを整形します。
    if not band_src.empty:
        # グループ別合計
        grp_tot = band_src.groupby("請求先顧客法人グループ法人名", as_index=False)["PaidSales"].sum().rename(columns={"PaidSales": "GroupPaid"})
        band_src = band_src.merge(grp_tot, on="請求先顧客法人グループ法人名", how="left")
        # 各グループ内 TopN 選定
        TOPN = 8
        band_src["rank"] = band_src.groupby("請求先顧客法人グループ法人名")["PaidSales"].rank(ascending=False, method="first")
        band_src["store_for_viz"] = np.where(band_src["rank"] <= TOPN, band_src["出荷先顧客店舗名"], "その他")
        band_viz = (
            band_src.groupby(["請求先顧客法人グループ法人名", "store_for_viz"], as_index=False)["PaidSales"].sum()
        )
        # グループ順を PaidSales 降順に
        grp_order = band_viz.groupby("請求先顧客法人グループ法人名", as_index=False)["PaidSales"].sum().sort_values("PaidSales", ascending=False)["請求先顧客法人グループ法人名"].tolist()
        band_viz["請求先顧客法人グループ法人名"] = pd.Categorical(band_viz["請求先顧客法人グループ法人名"], categories=grp_order, ordered=True)
        fig2b = px.bar(
            band_viz,
            y="請求先顧客法人グループ法人名",
            x="PaidSales",
            color="store_for_viz",
            orientation="h",
            labels={"PaidSales": "累計売上", "請求先顧客法人グループ法人名": "法人グループ", "store_for_viz": "店舗"},
            title="顧客階層（法人→店舗）積み上げ帯（Top8店舗＋その他、累計売上）",
        )
        fig2b.update_layout(barmode="stack", legend_title_text="店舗", xaxis_title="累計売上")
        html_snippets.append(pio.to_html(fig2b, include_plotlyjs=False, full_html=False))
        # HTML片として追加します。

    # 3) トップ店舗・トップ製品（棒グラフ）
    fig3a = px.bar(store.head(20), x="出荷先顧客店舗名", y="NetSales", title="店舗別売上 Top20")
    fig3a.update_layout(xaxis_tickangle=-45)
    fig3b = px.bar(prod.head(20), x="製品名称", y="NetSales", title="製品別売上 Top20")
    fig3b.update_layout(xaxis_tickangle=-45)
    html_snippets.append(pio.to_html(fig3a, include_plotlyjs=False, full_html=False))
    html_snippets.append(pio.to_html(fig3b, include_plotlyjs=False, full_html=False))
    # ランキング図を追加します。

    # 4) 価格×数量（有償のみ）散布図
    paid = df[df["合計出荷金額"] > 0]
    sample_paid = paid.sample(min(5000, len(paid)), random_state=42) if len(paid) > 5000 else paid
    # 価格×数量の散布図は最大5,000点にサンプリングします。
    fig4 = px.scatter(sample_paid, x="個数", y="単価", color="製品グループ名", trendline="ols", title="価格×数量 散布図（有償のみ、最大5Kサンプル）")
    html_snippets.append(pio.to_html(fig4, include_plotlyjs=False, full_html=False))
    # 散布図とトレンドラインを追加します。

    # 5) 都道府県別 売上（棒）
    fig5 = px.bar(pref.sort_values("NetSales", ascending=False), x="所在都道府県", y="NetSales", title="都道府県別 売上")
    fig5.update_layout(xaxis_tickangle=-45)
    html_snippets.append(pio.to_html(fig5, include_plotlyjs=False, full_html=False))
    # 都道府県別の棒グラフを追加します。

    # 6) 担当者（自社）Top20 時系列（合計ではなく月次 NetSales）
    # まず上位自社担当者を抽出
    top_comp_ids = reps_comp.head(10)["自社担当者ID"].tolist()
    work = df[df["自社担当者ID"].isin(top_comp_ids)].copy()
    ts = work.groupby(["year_month", "出荷時自社担当者名"], as_index=False)["合計出荷金額"].sum()
    fig6 = px.line(ts, x="year_month", y="合計出荷金額", color="出荷時自社担当者名", title="上位 自社担当者×月次 売上推移")
    html_snippets.append(pio.to_html(fig6, include_plotlyjs=False, full_html=False))
    # 上位担当者の月次売上推移を折れ線で追加します。

    # 7) 曜日×月 ヒートマップ（NetSales） — y軸を日本語曜日に変更
    day = df.groupby(["year_month", "weekday_jp"], as_index=False)["合計出荷金額"].sum()
    pivot = day.pivot(index="weekday_jp", columns="year_month", values="合計出荷金額").reindex(index=["月", "火", "水", "木", "金", "土", "日"])
    fig7 = px.imshow(pivot, labels=dict(x="year_month", y="曜日", color="NetSales"), title="曜日×月 ヒートマップ（NetSales）")
    html_snippets.append(pio.to_html(fig7, include_plotlyjs=False, full_html=False))
    # 曜日×月のヒートマップを追加します。

    return html_snippets


def build_dashboard_html(html_snippets: List[str], title: str = "Sales Dashboard") -> str:
    """複数のPlotly HTML片を1つのHTMLにまとめる。"""
    container_css = """
    <style>
    body { font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Helvetica, Arial, 'Noto Sans JP', 'Hiragino Kaku Gothic ProN', Meiryo, sans-serif; margin: 20px; }
    h1 { margin-bottom: 0.2rem; }
    .fig { margin: 24px 0; border: 1px solid #e5e7eb; padding: 12px; border-radius: 8px; }
    .note { color: #555; font-size: 0.9rem; }
    </style>
    """
    parts = [
        "<!DOCTYPE html>",
        "<html lang=\"ja\">",
        "<head>",
        f"<meta charset=\"utf-8\"><title>{title}</title>",
        container_css,
        "</head>",
        "<body>",
        f"<h1>{title}</h1>",
        "<p class=\"note\">このページはPlotlyで生成されたインタラクティブなダッシュボードです。各グラフはホバー、ズーム、凡例クリックでインタラクションできます。</p>",
    ]
    parts.extend([f"<div class=\"fig\">{frag}</div>" for frag in html_snippets])
    parts.extend(["</body>", "</html>"])
    return "\n".join(parts)
    # 完成したHTML文字列を返します。


# =========================
# 永続化（CSV/HTML 保存）
# =========================


def save_dataframes(out_dir: Path, frames: Dict[str, pd.DataFrame]) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    # 出力ディレクトリを作成します（既存でもエラーにしません）。
    for name, df in frames.items():
        df.to_csv(out_dir / f"{name}.csv", index=False, encoding="utf-8-sig")
        # 各DataFrameをUTF-8（BOMつき）で保存します。


def save_dashboard_html(out_dir: Path, html: str, open_after: bool = False) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    html_path = out_dir / "sales_dashboard.html"
    html_path.write_text(html, encoding="utf-8")
    # HTMLをファイルに書き出します。
    if open_after:
        try:
            # macOS
            import subprocess

            subprocess.run(["open", str(html_path)], check=False)
        except Exception:
            pass
            # 失敗しても致命的ではないので無視します。
    return html_path


# =========================
# メイン実行
# =========================


def main(cfg: AnalysisConfig) -> None:
    # データ読み込み
    df_all = load_sales_csv(cfg.input_csv)
    df = filter_by_date(df_all, cfg.start_date, cfg.end_date)

    # 品質チェック
    q = run_quality_checks(df, cfg.price_multiplication_tolerance)

    # 集計
    kpis = compute_core_kpis(df)
    mon = monthly_summary(df)
    cg, store = customer_hierarchy_summaries(df)
    prod = product_summaries(df)
    pref = prefecture_summary(df)
    reps_c, reps_comp = reps_summaries(df)
    pq = price_quantity_bins(df)

    # 可視化
    html_parts = build_figures(df, mon, cg, store, prod, pref, reps_c, reps_comp)
    html_full = build_dashboard_html(html_parts, title="売上ダッシュボード（基礎分析）")

    # 保存（CSV）
    outputs = {
        "monthly_summary": mon,
        "customer_group_summary": cg,
        "store_summary": store,
        "product_summary": prod,
        "prefecture_summary": pref,
        "reps_customer_summary": reps_c,
        "reps_company_summary": reps_comp,
        "price_quantity_bins": pq,
        "quality_issues": q,
    }
    save_dataframes(cfg.output_dir, outputs)

    # 保存（HTML）
    html_path = save_dashboard_html(cfg.output_dir, html_full, open_after=cfg.open_html_after_save)

    # コンソールに要約を出力（学習・検収用）
    print("=== KPI Summary ===")
    for k, v in kpis.items():
        print(f"{k}: {v}")
    print("\n出力先:")
    for name in outputs.keys():
        print(f" - {name}.csv -> {cfg.output_dir / (name + '.csv')}")
    print(f" - dashboard -> {html_path}")
    # 出力ファイルの場所を一覧表示します。


def parse_args() -> AnalysisConfig:
    p = argparse.ArgumentParser(
        description="売上CSVの基礎分析を実施し、集計済みCSVとPlotly/HTMLダッシュボードを出力します。",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--input", required=True, type=Path, help="入力CSVパス（UTF-8 / ヘッダは日本語列名）")
    p.add_argument("--outdir", required=True, type=Path, help="出力ディレクトリ")
    p.add_argument("--start", type=str, default=None, help="分析開始日（YYYY-MM-DD）。省略時は全期間")
    p.add_argument("--end", type=str, default=None, help="分析終了日（YYYY-MM-DD）。省略時は全期間")
    p.add_argument("--tol", type=float, default=0.01, help="単価×個数と合計金額の一致許容差（品質チェック）")
    p.add_argument("--open-html", action="store_true", help="出力後にHTMLを既定ブラウザで開く（macOSなど）")

    a = p.parse_args()
    # 実際に引数を解析します。
    start = pd.to_datetime(a.start) if a.start else None
    end = pd.to_datetime(a.end) if a.end else None
    # 文字列日付をTimestampに変換します。
    return AnalysisConfig(
        input_csv=a.input,
        output_dir=a.outdir,
        start_date=start,
        end_date=end,
        price_multiplication_tolerance=a.tol,
        open_html_after_save=a.open_html,
    )
    # 解析結果を設定オブジェクトにまとめて返します。


if __name__ == "__main__":
    cfg = parse_args()
    # 引数を解析して設定を得ます。
    main(cfg)
    # メイン処理を実行します。


