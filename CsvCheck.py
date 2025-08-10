import csv

CSV_PATH = "//”ファイルパスをここに記載。”//sample_sales_data.csv"

def detect_encoding(path: str) -> str:
    # 1) BOM検知でUTF-8 with BOMを優先
    with open(path, "rb") as fb:
        start = fb.read(4)
        if start.startswith(b"\xef\xbb\xbf"):
            return "utf-8-sig"
    # 2) utf-8で読めるか試行 → ダメならcp932（日本語Windows系）
    for enc in ("utf-8", "cp932"):
        try:
            with open(path, "r", encoding=enc) as f:
                f.read(4096)
            return enc
        except UnicodeDecodeError:
            continue
    # 3) 最後の手段
    return "utf-8"

def read_header_and_count(path: str, encoding: str):
    # Snifferで区切り文字を推定（基本はカンマ）
    with open(path, "r", encoding=encoding, newline="") as f:
        sample = f.read(8192)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=[",", "\t", ";", "|"])
        except Exception:
            dialect = csv.excel
        reader = csv.reader(f, dialect)
        try:
            header = next(reader)
        except StopIteration:
            return [], 0  # 空ファイル
        # 行数カウント（ヘッダ除く）
        row_count = sum(1 for _ in reader)
    return header, row_count


enc = detect_encoding(CSV_PATH)
print(f"[推定エンコーディング] {enc}")

header, n_rows = read_header_and_count(CSV_PATH, enc)
print(f"[カラム数] {len(header)}")
print(f"[行数(ヘッダ除く)] {n_rows}")
print("[ヘッダ]")
print(", ".join(header))
