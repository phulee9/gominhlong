"""
check_header.py - In ra N dòng đầu của 1 sheet Excel để xác định header_row/data_start_row
nên khai báo bao nhiêu trong pipeline_config.yaml.

Dùng:
    python check_header.py <duong_dan_file.xlsx> [so_dong] [sheet_name_hoac_index]
"""
import sys

import pandas as pd


def main():
    if len(sys.argv) < 2:
        print("Dùng: python check_header.py <duong_dan_file.xlsx> [so_dong=10] [sheet]")
        sys.exit(1)

    file_path = sys.argv[1]
    n_rows = int(sys.argv[2]) if len(sys.argv) > 2 else 10
    sheet = sys.argv[3] if len(sys.argv) > 3 else 0
    try:
        sheet = int(sheet)
    except ValueError:
        pass  # giữ là sheet_name (string)

    df = pd.read_excel(file_path, sheet_name=sheet, header=None, engine="openpyxl")

    for i in range(min(n_rows, len(df))):
        print(f"Row {i}: {df.iloc[i].tolist()}")


if __name__ == "__main__":
    main()
