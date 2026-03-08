import pandas as pd
import sys
import os

def read_and_display_parquet(file_path):
    """
    Đọc và hiển thị dữ liệu từ file Parquet
    """
    if not os.path.exists(file_path):
        print(f"[-] Lỗi: Không tìm thấy file tại đường dẫn: {file_path}")
        return

    try:
        print(f"[*] Đang đọc file: {file_path} ...\n")
        
        # Đọc file parquet bằng pandas
        df = pd.read_parquet(file_path)
        
        # In ra thông tin tổng quan của DataFrame
        print("="*50)
        print("THÔNG TIN DATAFRAME (df.info()):")
        print("="*50)
        df.info()
        print("\n")
        
        # In ra 10 dòng đầu tiên
        print("="*50)
        print("10 DÒNG ĐẦU TIÊN (df.head(10)):")
        print("="*50)
        # Thiết lập pandas hiển thị tất cả các cột để dễ xem
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)
        print(df.head(10))
        
        print("\n")
        print(f"[+] Tổng số dòng dữ liệu: {len(df)}")
        
    except Exception as e:
        print(f"[-] Lỗi khi đọc file Parquet: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("[-] Lỗi: Bạn chưa cung cấp đường dẫn file.")
        print("Cách sử dụng: python read_parquet.py <đường_dẫn_đến_file.parquet>")
        sys.exit(1)
        
    target_file = sys.argv[1]
    read_and_display_parquet(target_file)
