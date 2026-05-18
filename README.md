#  Weather Forecast ML Pipeline (Dự Báo Thời Tiết TP.HCM)

##  Giới thiệu
Dự án này là một **Data Pipeline** hoàn chỉnh từ khâu thu thập dữ liệu (Data Ingestion), tiền xử lý (ETL) đến áp dụng Machine Learning để hiệu chỉnh và dự đoán độ chính xác của dự báo thời tiết tại khu vực TP. Hồ Chí Minh.

Hệ thống tự động lấy dữ liệu từ OpenWeatherMap bằng **Apache Airflow**, sử dụng **Pandas** để xử lý tập dữ liệu, và áp dụng mô hình **Linear Regression** (Scikit-Learn) để dự báo nhiệt độ thực tế dựa trên các dữ liệu lịch sử sai số.

---

## Cấu trúc files (Project Structure)

```text
weather_forecast_project/
├── .env                  # Chứa biến môi trường (API Key)
├── .env.telegram         # Chứa Token Bot Telegram cho thông báo
├── .gitignore            # File bỏ qua cho Git
├── docker-compose.yaml   # Cấu hình các dịch vụ Airflow và PostgreSQL
├── requirement.txt       # Danh sách thư viện Python cần cài đặt
├── README.md             # Tài liệu hướng dẫn dự án
├── dags/
│   └── weather_forecast_dag.py  # Chứa các định nghĩa DAG cho Airflow
├── scripts/
│   ├── collect_actual_data.py   # Lấy dữ liệu thời tiết thực tế
│   ├── collect_forecast_data.py # Lấy dữ liệu thời tiết dự báo
│   ├── collect_forecast_data_for_predict.py # Lấy dữ liệu dự báo cho quá trình predict
│   ├── linear_regression.py     # Code huấn luyện và đánh giá mô hình hồi quy (phòng thí nghiệm)
│   ├── predict_today.py         # Dự đoán nhiệt độ thực tế của ngày hôm nay (chạy thực tế)
│   └── transform.py             # Xử lý ETL (làm sạch, nối bảng, tạo đặc trưng) bằng Pandas
└── data/
    ├── data/                    # Chứa dữ liệu thô (raw) và bộ dữ liệu Machine Learning (ml_ready)
    ├── raw_predict_data/        # Chứa dữ liệu dự báo dành riêng cho việc lấy kết quả hôm nay
    └── result_predict_today/    # Chứa kết quả dự đoán cuối cùng (file .csv)
```

---

## Cài đặt (Installation)

**1. Clone dự án và cài đặt môi trường ảo Python**
```bash
# Tạo môi trường ảo và kích hoạt
python3 -m venv .venv
source .venv/bin/activate  # Trên Windows dùng: .venv\Scripts\activate

# Cài đặt thư viện Python cần thiết
pip install -r requirement.txt
```

**2. Cấu hình biến môi trường (.env)**
Tạo file `.env` ở thư mục gốc của dự án với nội dung:
```env
AIRFLOW_UID=1000
OPENWEATHER_API_KEY=your_api_key_here
CITY_ID=1566083
```

*(Tùy chọn)* Nếu sử dụng tính năng thông báo Telegram khi Airflow chạy xong, cấu hình thêm `.env.telegram`:
```env
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
```

**3. Khởi tạo Apache Airflow qua Docker**
```bash
# Tạo các thư mục cần thiết
mkdir -p dags logs config plugins scripts data/raw_predict_data data/result_predict_today

# Khởi tạo Database (PostgreSQL) cho Airflow
docker compose up airflow-init

# Chạy toàn bộ hệ thống (Webserver, Scheduler, PostgreSQL) ở chế độ ngầm
docker compose up -d
```

---

## Cách chạy

Hệ thống cung cấp 2 chế độ: **Chạy tự động** để thu thập & huấn luyện dữ liệu và **Chạy thủ công** để lấy kết quả dự đoán hàng ngày.

### Cách 1: Chạy tự động thu thập & huấn luyện bằng Airflow
1. Truy cập Web UI của Airflow tại: `http://localhost:8080` (Tài khoản/Mật khẩu: `airflow`/`airflow`).
2. Bật (Unpause) 3 DAGs có sẵn trong giao diện:
   - `1_weather_collect_forecast`: Tự động chạy lúc 00:05 sáng để lấy "Đề bài" dự báo.
   - `2_weather_collect_actual`: Tự động chạy 8 lần/ngày để lấy "Đáp án" thời tiết thực tế.
   - `3_weather_pandas_transform`: Tự động chạy lúc 23:50 đêm bằng Pandas để gộp 2 bảng thành tập dữ liệu Machine Learning (`ml_ready_dataset`).

### Cách 2: Chạy dự báo lấy kết quả ngay cho ngày hôm nay
Nếu hệ thống đã có đủ file lịch sử (`ml_ready_dataset`), bạn có thể tiến hành lấy kết quả dự đoán nhiệt độ trong ngày hôm nay bằng cách chạy 2 lệnh sau ở Terminal:

```bash
# Bước 1: Kéo dữ liệu dự báo thô của hôm nay về
python scripts/collect_forecast_data_for_predict.py

# Bước 2: Model lập tức học dữ liệu lịch sử và hiệu chỉnh sai số cho dự báo của hôm nay
python scripts/predict_today.py
```
Kết quả dự đoán sẽ được xuất ra và lưu tại thư mục:
`data/result_predict_today/prediction_today_YYYYMMDD.csv`.
