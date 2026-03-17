from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum

# Cài đặt múi giờ Việt Nam
local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

import os
import requests
from dotenv import load_dotenv

# Load Telegram credentials
load_dotenv(dotenv_path="/opt/airflow/.env.telegram")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def send_telegram_success_msg(context):
    """Gửi thông báo Telegram khi Task chạy thành công"""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Thiếu cấu hình Telegram Token hoặc Chat ID, bỏ qua gửi thông báo.")
        return

    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date').in_timezone(local_tz).strftime('%Y-%m-%d %H:%M:%S')
    
    message = (
        f"✅ *Task Thành Công!*\n\n"
        f"• *Task ID:* `{task_id}`\n"
        f"• *Thời gian chạy:* `{execution_date}`\n"
        f"• *Dự án:* Weather Forecast"
    )

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message,
        'parse_mode': 'Markdown'
    }
    
    try:
        requests.post(url, json=payload)
    except Exception as e:
        print(f"Lỗi khi gửi thông báo Telegram: {e}")

# Thông số chung cho các DAGs
default_args = {
    'owner': 'duc1808',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'on_success_callback': send_telegram_success_msg,
}

# Khai báo đường dẫn trỏ tới thư mục script của bạn
SCRIPT_DIR = "/opt/airflow/scripts"

# =====================================================================
# DAG 1: LẤY DỮ LIỆU DỰ BÁO (1 Lần/Ngày lúc 0h05 sáng)
# Lấy trước 40 mốc thời gian của 5 ngày tới
# =====================================================================
dag_predict = DAG(
    '1_weather_collect_forecast',
    default_args=default_args,
    description='Call OpenWeatherMap 5-day Forecast API each 00:05 AM',
    schedule='5 0 * * *', # Chạy lúc 00:05 sáng mỗi ngày
    start_date=datetime(2026, 3, 7, tzinfo=local_tz),
    catchup=False,
    tags=['weather', 'forecast'],
)

task_collect_predict = BashOperator(
    task_id='collect_forecast_data',
    bash_command=f'python3 {SCRIPT_DIR}/collect_forecast_data.py',
    dag=dag_predict,
)

# =====================================================================
# DAG 2: LẤY DỮ LIỆU THỰC TẾ (8 Lần/Ngày lúc 1h05, 4h05, 7h05...)
# Cào số liệu thực khớp với thời điểm dự báo của OWM
# =====================================================================
dag_actual = DAG(
    '2_weather_collect_actual',
    default_args=default_args,
    description='Call OpenWeatherMap Current Weather API 8 times/day',
    schedule='5 1,4,7,10,13,16,19,22 * * *', # Chạy lúc x:05
    start_date=datetime(2026, 3, 7, tzinfo=local_tz),
    catchup=False,
    tags=['weather', 'actual'],
)

task_collect_actual = BashOperator(
    task_id='collect_actual_data',
    bash_command=f'python3 {SCRIPT_DIR}/collect_actual_data.py',
    dag=dag_actual,
)

# =====================================================================
# DAG 3: SPARK TRANSFORM BẢNG JOIN (1 Lần/Ngày lúc 23h50 đêm)
# Đọc cục HDFS hôm nay, Join lại và nén thành ML Feature Ready Dataset
# =====================================================================
dag_transform = DAG(
    '3_weather_spark_transform',
    default_args=default_args,
    description='Run Spark to merge Forecast and Actual data into ML Label',
    schedule='50 23 * * *', # Chạy lúc 23:50 phút mỗi tối
    start_date=datetime(2026, 3, 7, tzinfo=local_tz),
    catchup=False,
    tags=['weather', 'spark', 'etl'],
)

task_run_transform = BashOperator(
    task_id='run_spark_transform',
    bash_command=f'python3 {SCRIPT_DIR}/transform.py',
    dag=dag_transform,
)

# PHẦN MODEL SE ĐƯỢC THÊM SAU:
# task_train_model = BashOperator(
#     task_id='train_error_prediction_model',
#     bash_command=f'python3 {SCRIPT_DIR}/train.py',
#     dag=dag_transform,
# )
# task_run_transform >> task_train_model
