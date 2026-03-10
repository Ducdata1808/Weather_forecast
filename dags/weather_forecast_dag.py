from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum

# Cài đặt múi giờ Việt Nam
local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

# Thông số chung cho các DAGs
default_args = {
    'owner': 'duc1808',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
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
