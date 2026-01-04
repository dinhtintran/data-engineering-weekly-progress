# Week 3 - Airflow Orchestration

Goals
- Build familiarity with DAGs, tasks, scheduling, logging, retries, and SLA basics.
- Deliver a 3-step DAG `place_metric_ingestion` (extract → transform → load) with XCom handoff and DB write.

Planned Milestones
1) Scaffold Airflow project layout (dags, include/utils, tests, logs/ignored). 
2) Implement mock API call (PythonOperator) pushing list/summary to XCom.
3) Implement transform step computing overall average rating from XCom payload.
4) Implement load step inserting/upserting average rating into `Daily_Metrics` via DB operator (sqlite/postgres).
5) Validate end-to-end run, confirm logs show XCom flow and DB update, and document run steps.

Runbook (initial draft)
- Tạo venv, cài Airflow bằng constraints: xem `week3/requirements.txt` hoặc
	```
	export AIRFLOW_VERSION=2.9.2
	export PYTHON_VERSION=3.9
	export CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
	pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
	pip install apache-airflow-providers-sqlite==3.8.2
	```
- Đặt `AIRFLOW_HOME=$(pwd)/week3/airflow` (hoặc dùng riêng) trước khi init.
- Khởi tạo: `airflow db init`; tạo user: `airflow users create ...`; chạy webserver + scheduler.
- Kết nối DB cho load task (sqlite demo):
	```
	mkdir -p "$AIRFLOW_HOME"
	airflow connections add sqlite_default --conn-uri "sqlite:///${AIRFLOW_HOME}/metrics.db"
	```
- DAG sẵn ở `airflow/dags/`; helpers ở `airflow/include/`.

Outcome checklist alignment
- DAG named `place_metric_ingestion` with 3 sequential tasks.
- Task1: PythonOperator -> mock API -> XCom list or count.
- Task2: PythonOperator -> compute average rating -> XCom single value.
- Task3: DB operator -> read XCom -> insert/update `Daily_Metrics`.
- Logs verify XCom transfer and DB change.

Next actions
- Fill in DAG and helper modules under `airflow/dags` and `airflow/include`.
- Add lightweight tests under `airflow/tests` (unit + dag integrity).
- Add connection/variable notes to this README when wiring DB.
