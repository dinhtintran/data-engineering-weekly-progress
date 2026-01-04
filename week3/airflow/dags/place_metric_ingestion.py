"""DAG place_metric_ingestion: extract -> transform -> load."""

from __future__ import annotations

from datetime import timedelta
from typing import Any, List, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.utils.dates import days_ago

from include.mock_api import fetch_places
from include.transform import compute_average_rating


default_args = {
	"owner": "data-eng",
	"retries": 1,
	"retry_delay": timedelta(minutes=2),
}


def _transform_average(ti: Any) -> float:
	places: List[Dict[str, Any]] = ti.xcom_pull(task_ids="extract_places") or []
	avg = compute_average_rating(places)
	return avg


with DAG(
	dag_id="place_metric_ingestion",
	description="Extract mock places, compute avg rating, load into Daily_Metrics",
	default_args=default_args,
	schedule_interval="@daily",
	start_date=days_ago(1),
	catchup=False,
	tags=["places", "training", "example"],
) as dag:

	extract = PythonOperator(
		task_id="extract_places",
		python_callable=fetch_places,
		op_kwargs={"seed": 42},
		do_xcom_push=True,
	)

	transform = PythonOperator(
		task_id="transform_average",
		python_callable=_transform_average,
	)

	load = SqliteOperator(
		task_id="load_daily_metric",
		sqlite_conn_id="sqlite_default",
		sql="""
		CREATE TABLE IF NOT EXISTS Daily_Metrics (
			ds TEXT PRIMARY KEY,
			avg_rating REAL NOT NULL,
			updated_at TEXT DEFAULT CURRENT_TIMESTAMP
		);

		INSERT INTO Daily_Metrics (ds, avg_rating, updated_at)
		VALUES ('{{ ds }}', {{ ti.xcom_pull(task_ids="transform_average") | default(0) }}, CURRENT_TIMESTAMP)
		ON CONFLICT(ds) DO UPDATE SET
			avg_rating = excluded.avg_rating,
			updated_at = CURRENT_TIMESTAMP;
		""",
	)

	extract >> transform >> load

