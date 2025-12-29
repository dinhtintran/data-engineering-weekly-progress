"""DB helpers cho bước load."""

from __future__ import annotations

from typing import Any

from airflow.providers.sqlite.hooks.sqlite import SqliteHook


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS Daily_Metrics (
	ds TEXT PRIMARY KEY,
	avg_rating REAL NOT NULL,
	updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);
"""


def upsert_daily_metric(avg_rating: float, ds: str, conn_id: str = "sqlite_default") -> None:
	"""Tạo bảng nếu chưa có và upsert avg_rating cho ngày ds."""

	hook = SqliteHook(sqlite_conn_id=conn_id)
	with hook.get_conn() as conn:
		cur = conn.cursor()
		cur.execute(CREATE_TABLE_SQL)
		cur.execute(
			"""
			INSERT INTO Daily_Metrics (ds, avg_rating, updated_at)
			VALUES (?, ?, CURRENT_TIMESTAMP)
			ON CONFLICT(ds) DO UPDATE SET
				avg_rating = excluded.avg_rating,
				updated_at = CURRENT_TIMESTAMP;
			""",
			(ds, float(avg_rating)),
		)
		conn.commit()
