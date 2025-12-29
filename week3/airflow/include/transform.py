"""Transform helpers cho DAG Airflow."""

from __future__ import annotations

from typing import Any, List, Dict


def compute_average_rating(places: List[Dict[str, Any]]) -> float:
	"""Tính average rating, fallback 0.0 nếu danh sách rỗng."""

	if not places:
		return 0.0

	ratings = [float(p.get("rating") or 0.0) for p in places]
	return round(sum(ratings) / len(ratings), 2)
