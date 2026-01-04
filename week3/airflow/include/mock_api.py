"""Mock API helpers cho bước extract."""

from __future__ import annotations

import logging
from random import Random
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def fetch_places(seed: int = 42, **context: Any) -> List[Dict[str, Any]]:
	"""
	Sinh dữ liệu mock cho danh sách địa điểm với rating.

	Dùng seed cố định để có kết quả lặp lại trong log/test.
	Trả về danh sách dict để PythonOperator push lên XCom.
	"""

	# Cho phép override qua op_kwargs hoặc params.seed
	seed_override = context.get("params", {}).get("seed")
	rand = Random(seed_override or seed)

	base_places = [
		{"name": "Cafe", "rating": 4.5},
		{"name": "Bistro", "rating": 4.0},
		{"name": "Bakery", "rating": 3.8},
		{"name": "Steakhouse", "rating": 4.7},
		{"name": "Noodle", "rating": 4.2},
	]

	jittered = []
	for place in base_places:
		jitter = rand.uniform(-0.15, 0.15)
		rating = max(0.0, min(5.0, round(place["rating"] + jitter, 2)))
		jittered.append({**place, "rating": rating})

	logger.info("Generated %d mock places", len(jittered))
	logger.debug("Mock places payload: %s", jittered)
	return jittered
