import csv
import json
import sys

def main(in_path: str, out_path: str):
    with open(in_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, dict):
        data = [data]

    rows = []
    for item in data:
        author = item.get("author_name")
        rating = item.get("rating")
        if author is None or rating is None:
            continue
        rows.append({"author_name": author, "rating": rating})

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["author_name", "rating"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"OK: wrote {out_path} ({len(rows)} rows)")

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
