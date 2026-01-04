import csv
import io
import json
from datetime import datetime
from google.cloud import storage

storage_client = storage.Client()

def gcs_trigger(event, context):
    bucket_name = event["bucket"]
    object_name = event["name"]

    # Chỉ xử lý file trong raw/
    if not object_name.startswith("raw/"):
        print(f"Skip (not raw/): {object_name}")
        return

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(object_name)

    raw_text = blob.download_as_text(encoding="utf-8")

    data = json.loads(raw_text)
    if isinstance(data, dict):
        data = [data]
    if not isinstance(data, list):
        raise ValueError("JSON must be object or list of objects")

    rows = []
    for item in data:
        author = item.get("author_name")
        rating = item.get("rating")
        if author is None or rating is None:
            continue
        rows.append({"author_name": author, "rating": rating})

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_name = f"staging/transformed_reviews_{ts}.csv"

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=["author_name", "rating"])
    writer.writeheader()
    writer.writerows(rows)

    out_blob = bucket.blob(out_name)
    out_blob.upload_from_string(output.getvalue(), content_type="text/csv")
    print(f"Wrote: {out_name} rows={len(rows)}")

    # move raw -> processed
    processed_name = object_name.replace("raw/", "processed/", 1)
    bucket.copy_blob(blob, bucket, new_name=processed_name)
    blob.delete()
    print(f"Moved raw -> {processed_name}")
