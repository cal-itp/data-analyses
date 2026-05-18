from pathlib import Path
from typing import Tuple
from urllib.parse import urlparse

from google.cloud import storage


def parse_gs_url(url: str) -> Tuple[str, str]:
    """Split `gs://bucket/path/to/thing` into ('bucket', 'path/to/thing')."""
    parsed = urlparse(url)
    if parsed.scheme != "gs" or not parsed.netloc:
        raise ValueError(f"expected gs://bucket/... URL, got {url!r}")
    return parsed.netloc, parsed.path.lstrip("/").rstrip("/")


def upload_file(target_url: str, local_file: Path) -> None:
    """Upload `local_file` to `target_url` (gs://bucket/path/to/blob)."""
    bucket_name, blob_name = parse_gs_url(target_url)
    if not blob_name:
        raise ValueError(f"target_url must include a blob path, got {target_url!r}")
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    bucket.blob(blob_name).upload_from_filename(str(local_file))


def upload_directory(target_url: str, local_dir: Path) -> int:
    """Upload every file under `local_dir` to `gs://bucket/prefix/<relative path>`. Returns file count."""
    bucket_name, prefix = parse_gs_url(target_url)
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    count = 0
    for path in sorted(local_dir.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(local_dir).as_posix()
        blob_name = f"{prefix}/{rel}" if prefix else rel
        bucket.blob(blob_name).upload_from_filename(str(path))
        count += 1
    return count
