from pathlib import Path

import typer
from google.cloud import storage
from rich.progress import track

from calitp_portfolio.models import parse_gs_url


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

    files = [p for p in sorted(local_dir.rglob("*")) if p.is_file()]
    typer.echo(f"uploading {len(files)} files from {local_dir} to {target_url}")

    for path in track(files, description="uploading"):
        rel = path.relative_to(local_dir).as_posix()
        blob_name = f"{prefix}/{rel}" if prefix else rel
        bucket.blob(blob_name).upload_from_filename(str(path))
    return len(files)
