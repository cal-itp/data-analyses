import re
from pathlib import Path

import typer
from google.cloud import storage
from rich.progress import track

from calitp_portfolio.models import parse_gs_url

# myst-cli emits content-hashed asset names like `root-HKSGTRTI.css` or `entry.client-PCJPW7TK.js`.
# For these, a name match in the bucket implies a content match, so the upload can be skipped.
_HASHED_ASSET_RE = re.compile(r"-[A-Z0-9]{8,}\.")


def _is_hashed_asset(name: str) -> bool:
    return bool(_HASHED_ASSET_RE.search(name))


def upload_file(target_url: str, local_file: Path) -> None:
    """Upload `local_file` to `target_url` (gs://bucket/path/to/blob)."""
    bucket_name, blob_name = parse_gs_url(target_url)
    if not blob_name:
        raise ValueError(f"target_url must include a blob path, got {target_url!r}")
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    bucket.blob(blob_name).upload_from_filename(str(local_file))


def upload_directory(target_url: str, local_dir: Path) -> int:
    """Sync `local_dir` to `gs://bucket/prefix/`.

    Skips uploads for hashed-asset names already present in the bucket (myst content-hashes
    asset filenames, so name match = content match). After the upload loop, deletes any
    blobs at the prefix that aren't in the local set. Returns total local file count.
    """
    bucket_name, prefix = parse_gs_url(target_url)
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    files = [p for p in sorted(local_dir.rglob("*")) if p.is_file()]

    def blob_name_for(path: Path) -> str:
        rel = path.relative_to(local_dir).as_posix()
        return f"{prefix}/{rel}" if prefix else rel

    local_blob_names = {blob_name_for(p) for p in files}

    # Trailing slash scopes the listing so prefix="ahsc" doesn't pick up "ahsc-2/...".
    list_prefix = f"{prefix}/" if prefix else None
    existing = {b.name for b in bucket.list_blobs(prefix=list_prefix)}

    typer.echo(f"uploading {len(files)} files from {local_dir} to {target_url}")

    skipped = 0
    for path in track(files, description="uploading"):
        blob_name = blob_name_for(path)
        if blob_name in existing and _is_hashed_asset(blob_name):
            skipped += 1
            continue
        bucket.blob(blob_name).upload_from_filename(str(path))

    if skipped:
        typer.echo(f"skipped {skipped} unchanged hashed assets")

    stale = sorted(existing - local_blob_names)
    if stale:
        typer.echo(f"deleting {len(stale)} stale files from {target_url}")
        for name in track(stale, description="deleting stale"):
            bucket.blob(name).delete()

    return len(files)
