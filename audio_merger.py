import os
import re
import shutil
import tempfile
from functools import lru_cache
from typing import Iterable, List

import boto3
import ffmpeg

# ------------------------------
# ENVIRONMENT VARIABLES (Render)
# ------------------------------

S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
S3_BUCKET = os.getenv("S3_BUCKET")

TRACKS_PREFIX = os.getenv("TRACKS_PREFIX", "tracks/")
OUTPUT_KEY = os.getenv("OUTPUT_KEY", "merged-output.mp3")
EXPECTED_TRACK_COUNT = int(os.getenv("EXPECTED_TRACK_COUNT", "45"))

# ------------------------------
# INIT S3 CLIENT (Cloudflare R2)
# ------------------------------

def _require_env(value: str, name: str) -> str:
    if not value:
        raise EnvironmentError(
            f"Environment variable '{name}' is required. "
            "Set it in the Render service environment or render.yaml envVars."
        )
    return value


@lru_cache(maxsize=1)
def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=_require_env(S3_ENDPOINT, "S3_ENDPOINT"),
        aws_access_key_id=_require_env(S3_ACCESS_KEY, "S3_ACCESS_KEY"),
        aws_secret_access_key=_require_env(S3_SECRET_KEY, "S3_SECRET_KEY"),
    )


def _require_ffmpeg_binary():
    if not shutil.which("ffmpeg"):
        raise EnvironmentError(
            "System ffmpeg binary not found. Ensure ffmpeg is installed in the runtime image."
        )

# ------------------------------
# UTILITIES
# ------------------------------

def _natural_key(path: str):
    """Return a key that sorts strings with embedded numbers naturally."""

    return [int(part) if part.isdigit() else part.lower() for part in re.split(r"(\d+)", path)]


def _iter_object_keys(prefix: str) -> Iterable[str]:
    """Yield object keys under the given prefix, handling pagination."""

    paginator = _s3_client().get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=_require_env(S3_BUCKET, "S3_BUCKET"), Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"]


def download_tracks() -> List[str]:
    print("📥 Listing MP3 tracks in R2...")

    files = [key for key in _iter_object_keys(TRACKS_PREFIX) if key.endswith(".mp3")]

    if not files:
        raise Exception("No MP3 files found in bucket folder.")

    files.sort(key=_natural_key)

    print(f"📄 Found {len(files)} files")

    if EXPECTED_TRACK_COUNT and len(files) != EXPECTED_TRACK_COUNT:
        raise Exception(
            f"Expected {EXPECTED_TRACK_COUNT} MP3 files but found {len(files)}. "
            "Please verify the upload is complete before merging."
        )

    temp_files: List[str] = []

    for idx, key in enumerate(files, start=1):
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".mp3")
        print(f"⬇️ [{idx}/{len(files)}] Downloading {key} → {tmp.name}")

        _s3_client().download_file(_require_env(S3_BUCKET, "S3_BUCKET"), key, tmp.name)
        temp_files.append(tmp.name)

    return temp_files


# ------------------------------
# MERGE TRACKS USING FFMPEG
# ------------------------------

def merge_tracks(temp_files: List[str]) -> str:
    print("🎶 Merging MP3 files...")

    list_file = tempfile.NamedTemporaryFile(delete=False, mode="w", suffix=".txt")

    for f in temp_files:
        list_file.write(f"file '{f}'\n")

    list_file.close()

    output_path = tempfile.NamedTemporaryFile(delete=False, suffix=".mp3").name

    try:
        (
            ffmpeg
            .input(list_file.name, format="concat", safe=0)
            .output(output_path, c="copy")
            .run(overwrite_output=True, capture_stdout=True, capture_stderr=True)
        )

    except ffmpeg.Error as e:
        print("❌ FFMPEG MERGE FAILED")
        print("---- STDOUT ----")
        print(e.stdout.decode('utf-8') if e.stdout else "NO STDOUT")
        print("---- STDERR ----")
        print(e.stderr.decode('utf-8') if e.stderr else "NO STDERR")
        raise Exception("FFMPEG merge failed. See logs above.")

    finally:
        try:
            os.remove(list_file.name)
        except OSError:
            pass

    print(f"✅ Merge complete → {output_path}")
    return output_path


# ------------------------------
# UPLOAD MERGED FILE
# ------------------------------

def upload_output(file_path: str):
    print(f"📤 Uploading final merged file → {OUTPUT_KEY}")

    _s3_client().upload_file(
        Filename=file_path,
        Bucket=_require_env(S3_BUCKET, "S3_BUCKET"),
        Key=OUTPUT_KEY,
        ExtraArgs={"ContentType": "audio/mpeg"}
    )

    print("✅ Upload complete!")


# ------------------------------
# MAIN HANDLER
# ------------------------------

def _cleanup(paths: Iterable[str]):
    for path in paths:
        try:
            os.remove(path)
        except OSError:
            pass


def handler(event=None, context=None):
    print("🚀 Starting audio merge job...")

    _require_ffmpeg_binary()

    temp_files: List[str] = []
    merged: str = ""

    try:
        temp_files = download_tracks()
        merged = merge_tracks(temp_files)
        upload_output(merged)
    finally:
        _cleanup(temp_files)
        if merged:
            _cleanup([merged])

    print("🎉 Job finished successfully!")
    return {"status": "ok"}


if __name__ == "__main__":
    handler()
