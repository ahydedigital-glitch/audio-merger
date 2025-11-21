import os
import boto3
import ffmpeg
import tempfile
import sys

# ------------------------------
# ENVIRONMENT VARIABLES (Render)
# ------------------------------

S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
S3_BUCKET = os.getenv("S3_BUCKET")

TRACKS_PREFIX = os.getenv("TRACKS_PREFIX", "tracks/")
OUTPUT_KEY = os.getenv("OUTPUT_KEY", "merged-output.mp3")

# ------------------------------
# INIT S3 CLIENT (Cloudflare R2)
# ------------------------------

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
)

# ------------------------------
# DOWNLOAD TRACKS
# ------------------------------

def download_tracks():
    print("📥 Listing MP3 tracks in R2...")

    resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=TRACKS_PREFIX)

    if "Contents" not in resp:
        raise Exception("No MP3 files found in bucket folder.")

    files = [
        obj["Key"] for obj in resp["Contents"]
        if obj["Key"].endswith(".mp3")
    ]

    if len(files) == 0:
        raise Exception("Bucket contains zero MP3 files.")

    print(f"📄 Found {len(files)} files")

    temp_files = []

    for key in sorted(files):
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".mp3")
        print(f"⬇️ Downloading {key} → {tmp.name}")
        s3.download_file(S3_BUCKET, key, tmp.name)
        temp_files.append(tmp.name)

    return temp_files


# ------------------------------
# MERGE TRACKS USING FFMPEG
# ------------------------------

def merge_tracks(temp_files):
    print("🎶 Merging MP3 files...")

    # Build the concat list file
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
            .run(overwrite_output=True)
        )

    except ffmpeg.Error as e:
        print("\n❌ FFMPEG MERGE FAILED")
        print("---- STDOUT ----")
        print(e.stdout.decode() if e.stdout else "")
        print("---- STDERR ----")
        print(e.stderr.decode() if e.stderr else "")
        raise Exception("FFMPEG merge failed — see logs above")

    print(f"✅ Merge complete → {output_path}")
    return output_path


# ------------------------------
# UPLOAD MERGED FILE
# ------------------------------

def upload_output(file_path):
    print(f"📤 Uploading final merged file → {OUTPUT_KEY}")

    s3.upload_file(
        Filename=file_path,
        Bucket=S3_BUCKET,
        Key=OUTPUT_KEY,
        ExtraArgs={"ContentType": "audio/mpeg"}
    )

    print("✅ Upload complete!")


# ------------------------------
# MAIN HANDLER
# ------------------------------

def handler(event=None, context=None):
    print("🚀 Starting audio merge job...")

    temp_files = download_tracks()
    merged = merge_tracks(temp_files)
    upload_output(merged)

    # cleanup
    for f in temp_files:
        try:
            os.remove(f)
        except:
            pass

    try:
        os.remove(merged)
    except:
        pass

    print("🎉 Job finished successfully!")
    return {"status": "ok"}


if __name__ == "__main__":
    handler()
