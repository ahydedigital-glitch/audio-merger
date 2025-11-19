import os
import boto3
import ffmpeg
from ffmpeg_static import ffmpeg_path
import tempfile

# -------------------------------------------------------
# ENVIRONMENT VARIABLES (set on Render dashboard)
# -------------------------------------------------------
S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
S3_BUCKET = os.getenv("S3_BUCKET")
TRACKS_PREFIX = os.getenv("TRACKS_PREFIX", "tracks/")  # folder
OUTPUT_KEY = os.getenv("OUTPUT_KEY", "merged-output.mp3")

# -------------------------------------------------------
# INIT S3 CLIENT (Cloudflare R2)
# -------------------------------------------------------
s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
)


# -------------------------------------------------------
# DOWNLOAD TRACKS
# -------------------------------------------------------
def download_tracks():
    print("🔍 Listing MP3 tracks in R2...")

    resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=TRACKS_PREFIX)

    if "Contents" not in resp:
        raise Exception("No MP3 files found in bucket folder.")

    files = [obj["Key"] for obj in resp["Contents"] if obj["Key"].endswith(".mp3")]
    files.sort()

    if len(files) == 0:
        raise Exception("Bucket contains zero .mp3 files")

    print(f"🎵 Found {len(files)} files")

    temp_files = []

    for key in files:
        local_path = os.path.join(tempfile.gettempdir(), key.replace("/", "_"))
        print(f"⬇️ Downloading {key}")
        s3.download_file(S3_BUCKET, key, local_path)
        temp_files.append(local_path)

    return temp_files


# -------------------------------------------------------
# MERGE MP3 FILES USING FFMPEG-STATIC
# -------------------------------------------------------
def merge_mp3(files, output_path):
    print("🎛️ Merging MP3 files...")

    inputs = [ffmpeg.input(f) for f in files]

    merged = ffmpeg.concat(*inputs, v=0, a=1).output(output_path)

    print("⚙️ Running ffmpeg...")
    ffmpeg.run(merged, cmd=ffmpeg_path)

    print("✅ Merge complete:", output_path)


# -------------------------------------------------------
# UPLOAD FINAL MP3 TO R2
# -------------------------------------------------------
def upload_output(output_path):
    print(f"⬆️ Uploading result as: {OUTPUT_KEY}")

    with open(output_path, "rb") as f:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=OUTPUT_KEY,
            Body=f,
            ContentType="audio/mpeg",
        )

    print("🎉 Upload complete!")


# -------------------------------------------------------
def main():
    print("🚀 Starting merger…")
    tracks = download_tracks()

    output_path = os.path.join(tempfile.gettempdir(), "merged-output.mp3")

    merge_mp3(tracks, output_path)
    upload_output(output_path)

    print("🔥 Finished!")
# -------------------------------------------------------

if __name__ == "__main__":
    main()
