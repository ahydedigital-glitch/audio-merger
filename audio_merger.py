import os
import boto3
from flask import Flask, request, jsonify
import subprocess

app = Flask(__name__)

# Environment variables
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")
R2_ENDPOINT = os.getenv("R2_ENDPOINT")
R2_BUCKET = os.getenv("R2_BUCKET")

# R2 S3 client
s3 = boto3.client(
    "s3",
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY
)


@app.route("/merge", methods=["POST"])
def merge():
    try:
        data = request.json
        input_files = data["files"]
        output_name = data["output"]

        local_files = []

        # ---------------------------
        # CLEAN OLD OUTPUT FILE
        # ---------------------------
        if os.path.exists(output_name):
            print(f"[INFO] Removing old existing output file: {output_name}")
            os.remove(output_name)

        # ---------------------------
        # DOWNLOAD ALL INPUT TRACKS
        # ---------------------------
        for f in input_files:
            local_name = os.path.basename(f)

            print(f"[INFO] Downloading: {f} as {local_name}")
            s3.download_file(R2_BUCKET, f, local_name)
            local_files.append(local_name)

        # ---------------------------
        # CREATE MERGE LIST
        # ---------------------------
        with open("merge_list.txt", "w") as m:
            for local_name in local_files:
                m.write(f"file '{local_name}'\n")

        print("[INFO] Created merge_list.txt")

        # ---------------------------
        # RUN FFMPEG CONCAT
        # ---------------------------
        cmd = [
            "ffmpeg",
            "-y",                   # <- FORCE OVERWRITE (IMPORTANT)
            "-f", "concat",
            "-safe", "0",
            "-i", "merge_list.txt",
            "-c", "copy",
            output_name
        ]

        print(f"[INFO] Running FFmpeg merge → {output_name}")
        subprocess.run(cmd, check=True)

        # ---------------------------
        # UPLOAD MERGED FILE TO R2
        # ---------------------------
        print(f"[INFO] Uploading merged file: {output_name} to R2 bucket")
        s3.upload_file(output_name, R2_BUCKET, output_name)

        # ---------------------------
        # CLEANUP TEMP FILES
        # ---------------------------
        try:
            os.remove("merge_list.txt")
            for f in local_files:
                if os.path.exists(f):
                    os.remove(f)
        except:
            pass  # Not critical

        print("[INFO] Merge completed successfully.")

        return jsonify({
            "status": "success",
            "merged_file": output_name
        })

    except Exception as e:
        print("[ERROR] Merge failed:", e)
        return jsonify({"error": str(e)}), 500


@app.route("/", methods=["GET"])
def home():
    return "Audio Merger Running"


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
