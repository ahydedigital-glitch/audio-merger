import os
import boto3
from flask import Flask, request, jsonify
import subprocess

app = Flask(__name__)

R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")
R2_ENDPOINT = os.getenv("R2_ENDPOINT")
R2_BUCKET = os.getenv("R2_BUCKET")

s3 = boto3.client(
    "s3",
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY
)

@app.route("/merge", methods=["POST"])
def merge():
    data = request.json
    input_files = data["files"]
    output_name = data["output"]

    local_files = []

    # Download each file locally using only the base filename
    for f in input_files:
        local_name = os.path.basename(f)
        s3.download_file(R2_BUCKET, f, local_name)
        local_files.append(local_name)

    # Create merge list
    with open("merge_list.txt", "w") as m:
        for local_name in local_files:
            m.write(f"file '{local_name}'\n")

    # FFmpeg command
    cmd = [
        "ffmpeg",
        "-f", "concat",
        "-safe", "0",
        "-i", "merge_list.txt",
        "-c", "copy",
        output_name
    ]

    subprocess.run(cmd, check=True)

    # Upload merged result back to R2
    s3.upload_file(output_name, R2_BUCKET, output_name)

    return jsonify({
        "status": "success",
        "merged_file": output_name
    })


@app.route("/", methods=["GET"])
def home():
    return "Audio Merger Running"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
