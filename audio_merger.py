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

    for f in input_files:
        s3.download_file(R2_BUCKET, f, f)

    with open("merge_list.txt", "w") as m:
        for f in input_files:
            m.write(f"file '{}'\n".format(f))

    cmd = [
        "ffmpeg",
        "-f", "concat",
        "-safe", "0",
        "-i", "merge_list.txt",
        "-c", "copy",
        output_name
    ]

    subprocess.run(cmd, check=True)

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
