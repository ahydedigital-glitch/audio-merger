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

    # Download each file locally (strip folder paths)
    local_files = []
    for f in input_files:
        local_name = os.path.basename(f)  # Only filename, not the folder path
        local_files.append(local_name)

        try:
            s3.download_file(R2_BUCKET, f, local_name)
        except Exception as e:
            return jsonify({"error": f"Cannot download {f}", "details": str(e)}), 400

    # Build the merge list file
    with open("merge_list.txt", "w") as m:
        for local_name in local_files:
            m.write(f"file '{local_name}'\n")

    # Run ffmpeg concat
    cmd = [
        "ffmpeg",
        "-f", "concat",
        "-safe", "0",
        "-i", "merge_list.txt",
        "-c", "copy",
        output_name
    ]

    try:
        subprocess.run(cmd, check=True)
    except Exception as e:
        return jsonify({"error": "ffmpeg failed", "details": str(e)}), 500

    # Upload merged output back to R2
    try:
        s3.upload_file(output_name, R2_BUCKET, output_name)
    except Exception as e:
        return jsonify({"error": "Upload failed", "details": str(e)}), 500

    return jsonify({
        "status": "success",
        "merged_file": output_name
    })

@app.route("/", methods=["GET"])
def home():
    return "Audio Merger Running"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
