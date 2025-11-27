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


def trim_silence_from_file(input_file, output_file):
    """
    Remove trailing silence from an audio file using FFmpeg
    """
    cmd = [
        "ffmpeg",
        "-y",
        "-i", input_file,
        "-af", "silenceremove=stop_periods=-1:stop_duration=1:stop_threshold=-50dB",
        "-c:a", "libmp3lame",
        "-b:a", "192k",
        output_file
    ]
    print(f"[INFO] Trimming silence from: {input_file}")
    subprocess.run(cmd, check=True)


@app.route("/merge", methods=["POST"])
def merge():
    try:
        data = request.json
        input_files = data["files"]
        output_name = data["output"]
        crossfade_duration = data.get("crossfade", 3)  # Default 3 seconds

        local_files = []
        trimmed_files = []

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

        print(f"[INFO] Downloaded {len(local_files)} files")

        # ---------------------------
        # TRIM SILENCE FROM EACH TRACK
        # ---------------------------
        for i, local_file in enumerate(local_files):
            trimmed_name = f"trimmed_{i}.mp3"
            trim_silence_from_file(local_file, trimmed_name)
            trimmed_files.append(trimmed_name)
            # Clean up original
            os.remove(local_file)

        print(f"[INFO] Trimmed {len(trimmed_files)} files")

        # ---------------------------
        # BUILD FFMPEG CROSSFADE FILTER
        # ---------------------------
        
        if len(trimmed_files) == 1:
            # Only one file, no crossfade needed
            print("[INFO] Single file detected, skipping crossfade")
            cmd = [
                "ffmpeg", "-y",
                "-i", trimmed_files[0],
                "-c", "copy",
                output_name
            ]
        else:
            # Multiple files - build crossfade chain
            inputs = []
            for f in trimmed_files:
                inputs.extend(["-i", f])
            
            # Build crossfade filter chain
            # For N tracks: [0][1]acrossfade[a0]; [a0][2]acrossfade[a1]; ...  [a{N-3}][N-1]acrossfade[out]
            filter_parts = []
            num_tracks = len(trimmed_files)
            
            for i in range(num_tracks - 1):
                if i == 0:
                    # First crossfade: [0][1] -> [a0]
                    left = "0"
                    right = "1"
                else:
                    # Subsequent: [a{i-1}][i+1] -> [a{i}]
                    left = f"a{i-1}"
                    right = str(i + 1)
                
                if i == num_tracks - 2:
                    # Last crossfade outputs to final stream
                    output_label = "out"
                else:
                    output_label = f"a{i}"
                
                filter_parts.append(
                    f"[{left}][{right}]acrossfade=d={crossfade_duration}:c1=tri:c2=tri[{output_label}]"
                )
            
            filter_complex = ";".join(filter_parts)
            
            print(f"[INFO] Building crossfade filter for {num_tracks} tracks")
            print(f"[DEBUG] Filter: {filter_complex}")
            
            cmd = ["ffmpeg", "-y"]
            cmd.extend(inputs)
            cmd.extend([
                "-filter_complex", filter_complex,
                "-map", "[out]",
                "-c:a", "libmp3lame",
                "-b:a", "192k",
                output_name
            ])

        # ---------------------------
        # RUN FFMPEG WITH CROSSFADE
        # ---------------------------
        print(f"[INFO] Running FFmpeg merge with {crossfade_duration}s crossfade → {output_name}")
        print(f"[DEBUG] Command: {' '.join(cmd)}")
        
        result = subprocess. run(cmd, check=True, capture_output=True, text=True)
        print(f"[DEBUG] FFmpeg stdout: {result.stdout}")
        if result.stderr:
            print(f"[DEBUG] FFmpeg stderr: {result.stderr}")

        # ---------------------------
        # UPLOAD MERGED FILE TO R2
        # ---------------------------
        print(f"[INFO] Uploading merged file: {output_name} to R2 bucket")
        s3.upload_file(output_name, R2_BUCKET, output_name)

        # ---------------------------
        # CLEANUP TEMP FILES
        # ---------------------------
        try:
            for f in trimmed_files:
                if os.path.exists(f):
                    os.remove(f)
            if os.path.exists(output_name):
                os.remove(output_name)
        except Exception as cleanup_error:
            print(f"[WARN] Cleanup failed: {cleanup_error}")

        print("[INFO] Merge completed successfully with crossfade.")

        return jsonify({
            "status": "success",
            "merged_file": output_name,
            "tracks_processed": len(input_files),
            "crossfade_duration": crossfade_duration
        })

    except subprocess.CalledProcessError as e:
        print(f"[ERROR] FFmpeg failed: {e}")
        print(f"[ERROR] FFmpeg stderr: {e.stderr}")
        return jsonify({"error": f"FFmpeg error: {e. stderr}"}), 500
    except Exception as e:
        print(f"[ERROR] Merge failed: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/", methods=["GET"])
def home():
    return "Audio Merger Running (with Crossfade & Silence Trimming)"


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy", "version": "2.0-crossfade"})


if __name__ == "__main__":
    app.run(host="0. 0.0.0", port=10000)
