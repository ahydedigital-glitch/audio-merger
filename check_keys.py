import boto3, os

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("R2_ENDPOINT"),
    aws_access_key_id=os.getenv("R2_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("R2_SECRET_KEY")
)

bucket = os.getenv("R2_BUCKET")

files = [f"tracks/track_{i}.mp3" for i in range(46)]

print("\nChecking all files...\n")

for f in files:
    try:
        s3.head_object(Bucket=bucket, Key=f)
        print("OK:", f)
    except Exception as e:
        print("MISSING:", f, " → ", str(e)[:200])
