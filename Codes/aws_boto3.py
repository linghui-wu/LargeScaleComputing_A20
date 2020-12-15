import boto3

# Initialize Boto Client for S3
s3 = boto3.resource("s3")

# Create bucket
s3.create_bucket(Bucket="my-bucket")  # Universally unique name is requied

# Put objects into bucket
data = open("test.jpg", "rb")
s3.Bucket("my-bucket").put_object(Key="test.jpg", Body=data)

