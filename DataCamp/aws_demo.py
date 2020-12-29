import pandas as pd
import boto3

# Create boto3 client
s3 = boto3.client("s3",
    region_name="us-east-1",
    aws_acess_key_id=AWS_KEY_ID,
    aws_secret_access_key=AWS_SECRET)

# Create bucket
bucket = s3.create_bucket(Bucket="gid-requests")

# List buckets
bucket_response = s3.list_buckets()

# Get buckets dictionary
buckets = bucket_response["Buckets"]
print(buckets)

# Delete buckets
response = s3.delete_bucket(Bucket="gid-requests")

# Upload files
s3.upload_file(
    Filename="gid_requests_2019_01_01.csv",
    Bucket="gid-requests",
    Key="gid_request_2019_01_01.csv")

# List objects in a bucket
response = s3.list_objects(
    Bucket="gid-requests",
    MaxKeys=2,
    Prefix="gid_requests_2019_")
print(response)

# Get object metadata
response = s3.head_object(
    Bucket="gid-request",
    Key="gid_requests_2018_12_30.csv")
print(response)

# Download files
s3.download_file(
    Filename="gid_requests_download.csv",
    Bucket="gid-requests",
    Key="gid_Requests_2018_12_30.csv")

# Delete objects
s3.delete_object(
    Bucket="gid-requets",
    Key="gid_Requests_2018_12_30.csv")

# Upload File and set ACL to `public-read` or `private`
s3.upload_file(
    Filename="potholes.csv",
    Bucket="gid-requests",
    Key="potholes.csv")
s3.put_object_acl(
    Bucket="gid-requests",
    Key="potholes.csv",
    ACL="public-read")  # ACL="private"

# Upload file with `public-read` ACL
s3.upload_file(
    Bucket="gid-requests",
    Filename="potholes.csv",
    Key="potholes.csv",
    ExtraArgs={"ACL": "public-read"})

# Generate public object URL
url = "https://{}.s3.amazonaws.com/{}".format(
    "gid-requests", 
    "2019/potholes.csv")

# Read the URL into Pandas
df = pd.read_csv(url)

# Download private files
s3.download_file(
    Filename="potholes_local.csv",
    Bucket="gid-staging",
    Key="2019/potholes_private.csv")

# Read from dist
pd.read_csv("./potholes_local.csv")

# Use `.get_object()` to get the object
obj = s3.get_object(Bucket="gid-requests", Key="2019/potholes.csv")
print(obj)

# Read `StreamingBody` into Pandas
pd.read_csv(obj["Body"])

# Pre-signed URLS
# Upload a file
s3.upload_file(
    Filename="./potholes.csv",
    Key="potholes.csv",
    Bucket="gid-requests")
# Generate Presigned URL
share_url = s3.generate_presigned_url()
    ClientMethod="get_object",
    ExpiresIn=3600,
    Params={"Bucket": "gid-requests", "Key": "potholes.csv"}
)
# Open in Pandas
pd.read_csv(share_url)

# Load multiple files into one DataFrame
# Create list to hold our DataFrames
df_list = []
# Request the list of csv's from S3 with prefix; Get contents
response = s3.list_objects(
    Bucket="gid-requests",
    Prefix="2019/")
# Get response contents
request_files = response["Contents"]
# Iterate over each object
for file in request_files:
    obj = s3.get_object(Bucket="gid-requests", Key=file["Key"])
    # Read it as DataFrame
    obj_df = pd.read_csv(obj["Body"])
    # Append DataFrame to list
    df_list.append(obj_df)
# Concatenate all the DataFrames in the list
df = pd.concat(df_list)
# Preview the DataFrame
df.head()

# Convert DataFrame to html
df.to_html("table_agg.html", 
    render_links=True,
    columns["service_name", "request_count", "info_link"],
    border=0)

# Upload an HTML file to s3
s3.upload_file(
    Filename="./table_agg.html",
    Bucket="datacamp-website",
    Key="table.html",
    ExtraArgs={
    "ContentType": "text/html",
    "ACL": "public-read"
    })

# S3 object url template
# heeps://{bucket}.s3.amazonaws.com/{key}

# Upload an image file to S3
s3.upload_file(
    Filename="./plot_image.png",
    Bucket="datacamp-website",
    Key="plot_image.png",
    ExtraArgs={
    "ContentType": "image/png",
    "ACL": "public-read"
    })
# IANA media types
# Json: application/json
# PNG: image/png
# PDF: application/pdf
# CSV: text/csv

# Generate an index page
# List the gid-reports bucket objects starting with 2019/
r = s3.list_objects(Bucket="gid-reports", Prefix="2019/")
# Convert the response contents to DataFrames
objects_df = pd.DataFrame(r["Contents"])
# Create a column "Link" that contains website url + key
base_url = "http://datacamp-website.s3.amazonaws.com/"
objects_df["Link"] = base_url + objects_df["Key"]
# Write DataFrame to html
objects_df.to_html("report_listing.html",
    columns=["Link", "LastModified", "Size"],
    render_links=True)
# Upload an HTML file to s3
s3.upload_file(
    Filename="./repost_listing.html",
    Bucket="datacamp-website",
    Key="index.html",
    ExtraArgs={
    "ContentType": "text/html",
    "ACL": "public-read"
    })


# Read raw data files
# Create list to hold our DataFrames
df_list = []
# Request the list of csv's from S3 with prefix; Get contents
response = s3.list_objects(
    Bucket="gid-requests",
    Prefix="2019_jan")
# Get response contents
request_files = response["Contents"]
# Iterate over each object
for file in request_files:
    obj = s3.get_object(Bucket="gid-requests", Key=file["Key"])
    # Read it as DataFrame
    obj_df = pd.read_csv(obj["Body"])
    # Append DataFrame to list
    df_list.append(obj_df)
# Concatenate all the DataFrames in th list
df = pd.concat(df_list)
# Preview the DataFrame
df.head()
# ...
# Upload aggregated csv to s3
s3.upload_file(
    Filename="./jan_final_report.csv",
    Key="2019/jan/final_report.csv",
    Bucket="gid-reports",
    ExtraArgs={"ACL": "public-read"})
# Upload HTML table to s3
s3.upload_file(Filename="./jan_final_report.html",
    Key="2019/jan/final_report.html",
    Bucket="gid-reports",
    ExtraArgs={
    "ContentType": "text/html",
    "ACL": "public-read"
    })
# List the gid-reports bucket objects starting with 2019/
r = s3.list_objects(Bucket="gid-reports", Prefix="2019/")
# Convert the response contents to DataFrame
objects_df = pd.DataFrame(r["Contents"])
# Create a column "Link" that contains website url + key
base_url = "https://gid-reports.s3.amazonaws.com/"
objects_df["Link"] = base_url + objects_df["Key"]
# Write DataFrame to html
objects_df.to_html("report_listing.html",
    columns=["Link", "LastModified", "Size"],
    render_links=True)
# Upload the file to gid-reports bucket root
s3.upload_file(
    File="./report_listing.html",
    Key="index.html",
    Bucket="gid-reports",
    ExtraArgs={
    "ContentType": "text/html",
    "ACL": "public-read"
    })

# Create an SNS topic
sns = boto3.client("sns",
    region_name="us-east-1",
    aws_access_key_id=AWS_KEY_ID,
    aws_secret_access_key=aws_secret_access_key)

response = sns.create_topic(Name="city_alerts")
topic_arn = response["TopicArn"]

# List topics
response = sns.list_topics()

# Delete topics
sns.delete_topic(TopicArn="arn:aws:sns:us-east-1:320333787981:city_alerts")

# Create an SMS subscription
response = sns.subscribe(
    TopicArn="arn:aws:sns:us-east-1:320333787981:city_alerts",
    Protocol="SMS",
    Endpoint="+13125551123")

# Create an email subscription
response = sns.subscribe(
    TopicArn="arn:aws:sns:us-east-1:320333787981:city_alerts",
    Protocol="email",
    Endpoint="max@maksimize.com")

# List subscriptions by topics
sns.list_subsriptions_by_topic(
    TopicArn="arn:aws:sns:us-east-1:320333787981:city_alerts",
    )

# List subscritions
sns.list_subscriptions()["Subscriptions"]

# Delete subscriptions
sns.unsubscribe(
    SubscriptionArn="arn:aws:sns:us-east-1:320333787981:city_alerts",
    )

# Delete multiple subscriptions
response = sns.list_subscriptions_by_topic(
    TopicArn="arn:aws:sns:us-east-1:320333787981:city_alerts")
subs = response["Subscriptions"]
# Unsubscribe SMS subsctiptions
for sub in subs:
    if sub["Protocol"] == "sms":
        sns.unsubscribe(sns["SubscriptionArn"])

# Publish to a topic
response = sns.publish(
    TopicArn="arn:aws:sns:us-east-1:320333787981:city_alerts",
    Message="Body text of SMS or email",
    Subject="Subject Line for Email")

# Send custom messages
num_of_reports = 137
response = client.publish(
    TopicArn="arn:aws:sns:us-east-1:320333787981:city_alerts",
    Message="There are {} reports outstanding.".format(num_of_reports),
    Subject="Subject Line for Email"
    )

# Send a single SMS
response = sns.publish(
    PhoneNumber="+13121233211",
    Message="Body text of SMS or e-mail.")


# Topic set-up
# Initialize SNS client
sns = boto3.client("sns",
    region_name="us-east-1",
    aws_access_key_id=AWS_KEY_ID,
    aws_secret_access_key=AWS_SECRET)
# Create topics and store their ARNS
trash_arn = sns.create_topic(Name="trash_notifications")["TopicArn"]
streets_arn = sns.create_topic(Name="streets_notifications")["TopicArn"]
# Subscribe users to the topics
contacts = pd.read_csv("http://gid-staging.s3.amazonaws.com/contacts.csv")
# Create subscribe_user method
def subscribe_user(user_row):
    if user_row["Department"] = "trash":
        sns.subscribe(TopicArn=trash_arn, Protocol="sms", Endpoint=str(user_row["Phone"]))
        sns.subscribe(TopicArn=trash_arn, Propotal="email", Endpoint=user_row["Email"])
    else:
        sns.subscribe(TopicArn=streets_arn, Protocol="sms", Endpoint=str(user_row["Phone"]))
        sns.subscribe(TopicArn=streets_arn, Propotal="email", Endpoint=user_row["Email"])
# Apply the subscribe_user method to every row
contacts.apply(subsribe_user, axis=1)
# Get the aggregated numbers
# Load January's report into a DataFrame
df = pd.read_csv("http://gid-reports.s3.amazonaws.com/2019/feb/final_report.csv")
# Get the aggregated numbers
# Set the index so we can access counts by service name directly
df.set_index("service_name", inplace=True)
# Get the aggregated numbers
trash_violations_count=df.at["Illegal Dumpting", "count"]
streets_violations_count=df.at["Pothole", "count"]
# Send alerts
if trash_violations_count > 100:
    # Construct the message to send
    messga = "Trash violations count is now {}".format(trash_violations_count)
    # Send message
    sns.publish(TopicArn = trash_arn,
        Message=message,
        Subject="Trash Alert")
if streets_violations_count > 100:
    # Construct the message to send
    messga = "Street violations count is now {}".format(streets_violations_count)
    # Send message
    sns.publish(TopicArn = street,
        Message=message,
        Subject="Street Alert")

# Upload an image to s3
# Initialize s3 client
s3 = boto3.client("s3", 
    region_name="us-east-1",
    aws_access_key_id=AWS_KEY_ID,
    aws_secret_access_key=AWS_SECRET)
# Upload a file
s3.upload_file(
    Filename="report.jpg",
    Key="report.jpg",
    Bucket="datacamp-img")
# Object detection
# Initiate the client
rekog = boto3.client("rekoginition",
    region_name="us-east-1",
    aws_access_key_id=AWS_KEY_ID,
    aws_secret_access_key=AWS_SECRET)
# Image detection
response = rekog.detect_labels(
    Image={
    "S3Object": {
    "Bucket": "datacamp-img",
    "Name": "report.jpg"
    },
    MaxLabels=10, 
    MinConfidence=95 
    })
# Text detection
response = rekog.detect_text(
    Image={
    "S3Object":{
    "Bucket": "datacamp-img",
    "Name": "report.jpg"
    }
    })

# Translate text
# Initialize client
translate = boto3.client("translate", 
    region_name="us-east-1",
    aws_acess_key_id=AWS_KEY_ID,
    aws_secret_acess_key=AWS_SECRET)
# Translate text
response = translate.translate_text(
    Text="Hello, how are you?",
    SourceLanguageCode="auto",
    TargetLanguageCode="es")
translated_text = response["TranslatedText"]

# Detect language
# Initialize boto3 comprehend client
comprehend = boto3.client("comprehend",
    region_name="us-east-1",
    aws_acess_key_id=AWS_KEY_ID,
    aws_secret_access_key=AWS_SECRET)
# Detect dominant language
response = comprehend.detect_dominant_language(
    Text="Hay basura por todas partes a lo largo de la carretera.")

# Understanding sentiment
# Detent text sentiment
response = comprehend.detect_sentiment(
    Text="DataCamp students are amazing.",
    LanguageCode="en")
sentiment = response["Sentiment"]


# Initialize rekognition client
rekog = boto3.client("rekoginition",
    region_name="us-east-1",
    aws_access_key_id=AWS_KEY_ID,
    aws_secret_access_key=AWS_SECRET)
# Initialize comprehend client
comprehent = boto3.client("comprehend",
    region_name="us-east-1",
    aws_access_key_id=AWS_KEY_ID,
    aws_secret_access_key=AWS_SECRET)
# Initialize translate client
translate = boto3.client("translate",
    region_name="us-east-1",
    aws_access_key_id=AWS_KEY_ID,
    aws_secret_access_key=AWS_SECRET)
# Translate all descriptions to English
for index, row in df.iterrows():
    desc = df.loc[index, "public_description"]
    if desc != "":
        resp = translate_fake.translate_text(
            Text=desc,
            SourceLanguageCode="auto",
            TargetLanguageCode="en")
        df.loc[index, "public_description"] = resp["TranslatedText"]
# Detext text sentiment
for index, row in df.iterrows():
    desc = df.loc[index, "public_description"]
    if desc != "":
        resp = comprehend.detect_sentiment(
            Text=desc,
            LanguageCode="en")
        df.loc[index, "sentiment"] = resp["Sentiment"]
# Detect scotter in image
df["img_scotter"] = 0
for index, row in df.iterrows():
    image = df.loc[index, "image"]
    response = rekog.detect_labels(
        # Spevify the image as an S3Object
        Image={"S3Object": {
        "Bucket": "gid-images", 
        "Name": image
        }}
        )
    for label in response["Labels"]:
        if label["Name"] == "Scotter":
            df.loc[index, "img_scotter"] == 1
            break
# Select only rows where there was a scooter image and that have negative sentiment
pickups = df[((df.img_scooter == 1) & (df.sentiment == "NEGATIVE"))]
num_pickups = len(pickups)
