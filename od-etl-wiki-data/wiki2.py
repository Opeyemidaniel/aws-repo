import requests
import boto3
import csv
from io import StringIO

# Define the API endpoint and parameters
url = "https://meta.wikimedia.org/w/api.php"
params = {
    "action": "query",
    "meta": "siteinfo",
    "format": "json",
    "siprop": "statistics"
}

# Make a GET request to the API
response = requests.get(url, params=params)

# Extract the data from the response
data = response.json()["query"]["statistics"]

# Convert the data to CSV format
csv_data = StringIO()
csv_writer = csv.writer(csv_data)

# Write the header row
csv_writer.writerow(data.keys())

# Write the data rows
csv_writer.writerow(data.values())

# Write the CSV data to an S3 bucket
s3 = boto3.client('s3')
bucket_name = "0d-bucket-test"
object_key = "wiki_stats.csv"

s3.put_object(
    Body=csv_data.getvalue(),
    Bucket=bucket_name,
    Key=object_key
)
