import requests
import csv
import boto3

# Define the API endpoint and parameters
api_url = 'https://meta.wikimedia.org/w/api.php'
params = {
    'action': 'query',
    'format': 'json',
    'list': 'allpages',
    'aplimit': 100,  # Number of pages to retrieve per request
    'formatversion': 2  # Use JSON format version 2
}

# Send API requests to retrieve data in batches
all_pages = []
continue_param = None
while True:
    if continue_param:
        params['apcontinue'] = continue_param
    response = requests.get(api_url, params=params)
    data = response.json()
    all_pages.extend(data['query']['allpages'])
    if 'continue' in data:
        continue_param = data['continue']['apcontinue']
    else:
        break

# Save retrieved data as CSV with UTF-8 encoding
with open('wiki_pages.csv', 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = ['pageid', 'ns', 'title']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for page in all_pages:
        writer.writerow({'pageid': page['pageid'], 'ns': page['ns'], 'title': page['title']})


# Upload the CSV file to S3
s3 = boto3.client('s3')
s3.upload_file('wiki_pages.csv', '0d-bucket-test', 'wiki_pages.csv')

#print(f'Data has been saved to S3 bucket '0d-bucket-test' as 'wiki_pages.csv'')
