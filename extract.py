## Generate fake employee data using Faker

from faker import Faker
import string
import random

# Number of employees to generate
num_employees = 100

# Faker instance
fake = Faker('en_US')

# List to hold employee records
employee_data = []

# Character set for password generation
pass_char = string.ascii_letters + string.digits + 'm'

for _ in range(num_employees):
    employee = {
        'first_name': fake.first_name(),
        'last_name': fake.last_name(),
        'job_title': fake.job().replace('"', "'"),
        'department': fake.job().replace('"', "'"),
        'email': fake.email(),
        'address': fake.address().replace('\n', ', ').replace('"', "'"),
        'phone_number': fake.phone_number(),
        'salary': fake.random_number(digits=5),
        'password': ''.join(random.choice(pass_char) for _ in range(8))
    }
    employee_data.append(employee)

print("Fake data is generated")

## Load data into a CSV file
import csv
from google.cloud import storage

file_name = 'employee_data.csv'

with open(file_name, mode='w', newline='', encoding='utf-8') as csv_file:
    fieldnames = employee_data[0].keys()
    writer = csv.DictWriter(
        csv_file,
        fieldnames=fieldnames,
        quoting=csv.QUOTE_ALL,
        quotechar='"',
        escapechar='\\'
    )
    writer.writeheader()
    writer.writerows(employee_data)

print(f'Fake data written to {file_name}')

# Upload CSV file to Google Cloud Storage
def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f'File {source_file_name} uploaded to {destination_blob_name} in bucket {bucket_name}.')

bucketname = 'big-data-dataset-bucket14082003'
source_file_name = file_name
destination_blob_name = 'employee_data.csv'
upload_to_gcs(bucketname, source_file_name, destination_blob_name)
