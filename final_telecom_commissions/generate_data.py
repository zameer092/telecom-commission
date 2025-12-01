import csv  # Library to write CSV files—like Excel exporter
import random  # For fake random numbers
from datetime import datetime, timedelta  # For dates
from google.cloud import storage  # GCP library to upload (install: pip install google-cloud-storage)

# Step 1: Define products and their commission rates (varies by product)
products = {
    'prepaid': 0.05,
    'postpaid': 0.07,
    'dth': 0.06,
    'phones': 0.08,
    'netflix': 0.10,
    'prime': 0.09
}

# Step 2: Generate fake data—loop 100 times for rows
def generate_csv(filename):
    with open(filename, 'w', newline='') as file:  # Open file to write
        writer = csv.writer(file)  # CSV writer object
        writer.writerow(['agent_id', 'pos_id', 'product_type', 'sale_amount', 'sale_date', 'commission_rate'])  # Header row
        
        for i in range(100):  # 100 sales
            agent_id = f'AGENT_{random.randint(1, 50)}'  # Fake agent IDs 1-50
            pos_id = f'POS_{random.randint(100, 200)}'  # Fake POS IDs
            product = random.choice(list(products.keys()))  # Pick random product
            sale_amount = round(random.uniform(10, 1000), 2)  # Random sale $10-1000
            sale_date = (datetime.now() - timedelta(hours=random.randint(1, 24))).isoformat()  # Recent date
            rate = products[product]  # Get rate for product
            
            writer.writerow([agent_id, pos_id, product, sale_amount, sale_date, rate])  # Write row

    print(f"Generated {filename} with 100 rows!")  # Confirmation

# Step 3: Upload to GCS (why: simulates external source)
def upload_to_gcs(filename, bucket_name='telecom-commissions'):
    client = storage.Client()  # Connect to GCP (needs auth: gcloud auth login)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(f'raw_commissions/{filename}')  # Path in bucket
    blob.upload_from_filename(filename)  # Upload file
    print(f"Uploaded to gs://{bucket_name}/{blob.name}")

# Run it
if __name__ == "__main__":
    now = datetime.now().strftime("%Y%m%d_%H%M%S")  # Timestamp for unique file
    filename = f'sales_{now}.csv'
    generate_csv(filename)
    upload_to_gcs(filename)

