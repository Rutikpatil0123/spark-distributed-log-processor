import csv
import random
from datetime import datetime, timedelta

# Sample endpoints and status codes 
endpoints = ["/home", "/login", "/api/v1/users", "/checkout", "/dashboard", "/favicon.ico"]
status_codes = [200, 200, 200, 200, 404, 401, 500, 503]
ips = ["192.168.1.10", "10.0.0.45", "172.16.8.99", "192.168.1.55", "127.0.0.1"]

print("Generating sample log data...")
with open("data/raw_logs.csv", mode="w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["ip_address", "timestamp", "endpoint", "status_code"])
    
    # Generate 1000 rows for mock server logs
    base_time =  datetime.now() - timedelta(days=1)

    for i in range(1000):
        ip =  random.choice(ips)
        timestamp = (base_time + timedelta(seconds=random.randint(1, 86400))).strftime("%Y-%m-%d %H:%M:%S")
        endpoint = random.choice(endpoints)
        status_code = random.choice(status_codes)
        writer.writerow([ip, timestamp, endpoint, status_code])

    print("Successfully generated sample log data at 'data/raw_logs.csv")