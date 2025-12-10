# gen_medium.py
import csv, random
from faker import Faker
fake = Faker()

with open('sample_medium.csv','w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['id','price','name','email','city'])
    for i in range(1,10001):             # 10k rows
        writer.writerow([i, round(random.uniform(0,1000),3), fake.first_name(), fake.email(), fake.city()])
