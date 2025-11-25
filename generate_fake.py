import pandas as pd
import random
import os

# 5 fake agents
agents = ["A001","A002","A003","A004","A005"]
names  = ["Rahul Delhi","Priya Mumbai","Vikram Bangalore","Anita Hyderabad","Sameer Chennai"]

data = []
for i in range(150):
    product = random.choice(["Prepaid Recharge", "New Postpaid", "DTH Upgrade"])
    amount = 0
    if product == "Prepaid Recharge":
        amount = random.choice([199,299,399,499,599,799])
    elif product == "New Postpaid":
        amount = random.choice([599,799,999])
    else:
        amount = random.choice([899,1299,1599])
        
    data.append({
        "transaction_id": f"T20251125{i:06d}",
        "agent_id": random.choice(agents),
        "agent_name": names[agents.index(random.choice(agents))],
        "product_type": product,
        "amount": amount,
        "transaction_date": "2025-11-25"
    })

df = pd.DataFrame(data)
os.makedirs("2025/11/25", exist_ok=True)
df.to_csv("2025/11/25/transactions_20251125.csv", index=False)
print("Fake file created!")
