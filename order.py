from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt

#ket noi den csdl MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['eShop']
students_col= db['OrderCollection']

docs = list(students_col.find({}))
if not docs:
    print("Khong co du lieu")
    exit()

df = pd.DataFrame(docs)
print("Du lieu lay tu MongoDB: ")
print(df)

#Insert
orders = [
    {
        "orderid": 1,
        "products": [
            {"product_id": "quanau", "product_name": "quan au", "size": "XL", "price": 10, "quantity": 1},
            {"product_id": "somi", "product_name": "ao so mi", "size": "XL", "price": 10.5, "quantity": 2}
        ],
        "total_amount": 31,
        "delivery_address": "Hanoi"
    },
    {
        "orderid": 2,
        "products": [
            {"product_id": "somi", "product_name": "ao so mi", "size": "L", "price": 12, "quantity": 1}
        ],
        "total_amount": 12,
        "delivery_address": "HCM"
    }
]

result = db.OrderCollection.insert_many(orders)
print(f'\nInserted IDs: {result.inserted_ids}')

#Edit
db.OrderCollection.update_one(
    {"orderid": 1},
    {"$set": {"delivery_address": "Da Nang"}}
)

#Remove
db.OrderCollection.delete_one({"orderid": 2})

#Read
orders = db.OrderCollection.find()

print("\nNo\tProduct name\tPrice\tQuantity\tTotal")
for order in orders:
    no = 1
    for product in order['products']:
        total = product['price'] * product['quantity']
        print(f"{no}\t{product['product_name']}\t{product['price']}\t{product['quantity']}\t{total}")
        no += 1

#Calculate
orders = db.OrderCollection.find()
for order in orders:
    calculated_total = sum(p['price'] * p['quantity'] for p in order['products'])
    print(f"\nOrder ID: {order['orderid']} => Calculated Total: {calculated_total}")

#Count
count = db.OrderCollection.aggregate([
    {"$unwind": "$products"},
    {"$match": {"products.product_id": "somi"}},
    {"$count": "total_somi"}
])

for doc in count:
    print(f"\nTotal 'somi' products: {doc['total_somi']}")
