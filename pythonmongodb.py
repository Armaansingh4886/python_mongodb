import pandas as pd
import csv
import pymongo
from collections import defaultdict
from datetime import datetime
import math
import json
from bson.objectid import ObjectId

def create_nested_dict():
    return defaultdict(create_nested_dict)


def safe_float(value, default=0.0):
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

def safe_int(value, default=0):
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default
    

def process_csv_to_nested_dict(csv_file,mongo_url,db_name,collection_name,size):
    nested_dict = create_nested_dict()
    
    client = pymongo.MongoClient(mongo_url)
    db = client[db_name]
    collection = db[collection_name]
    count =0

    collection.delete_many({})
    
    new_document_id = ObjectId() 
    collection.insert_one({"_id": new_document_id})
    EXTRA=0
    for chunk in pd.read_csv(csv_file,chunksize=size):
        count = count +1
        print(f"{size}-{count}")
        data = chunk.to_dict(orient="records")
        for row in data:
            EXTRA=EXTRA+1
            print(EXTRA)
            distribution_channel = str(safe_int(row["Distribution Channel Code"]))
            location_code = str(safe_int(row["Location Code"]))
            sales_organization_code= str(safe_int(row["Sales Orgranization Code"]))
            item_no= str(safe_int(row["Item No_"]))
            customer= str(safe_int(row["Customer No_"]))            
            customer_group= str(safe_int(row["Customer Group"]))
            customer_hierarchy= str(safe_int(row["Customer Hierarchy"]))
            item_child = "nan"
            if(customer != "0"):
                item_child = f"customer.{customer}"
            elif (customer_group != "0"):
                item_child = f"customer_group.{customer_group}"
            elif (customer_hierarchy != "0"):
                item_child = f"customer_hierarchy.{customer_hierarchy}"

            condition = {
                "condition_type": row["Condition Type"],
                "starting_date": row["Starting Date"],
                "unit_of_measure_code": row["Unit of Measure Code"],
                "minimum_quantity": safe_float(row["Minimum Quantity"]),
                "priority": safe_int(row["Priority"]),
                "sales_organization_code": str(row["Sales Orgranization Code"]),
                "sales_price": safe_float(row["Sales Price"]),
                "ending_date": row["Ending Date"],
                "lower_limit": safe_float(row["Lower Limit"]),
                "upper_limit": safe_float(row["Upper Limit"]),
                "currency": row["Currency"],
                "offer_article": safe_int(row["Offer Artical"]),
                "tax_percent": safe_float(row["Tax Percent"]),
                "price_with_tax": safe_float(row["Price with TAX"])
            }
            filter_query = {
                "_id": new_document_id
                }
            
            update_query = {
                    "$push": {
                            f"sales_organization_code.{sales_organization_code}.distribution_channels.{distribution_channel}.locations.{location_code}.items.{item_no}.{item_child}.conditions": condition
                        }
                   
                }
            collection.update_one(filter_query, update_query)
            
        



csv_file = "./your_large_file.csv"
# csv_file = "./data.csv"

# mongo_uri="mongodb://localhost:27017"
mongo_url = "mongodb+srv://<username>:<password>@cluster0.swa3ygw.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
db_name = "dataitems2"
collection_name = "data"

# new_item_add = create_nested_dict()
new_item_add ={
    "asssd":1232,
    "asda":124312
}

process_csv_to_nested_dict(csv_file,mongo_url,db_name,collection_name,10000)


