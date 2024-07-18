import pandas as pd
import csv
import pymongo
from collections import defaultdict
from datetime import datetime
import math
import json
from bson.objectid import ObjectId
import copy


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
    
    
    count =0

    for chunk in pd.read_csv(csv_file,chunksize=size):
        count = count +1
        print(f"{size * count}")
        data = chunk.to_dict(orient="records")
        for row in data:
            
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
                "sales_price": safe_float(row["Sales Price"]),
                "ending_date": row["Ending Date"],
                "lower_limit": safe_float(row["Lower Limit"]),
                "upper_limit": safe_float(row["Upper Limit"]),
                "currency": row["Currency"],
                "offer_article": safe_int(row["Offer Article"]),
                "tax_percent": safe_float(row["Tax Percent"]),
                "price_with_tax": safe_float(row["Price with TAX"])
            }
         

            if item_child == "nan":
                if "conditions" not in nested_dict["sales_organization_code"][sales_organization_code]["distribution_channel"][distribution_channel]["location_code"][location_code]["item"][item_no][item_child]:
                    nested_dict["sales_organization_code"][sales_organization_code]["distribution_channel"][distribution_channel]["location_code"][location_code]["item"][item_no][item_child]["conditions"] = []
                nested_dict["sales_organization_code"][sales_organization_code]["distribution_channel"][distribution_channel]["location_code"][location_code]["item"][item_no][item_child]["conditions"].append(condition)
            else:
                item_child_parts = item_child.split(".")
                if "conditions" not in nested_dict["sales_organization_code"][sales_organization_code]["distribution_channel"][distribution_channel]["location_code"][location_code]["item"][item_no][item_child_parts[0]][item_child_parts[1]]:
                    nested_dict["sales_organization_code"][sales_organization_code]["distribution_channel"][distribution_channel]["location_code"][location_code]["item"][item_no][item_child_parts[0]][item_child_parts[1]]["conditions"] = []
                nested_dict["sales_organization_code"][sales_organization_code]["distribution_channel"][distribution_channel]["location_code"][location_code]["item"][item_no][item_child_parts[0]][item_child_parts[1]]["conditions"].append(condition)

            
        # return nested_dict
    return nested_dict           
        
def upload(data):
    client = pymongo.MongoClient(mongo_url)
    db = client[db_name]
    collections = db.list_collection_names()

    for collection_name in collections:
        db.drop_collection(collection_name)
    
   
    for key, value in data.items():
        print(f"Key: {key}")
        print(f"Value: {json.dumps(value)}\n")
       
        for itemno,itemdata in value.items():
                    document = {"_id": itemno}
                    document.update(itemdata)
                    db[key].insert_one(document)
      

def save(data):
    filename = "./data.json"
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)

    print(f'JSON data has been written to {filename}')



def create_key(path):
    return ".".join([
      
        f"{path[1]}.{path[3]}.{path[5]}"
    ])

def traverse_and_transform(d, path=None, result=None):
    if path is None:
        path = []
    if result is None:
        result = {}
 
    for key, value in d.items():
        current_path = path + [key]
        
        if key == 'item':
            obj_key = create_key(current_path)
            result[obj_key] = copy.deepcopy(value)
        elif isinstance(value, defaultdict):
            traverse_and_transform(value, current_path, result)
    
    return result

csv_file = "./your_large_file.csv"
# csv_file = "./data.csv"

mongo_url="mongodb://localhost:27017"
db_name = "dataitems2"
collection_name = "data"

# new_item_add = create_nested_dict()
new_item_add ={
    "asssd":1232,
    "asda":124312
}

data = process_csv_to_nested_dict(csv_file,mongo_url,db_name,collection_name,10000)
transformed_result = traverse_and_transform(data)
print(transformed_result)
upload(transformed_result)
