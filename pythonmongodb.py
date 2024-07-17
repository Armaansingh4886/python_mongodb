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
    
    # client = pymongo.MongoClient(mongo_url)
    # db = client[db_name]
    # collection = db[collection_name]
    count =0

    # collection.delete_many({})
    
    # new_document_id = ObjectId() 
    # collection.insert_one({"_id": new_document_id})
    EXTRA=0
    for chunk in pd.read_csv(csv_file,chunksize=size):
        count = count +1
        print(f"{size * count}")
        data = chunk.to_dict(orient="records")
        for row in data:
            # EXTRA=EXTRA+1
            # print(EXTRA)
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
                "offer_article": safe_int(row["Offer Artical"]),
                "tax_percent": safe_float(row["Tax Percent"]),
                "price_with_tax": safe_float(row["Price with TAX"])
            }
            # filter_query = {
            #     "_id": new_document_id
            #     }
            
            # update_query = {
            #         "$push": {
            #                 f"sales_organization_code.{sales_organization_code}.distribution_channels.{distribution_channel}.locations.{location_code}.items.{item_no}.{item_child}.conditions": condition
            #             }
                   
            #     }

            if item_child == "nan":
                if "conditions" not in nested_dict["sales_organization_code"][sales_organization_code]["distribution_channel"][distribution_channel]["location_code"][location_code]["item"][item_no][item_child]:
                    nested_dict["sales_organization_code"][sales_organization_code]["distribution_channel"][distribution_channel]["location_code"][location_code]["item"][item_no][item_child]["conditions"] = []
                nested_dict["sales_organization_code"][sales_organization_code]["distribution_channel"][distribution_channel]["location_code"][location_code]["item"][item_no][item_child]["conditions"].append(condition)
            else:
                item_child_parts = item_child.split(".")
                if "conditions" not in nested_dict["sales_organization_code"][sales_organization_code]["distribution_channel"][distribution_channel]["location_code"][location_code]["item"][item_no][item_child_parts[0]][item_child_parts[1]]:
                    nested_dict["sales_organization_code"][sales_organization_code]["distribution_channel"][distribution_channel]["location_code"][location_code]["item"][item_no][item_child_parts[0]][item_child_parts[1]]["conditions"] = []
                nested_dict["sales_organization_code"][sales_organization_code]["distribution_channel"][distribution_channel]["location_code"][location_code]["item"][item_no][item_child_parts[0]][item_child_parts[1]]["conditions"].append(condition)

            # collection.update_one(filter_query, update_query)
        # print((nested_dict))
        return nested_dict
    return nested_dict           
        
def upload(data):
    client = pymongo.MongoClient(mongo_url)
    db = client[db_name]
    collections = db.list_collection_names()

    for collection_name in collections:
        db.drop_collection(collection_name)
    
    # new_document_id = ObjectId() 
    # collection.insert_one({"_id": new_document_id})

    for key, value in data.items():
        print(f"Key: {key}")
        print(f"Value: {json.dumps(value)}\n")
        key_list = key.split(".")
        if key_list[1] == '0' and key_list[2] =='0':
                document = {"_id": key}
                document.update(value)
                db["data00"].insert_one(document)
        elif key_list[1] == '0' and key_list[2] =='1':
            document = {"_id": key}
            document.update(value)
            db["data01"].insert_one(document)
        elif key_list[1] == '1' and key_list[2] =='0':
            document = {"_id": key}
            document.update(value)
            db["data10"].insert_one(document)
        else:
            document =  {"_id": key}
            document.update(value)
        
            db["data"].insert_one(document)
        # filter_query = {
        #         "_id": new_document_id
        #         }
            
        # update_query = {
        #            "$set": {
        #                     key: value
        #                 }
        #         }
        # collection.update_one(filter_query, update_query)

def save(data):
    filename = "./data.json"
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)

    print(f'JSON data has been written to {filename}')



def create_key(path):
    return ".".join([
        # f"{path[0]}.{path[1]}",
        # f"{path[2]}.{path[3]}",
        # f"{path[4]}.{path[5]}",
        # f"{path[6]}"
        f"{path[1]}.{path[3]}.{path[5]}"
    ])

def traverse_and_transform(d, path=None, result=None):
    if path is None:
        path = []
    if result is None:
        result = {}
    # if result10 is None:
    #     result10 ={}

    for key, value in d.items():
        current_path = path + [key]
        # if key =='0':
        #     next_key =next(iter(value))
        #     if (next_key =="item"):
        #         obj_key = create_key(current_path)
        #         result10[obj_key]= copy.deepcopy(value[next_key])
        #     if next_key == "location_code":
        #         obj_key = create_key(current_path)
        #         result10[obj_key]= copy.deepcopy(value[next_key])
        if key == 'item':
            obj_key = create_key(current_path)
            # Deepcopy to ensure no reference issues
            result[obj_key] = copy.deepcopy(value)
        elif isinstance(value, defaultdict):
            traverse_and_transform(value, current_path, result)
    
    return result

csv_file = "./your_large_file.csv"
# csv_file = "./data.csv"

mongo_url="mongodb://localhost:27017"
# mongo_url = "mongodb+srv://o7:pb12q4886@cluster0.swa3ygw.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
db_name = "dataitems2"
collection_name = "data"

# new_item_add = create_nested_dict()
new_item_add ={
    "asssd":1232,
    "asda":124312
}

data = process_csv_to_nested_dict(csv_file,mongo_url,db_name,collection_name,500000)
# save(data)
# print(data)
# print(json.dumps(data["sales_orgamization_code"]["2100"]["distribution_channel"]["3"]["location_code"]["2120"]))
transformed_result = traverse_and_transform(data)
print(transformed_result)
upload(transformed_result)
# upload10(transformed_result[1])

# for key, value in transformed_result.items():
#     print(f"Key: {key}")
#     print(f"Value: {json.dumps(value)}\n")
