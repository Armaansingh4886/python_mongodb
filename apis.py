from flask import Flask, request, jsonify
from flask_pymongo import PyMongo
from pymongo import MongoClient
from bson.objectid import ObjectId


app = Flask(__name__)


app.config["MONGO_URI"] =  "mongodb+srv://o7:pb12q4886@cluster0.swa3ygw.mongodb.net/dataitems2?retryWrites=true&w=majority&appName=Cluster0"
mongo = PyMongo(app)
db_name = "dataitems2"
collection_name = "data"

# Example function to get you started
def greet(name):
    return f"Hello, {name}!"

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
 

@app.route('/api/add_item', methods=['POST'])
def add_item():
    data = request.get_json()
    distribution_channel = str(safe_int(data.get("distribution_channel_code", 0)))
    location_code = str(safe_int(data.get("location_code", 0)))
    sales_organization_code = str(safe_int(data.get("sales_orgranization_code", 0)))
    item_no = str(safe_int(data.get("item_no", 0)))
    customer = str(safe_int(data.get("customer_no", 0)))
    customer_group = str(safe_int(data.get("customer_group", 0)))
    customer_hierarchy = str(safe_int(data.get("customer_hierarchy", 0)))

    if not sales_organization_code or not item_no or not data.get("Sales Price"):
        return jsonify({"error": "Missing required parameters"}), 400

    item_child = "nan"
    if customer != "0": 
        item_child = f"customer.{customer}"
    elif customer_group != "0": 
        item_child = f"customer_group.{customer_group}"
    elif customer_hierarchy != "0":
        item_child = f"customer_hierarchy.{customer_hierarchy}"

    condition = {
        "condition_type": data.get("condition_type", ""),
        "starting_date": data.get("starting_cate", ""),
        "unit_of_measure_code": data.get("unit_of_measure_code", ""),
        "minimum_quantity": safe_float(data.get("minimum_quantity", 0.0)),
        "priority": safe_int(data.get("priority", 0)),
        "sales_price": safe_float(data.get("sales_price", 0.0)),
        "ending_date": data.get("ending_date", ""),
        "lower_limit": safe_float(data.get("lowe_limit", 0.0)),
        "upper_limit": safe_float(data.get("upper_limit", 0.0)),
        "currency": data.get("currency", ""),
        "offer_article": safe_int(data.get("offer_artical", 0)),
        "tax_percent": safe_float(data.get("tax_percent", 0.0)),
        "price_with_tax": safe_float(data.get("price_with_tax", 0.0))
    }

    filter_query = {
                "_id": ObjectId("66964e961dd4adaa6dcde374")
                }
            
    update_query = {
                    "$push": {
                            f"sales_organization_code.{sales_organization_code}.distribution_channels.{distribution_channel}.locations.{location_code}.items.{item_no}.{item_child}.conditions": condition
                        }
                   
                }
    result = mongo.db.data.update_one(filter_query, update_query)
    print(result)
    if result.matched_count > 0:
        return jsonify({"success": True, "message": "Item added successfully"}), 200
    else:
        return jsonify({"error": "Unable to add item"}), 404
    


@app.route('/api/get_item', methods=['GET'])
def get_item():
    data = request.get_json()
    distribution_channel = str(safe_int(data.get("distribution_channel_code", 0)))
    location_code = str(safe_int(data.get("location_code", 0)))
    sales_organization_code = str(safe_int(data.get("sales_orgranization_code", 0)))
    item_no = str(safe_int(data.get("item_no", 0)))
    customer = str(safe_int(data.get("customer_no", 0)))
    customer_group = str(safe_int(data.get("customer_group", 0)))
    customer_hierarchy = str(safe_int(data.get("customer_hierarchy", 0)))

    if not sales_organization_code or not item_no :
        return jsonify({"error": "Missing required parameters"}), 400

    item_child = "nan"
    if customer != "0": 
        item_child = f"customer.{customer}"
    elif customer_group != "0": 
        item_child = f"customer_group.{customer_group}"
    elif customer_hierarchy != "0":
        item_child = f"customer_hierarchy.{customer_hierarchy}"
    
    path = f"sales_organization_code.{sales_organization_code}.distribution_channels.{distribution_channel}.locations.{location_code}.items.{item_no}.{item_child}"
    query = { path: { '$exists': True } }
    projection = {f'{path}.conditions': 1, '_id': 0}
    item = mongo.db.data.find_one(query,projection)
    print(item != None)
    if (item != None):
        fetched_item = item['sales_organization_code'][sales_organization_code]['distribution_channels'][distribution_channel]['locations'][location_code]['items'][item_no]

        print("here")
        if item_child == 'nan':
            return jsonify({"message":fetched_item['nan']['conditions']})
        else:
            item_childs = item_child.split(".")
            return jsonify({'message':fetched_item[item_childs[0]][item_childs[1]]['conditions']})
    else:
        return jsonify({"error": "Unable to find item"}), 404


@app.route('/api/update_item', methods=['POST'])
def update_item():
    data = request.get_json()
    distribution_channel = str(safe_int(data.get("distribution_channel_code", 0)))
    location_code = str(safe_int(data.get("location_code", 0)))
    sales_organization_code = str(safe_int(data.get("sales_orgranization_code", 0)))
    item_no = str(safe_int(data.get("item_no", 0)))
    customer = str(safe_int(data.get("customer_no", 0)))
    customer_group = str(safe_int(data.get("customer_group", 0)))
    customer_hierarchy = str(safe_int(data.get("customer_hierarchy", 0)))

    if not sales_organization_code or not item_no or not data.get("sales_price"):
        return jsonify({"error": "Missing required parameters"}), 400

    item_child = "nan"
    if customer != "0": 
        item_child = f"customer.{customer}"
    elif customer_group != "0": 
        item_child = f"customer_group.{customer_group}"
    elif customer_hierarchy != "0":
        item_child = f"customer_hierarchy.{customer_hierarchy}"
    condition_keys = ["condition_type","currency","ending_date", "lower_limit", "minimum_quantity","offer_article","price_with_tax",
      "priority",
      "sales_organization_code",
      "sales_price",
      "starting_date",
      "tax_percent",
      "unit_of_measure_code",
      "upper_limit"]
    condition_obj={}
    array_filter_obj={}
    for key in condition_keys:
        value = data.get(key)
        if(value):
            condition_obj[key]=value
            array_filter_obj[f"elem.{key}"]=value
    print(data.get("set"))
    for key in data.get("set"):
        print(key)

    filter = {
            f"sales_organization_code.{sales_organization_code}.distribution_channels.{distribution_channel}.locations.{location_code}.items.{item_no}.{item_child}.conditions" :{
                '$elemMatch': condition_obj
            }
    }
    path = f"sales_organization_code.{sales_organization_code}.distribution_channels.{distribution_channel}.locations.{location_code}.items.{item_no}.{item_child}.conditions.$[elem]"
    setting_values = {}
    setter = data.get("set")
    for key in setter:
        setting_values[f"{path}.{key}"]=setter[key]
    update = {
        '$set': setting_values
    }

    array_filters = [array_filter_obj]
    
    item = mongo.db.data.update_one(filter,update,array_filters=array_filters)
    print(item)
    if (item != None):
        return jsonify({"message":"updated successfuly"})

    else:
        return jsonify({"error": "error while updating item"}), 400
    

@app.route('/api/delete_item', methods=['DELETE'])
def delete_item():
    data = request.get_json()
    item_no = str(safe_int(data.get("item_no", 0)))
    distribution_channel = str(safe_int(data.get("distribution_channel_code", 0)))
    location_code = str(safe_int(data.get("location_code", 0)))
    sales_organization_code = str(safe_int(data.get("sales_orgranization_code", 0)))
    

    filter = {
        f'sales_organization_code.{sales_organization_code}.distribution_channels.{distribution_channel}.locations.{location_code}.items.{item_no}': {'$exists': True}
    }
      
    update_query = {
        '$unset': {
            f'sales_organization_code.{sales_organization_code}.distribution_channels.{distribution_channel}.locations.{location_code}.items.{item_no}': ""
        }
    }
    result = mongo.db.data.update_many(filter, update_query)
    # result = mongo.db.data.find_one(filter)
    print("result",result)
    if result.matched_count > 0:
        return jsonify({"success": True, "message": "Item deleted successfully"}), 200
    else:
        return jsonify({"error": "Unable to delete item"}), 404


if __name__ == '__main__':
    app.run(debug=True)


