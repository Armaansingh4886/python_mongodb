from flask import Flask, request, jsonify
from flask_pymongo import PyMongo
from pymongo import MongoClient
from bson.objectid import ObjectId
from pymongo.errors import PyMongoError

app = Flask(__name__)


# app.config["MONGO_URI"] =  "mongodb+srv://<username>:<password>@cluster0.swa3ygw.mongodb.net/dataitems2?retryWrites=true&w=majority&appName=Cluster0"
# app.config["MONGO_URI"]="mongodb://localhost:27017/dataitems2"
# mongo = PyMongo(app)
# db_name = "dataitems2"
# collection_name = "data"
client = MongoClient('mongodb://localhost:27017/')
db = client['dataitems2']

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
    try:
        data = request.get_json()
        distribution_channel = str(safe_int(data.get("distribution_channel_code", 0)))
        location_code = str(safe_int(data.get("location_code", 0)))
        sales_organization_code = str(safe_int(data.get("sales_organization_code", 0)))
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

        conditions = {
            "condition_type": data.get("condition_type", ""),
            "starting_date": data.get("starting_date", ""),
            "unit_of_measure_code": data.get("unit_of_measure_code", ""),
            "minimum_quantity": safe_float(data.get("minimum_quantity", 0.0)),
            "priority": safe_int(data.get("priority", 0)),
            "sales_price": safe_float(data.get("sales_price", 0.0)),
            "ending_date": data.get("ending_date", ""),
            "lower_limit": safe_float(data.get("lower_limit", 0.0)),
            "upper_limit": safe_float(data.get("upper_limit", 0.0)),
            "currency": data.get("currency", ""),
            "offer_article": safe_int(data.get("offer_article", 0)),
            "tax_percent": safe_float(data.get("tax_percent", 0.0)),
            "price_with_tax": safe_float(data.get("price_with_tax", 0.0))
        }

        document = {"_id":item_no}
       
        if "nan"==item_child:
            document.update({"conditions": [conditions]})
        else:
            item_child_parts = item_child.split('.')
            current_level = document
            for part in item_child_parts:
                if part not in current_level:
                    current_level[part] = {}
                current_level = current_level[part]
            if "conditions" not in current_level:
                current_level["conditions"] = []
            current_level["conditions"].append(conditions)
        collection = f"{sales_organization_code}.{distribution_channel}.{location_code}"
        result = db[collection].insert_one(document)
        print(result)
        if result.inserted_id:
            return jsonify({"success": True, "message": "Item added successfully"}), 200
        else:
            return jsonify({"error": "Unable to add item"}), 404
    except PyMongoError as e:
        return jsonify({'error': f'Unable to add item: {str(e)}'}), 500


@app.route('/api/get_item', methods=['GET'])
def get_item():
    try:
        data = request.get_json()
    
        distribution_channel = str(safe_int(data.get("distribution_channel_code", 0)))
        location_code = str(safe_int(data.get("location_code", 0)))
        sales_organization_code = str(safe_int(data.get("sales_organization_code", 0)))
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
    
        query = {"_id": item_no}
        projection = {f'{item_child}.conditions': 1, '_id': 0}
        collection = f"{sales_organization_code}.{distribution_channel}.{location_code}"
        item = db[collection].find_one(query,projection)
        # print(item )
        if (item != None):
            fetched_item = item
            print("here")
            if item_child == 'nan':
                return jsonify({"message":fetched_item['nan']['conditions']})
            else:
                item_childs = item_child.split(".")
                return jsonify({'message':fetched_item[item_childs[0]][item_childs[1]]['conditions']})
        else:
            return jsonify({"error": "No item found"}), 404
    except PyMongoError as e:
        return jsonify({'error': f'Unable to get item: {str(e)}'}), 500

@app.route('/api/update_item', methods=['PUT'])
def update_item():
    try:
        data = request.get_json()
        distribution_channel = str(safe_int(data.get("distribution_channel_code", 0)))
        location_code = str(safe_int(data.get("location_code", 0)))
        sales_organization_code = str(safe_int(data.get("sales_organization_code", 0)))
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
        condition_keys = ["condition_type","currency","ending_date", "lower_limit", "minimum_quantity","offer_article","price_with_tax",
        "priority",
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
                "_id":item_no,
                f"{item_child}.conditions":{"$elemMatch":condition_obj}
        }
        path = f"{item_child}.conditions.$[elem]"
        setting_values = {}
        setter = data.get("set")
        for key in setter:
            setting_values[f"{path}.{key}"]=setter[key]
        update = {
            '$set': setting_values
        }

        array_filters = [array_filter_obj]
        collection = f"{sales_organization_code}.{distribution_channel}.{location_code}"
        print("asd",condition_obj)
        item = db[collection].update_one(filter,update,array_filters=array_filters)
        print(item)
        if (item.modified_count >0):
            return jsonify({"message":"updated successfuly"})

        else:
            return jsonify({"error": "no such item to update"}), 400
    except PyMongoError as e:
        return jsonify({'error': f'Unable to update item: {str(e)}'}), 500
    

@app.route('/api/delete_item', methods=['DELETE'])
def delete_item():
    try:
        data = request.get_json()
        item_no = str(safe_int(data.get("item_no", 0)))
        distribution_channel = str(safe_int(data.get("distribution_channel_code", 0)))
        location_code = str(safe_int(data.get("location_code", 0)))
        sales_organization_code = str(safe_int(data.get("sales_organization_code", 0)))
        

        filter = {
                "_id":item_no
            }
        collection = f"{sales_organization_code}.{distribution_channel}.{location_code}"
        result = db[collection].delete_one(filter)
        # result = mongo.db.data.find_one(filter)
        print("result",result)
        if result.deleted_count > 0:
            return jsonify({"success": True, "message": "Item deleted successfully"}), 200
        else:
            return jsonify({"error": "No such item to delete"}), 404
    except PyMongoError as e:
        return jsonify({'error': f'Unable to delete item: {str(e)}'}), 500


if __name__ == '__main__':
    app.run(debug=True)


