
---

# Flask API for Managing Items in MongoDB

This Flask API provides endpoints to add, retrieve, update, and delete items stored in MongoDB. It is designed to handle complex nested data structures related to sales organizations, distribution channels, locations, and items.

## Installation

To run this API locally, follow these steps:

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd <repository-name>
   ```

2. Install dependencies using pip:
   ```bash
   pip install Flask
   pip install Flask-PyMongo
 pip install pymongo
 pip install bson
   ```

3. Set up MongoDB:
   - Create a MongoDB Atlas account or use a local MongoDB instance.
   - Replace the `MONGO_URI` in `app.py` with your MongoDB URI.

4. Run the Flask application:
   ```bash
   python app.py
   ```

## API Endpoints

### Add Item

- **URL**: `/api/add_item`
- **Method**: `POST`
- **Request Body**:
  ```json
  {
    "distribution_channel_code": 1,
    "location_code": 2,
    "sales_organization_code": 3,
    "item_no": 4,
    "customer_no": 123,
    "customer_group": 456,
    "customer_hierarchy": 789,
    "Sales Price": 100.0,
    "condition_type": "Type A",
    "starting_date": "2024-01-01",
    "unit_of_measure_code": "kg",
    "minimum_quantity": 10,
    "priority": 1,
    "sales_price": 90.0,
    "ending_date": "2024-12-31",
    "lower_limit": 5,
    "upper_limit": 100,
    "currency": "USD",
    "offer_article": 123,
    "tax_percent": 10.0,
    "price_with_tax": 99.0
  }
  ```
- **Response**:
  - Success: `200 OK`
  - Error: `400 Bad Request`, `404 Not Found`

### Get Item

- **URL**: `/api/get_item`
- **Method**: `GET`
- **Query Parameters**:
  - `distribution_channel_code`: Numeric (required)
  - `location_code`: Numeric (required)
  - `sales_organization_code`: Numeric (required)
  - `item_no`: Numeric (required)
  - `customer_no`, `customer_group`, `customer_hierarchy`: Numeric (optional)
- **Response**:
  - Success: Returns item details
  - Error: `404 Not Found`

### Update Item

- **URL**: `/api/update_item`
- **Method**: `POST`
- **Request Body**:
  ```json
  {
    "distribution_channel_code": 1,
    "location_code": 2,
    "sales_organization_code": 3,
    "item_no": 4,
    "customer_no": 123,
    "customer_group": 456,
    "customer_hierarchy": 789,
    "sales_price": 95.0,
    "set": {
      "sales_price": 95.0,
      "priority": 2
    }
  }
  ```
- **Response**:
  - Success: `200 OK`
  - Error: `400 Bad Request`

### Delete Item

- **URL**: `/api/delete_item`
- **Method**: `DELETE`
- **Request Body**:
  ```json
  {
    "distribution_channel_code": 1,
    "location_code": 2,
    "sales_organization_code": 3,
    "item_no": 4
  }
  ```
- **Response**:
  - Success: `200 OK`
  - Error: `404 Not Found`

---
