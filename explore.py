# This shows the actual location of the assignment file if you 
# are having trouble with your file locations during setup
# https://www.w3schools.com/python/ref_os_getcwd.asp
import os
print(os.getcwd())

print("---------------------------------------------------------------------------------------")

# This loads MongoDB and Pandas
# https://www.mongodb.com/resources/languages/python
from pymongo import MongoClient
import pprint
import pandas as pd

# This connects to MongoDB
# https://www.w3schools.com/python/python_mongodb_create_db.asp
client = MongoClient("mongodb://localhost:27017/")
database = client["airbnb"]
collection1 = database["listings"]

# This prints how many entries are in the database
print(collection1.count_documents({}))

# This loads the AirBNB CSV file
# file = pd.read_csv("DB2-A3/data/listings.csv")

# This ensures filepath is correct no matter who is working on file
# https://www.geeksforgeeks.org/python/python-os-path-join-method/
base_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(base_dir, "data", "listings.csv")

file = pd.read_csv(file_path)

print("---------------------------------------------------------------------------------------")

# This shows the first five row
print(file.head())

print("---------------------------------------------------------------------------------------")

# This shows the names of the columns
print(file.columns)

print("---------------------------------------------------------------------------------------")

# This shows general info
print(file.info())

print("---------------------------------------------------------------------------------------")

# This converts the file to a dictionary
# https://www.geeksforgeeks.org/python/pandas-dataframe-to_dict/
data = file.to_dict(orient="records")

# This inserts into MongoDB if the data isn't there already
# https://www.mongodb.com/docs/manual/core/transactions-operations/
if collection1.count_documents({}) == 0:
    collection1.insert_many(data)
    print("Data has been inserted!")
else:
    print("Data already exists, skipping")

print("---------------------------------------------------------------------------------------")

# This retrieves 3 documents from the collection with the find() function and limits() results to 3
with open("query1.txt", "w", encoding="utf-8") as p:
    for document in collection1.find().limit(3):
        p.write(pprint.pformat(document) + "\n\n")

print("---------------------------------------------------------------------------------------")

# This finds 10 documents in Pretty format using pprint() and outputs to a text file
with open("query2.txt", "w", encoding="utf-8") as p:
    for document in collection1.find().limit(10):
        p.write(pprint.pformat(document))
        p.write("\n\n")

print("---------------------------------------------------------------------------------------")

# This converts the two find() results into a Python list so it can be stored in memory
superhosts = list(collection1.find({"host_is_superhost": "t"}).limit(2))
# This loops through superhosts and returns host_id
host_ids = [host["host_id"] for host in superhosts]
# This uses a MongoDB operator "$in" to find all results where host_id is a superhost
# https://www.mongodb.com/docs/manual/reference/operator/query/in/
results = collection1.find({"host_id": {"$in": host_ids}})
# This outputs to a text file using pprint()
with open("query3.txt", "w", encoding="utf-8") as p:
    for r in results:
        p.write(pprint.pformat(r) + "\n\n")
print("---------------------------------------------------------------------------------------")

# Find all unique host_name values
# https://docs.mongodb.com/manual/reference/method/db.collection.distinct/

unique_hosts = collection1.distinct("host_name")
print(f"Total unique host names: {len(unique_hosts)}\n")
for host in unique_hosts:
    print(host)

print("---------------------------------------------------------------------------------------")

# Find all places with more than 2 beds in Chamartín neighbourhood, ordered by review_scores_rating descending
# https://docs.mongodb.com/manual/reference/method/db.collection.find/

neighbourhood = "Chamartín"

results = collection1.find(
    {
        "bedrooms": {"$gt": 2},
        "neighbourhood_group_cleansed": neighbourhood,
        "review_scores_rating": {"$ne": None}
    }
).sort("review_scores_rating", -1)

print(f"\nPlaces with more than 2 beds in {neighbourhood}, ordered by review score \n")
for place in results:
    review_score = place.get('review_scores_rating')
    
    # Skip if review_score is None or can't be converted to a valid number
    if review_score is None:
        continue
    
    try:
        review_score = float(review_score)
        # Skip if NaN
        if review_score != review_score:
            continue
    except:
        continue
    
    print(f" Name: {place.get('name')}")
    print(f"    Review Score: {review_score}")
    print(f"    Bedrooms: {place.get('bedrooms')}")
    print(f"    Host: {place.get('host_name')}")
    print(f"    Price: {place.get('price')}")

print("---------------------------------------------------------------------------------------")

# This shows the number of listings per host
# https://docs.mongodb.com/manual/aggregation/

print("\nNumber of listings per host:\n")
listings_per_host = collection1.aggregate([
    {
        "$group": {
            "_id": "$host_name",
            "listing_count": {"$sum": 1}
        }
    },
    {
        "$sort": {"listing_count": -1}
    }
])

for result in listings_per_host:
    host_name = result.get("_id", "Unknown")
    count = result.get("listing_count")
    print(f"{host_name}: {count} listings")


print("---------------------------------------------------------------------------------------")

# This shows the neighbourhoods with the average review scores of 95 or higher.
# https://docs.mongodb.com/manual/aggregation/

print("\n\033[1m\033[4m" + "Neighbourhoods with an average review score of 95 or higher:\033[0m\n")

avg_review_scores = collection1.aggregate([
    # Filter out listings with missing or null review scores
    {
        "$match": {
            "review_scores_rating": {"$ne": None}
        }
    },
    # Convert review_scores_rating to a number (CSV imports are strings)
    {
        "$addFields": {
            "rating_num": {
                "$cond": [
                    {"$eq": [{"$type": "$review_scores_rating"}, "double"]},
                    "$review_scores_rating",
                    {
                        "$convert": {
                            "input": "$review_scores_rating",
                            "to": "double",
                            "onError": 0,
                            "onNull": 0
                        }
                    }
                ]
            }
        }
    },
   # Filter out records where conversion resulted in 0
    {
        "$match": {
            "rating_num": {"$gt": 0}
        }
    },
    # Group by neighbourhood and compute average
    {
        "$group": {
            "_id": "$neighbourhood_group_cleansed",
            "avg_rating": {"$avg": "$rating_num"}
        }
    },
    # Sort highest → lowest
    {
        "$sort": {"avg_rating": -1}
    }
])

for result in avg_review_scores:
    neighbourhood = result.get("_id", "Unknown")
    avg_rating = result.get("avg_rating")
    if avg_rating >= 95:
        print(f"\t{neighbourhood}: Average Rating = {avg_rating:.2f}")
