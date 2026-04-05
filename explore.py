# This shows the actual location of the assignment file if you 
# are having trouble with your file locations during setup
import os
print(os.getcwd())

print("---------------------------------------------------------------------------------------")

# This loads MongoDB and Pandas
from pymongo import MongoClient
import pprint
import pandas as pd

# This connects to MongoDB
client = MongoClient("mongodb://localhost:27017/")
database = client["airbnb"]
collection1 = database["listings"]

# This prints how many entries are in the database
print(collection1.count_documents({}))

# This loads the AirBNB CSV file
file = pd.read_csv("data/listings.csv")

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
data = file.to_dict(orient="records")

# This inserts into MongoDB if the data isn't there already
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
results = collection1.find({"host_id": {"$in": host_ids}})
# This outputs to a text file using pprint()
with open("query3.txt", "w", encoding="utf-8") as p:
    for r in results:
        p.write(pprint.pformat(r) + "\n\n")

