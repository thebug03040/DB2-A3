import csv
import os

def mapper_regions(filename):
    mapped=[]

    with open(filename, newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file) # Reads CSV rows as dictionaries

        for row in reader:
            price = float(row["price"])
            quantity = int(row["quantity"])
            sale_amount = price * quantity

            # Emit a key-value pair
            mapped.append((row["region"], sale_amount))
    
    return mapped


def shuffle(mapped):
    grouped = {}

    for key, value in mapped:
        if key not in grouped:
            grouped[key] = [] # Create a new list if key not seen before
        grouped[key].append(value) # Append value to the list for this key
    
    return grouped


def reduce_sum(grouped):
    reduced = {}

    for key in grouped:
        total = 0
        for value in grouped[key]:
            total += value # Sum all values for this key
        reduced[key] = total # Store the total sum in the reduced dictionary
    
    return reduced


def total_sales_per_region():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_dir, "data", "transactions.csv")

    mapped = mapper_regions(file_path)
    grouped = shuffle(mapped)
    reduced = reduce_sum(grouped)

    results = sorted(reduced.items())

    #Print to console
    print("\nTotal Sales Per Region:")
    for region, total in results:
        print(f"{region}, {total:.2f}")

    # Write results to CSV
    with open("taskA_output.csv", "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["region", "total_sales"])
        for row in results:
            writer.writerow([row[0], f"{row[1]:.2f}"])


# Reads CSV and emits (product_id, revenue) for each transaction
def mapper_products(filename):
    mapped = []

    with open(filename, newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file) # Reads CSV rows as dictionaries

        for row in reader:
            price = float(row["price"])
            quantity = int(row["quantity"])
            revenue = price * quantity

            # Emit a key-value pair
            mapped.append((row["product_id"], revenue))

    return mapped   


def top_products_by_revenue():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_dir, "data", "transactions.csv")

    mapped = mapper_products(file_path) # Map Phase
    grouped = shuffle(mapped)                         # Shuffle Phase
    reduced = reduce_sum(grouped)                     # Reduce Phase

    # Sort by revenue descending, take top 5
    results = sorted(reduced.items(), key=lambda x: x[1], reverse=True)[:5]

    #Print to console
    print("\nTop 5 Products by Revenue:")
    for product, revenue in results:
        print(f"{product}, {revenue:.2f}")

    # Write results to CSV
    with open("taskB_output.csv", "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["product_id", "revenue"])
        for row in results:
            writer.writerow([row[0], f"{row[1]:.2f}"])


def main():
    total_sales_per_region() #Task A
    top_products_by_revenue() #Task B


main()