import csv

csv_path = r"c:\folder\t_land_address_638860e3abde4a4cb5375943bf39b914.csv"

with open(csv_path, newline='', encoding='utf-8') as csvfile:
    reader = csv.reader(csvfile)
    for idx, row in enumerate(reader, 1):
        if len(row) == 0 or not row[0].strip():
            print(f"[Row {idx}] PNU is NULL or empty: {row}")