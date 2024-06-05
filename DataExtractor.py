import pandas as pd
import json


class DataExtractor:
    def __init__(self, csv_file_path, expired_ids_file_path):
        self.csv_file_path = csv_file_path
        self.expired_ids_file_path = expired_ids_file_path
        self.data = None
        self.expired_ids = []

    def load_csv_file(self):
        try:
            self.data = pd.read_csv(self.csv_file_path)
            print("Data loaded successfully.")
        except Exception as e:
            print(f"An error occurred: {e}")

    def save_csv_file(self, file_path):
        try:
            self.data.to_csv(file_path, index=False)
            print(f"Data saved successfully to {file_path}.")
        except Exception as e:
            print(f"An error occurred while saving the file: {e}")

    def load_expired_ids(self):
        try:
            with open(self.expired_ids_file_path, 'r') as file:
                self.expired_ids = file.read().replace(' ', '').split(',')
                print("Expired IDs loaded successfully.")
        except Exception as e:
            print(f"An error occurred while loading expired IDs: {e}")

    def process_data(self):
        if self.data is not None:
            print(self.data.head())

        new_rows = []

        for index, row in self.data.iterrows():
            items = str(row['items'])
            id = str(row['id']).strip()
            created_on = str(row['created_on'])
            is_expired = id in self.expired_ids  # Check if the ID is in the expired IDs list
            print(f"Processing row {index}: id={id}, is_expired={is_expired}")
            try:
                items = json.loads(items.replace("'", "\""))
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON for row {index}: {e}")
                continue

            print(f"Row {index} items: {items}")
            for item in items:
                invoiceitem_id = item.get("item").get('id')
                invoiceitem_name = item.get("item").get('name')
                unit_price = item.get("item").get('unit_price')
                type = item.get("item").get('type')
                quantity = item.get('quantity')

                if isinstance(quantity, int):
                    total_price = unit_price * quantity

                    new_rows.append({
                        'invoice_id': id,
                        'created_on': created_on,
                        'invoiceitem_id': invoiceitem_id,
                        'invoiceitem_name': invoiceitem_name,
                        'type': type,
                        'unit_price': unit_price,
                        'quantity': quantity,
                        'total_price': total_price,
                        'is_expired': is_expired
                    })
                else:
                    print(f"Skipping row {index} due to non-integer quantity: {quantity}")

        self.data = pd.DataFrame(new_rows)

        print(self.data.head())

    def run(self, save_path):
        self.load_csv_file()
        self.load_expired_ids()
        self.process_data()
        self.save_csv_file(save_path)


