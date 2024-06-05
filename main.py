import DataExtractor
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    csv_file_path = 'invoices_new.pkl'
    expired_ids_file_path = 'expired_invoices.txt'
    save_path = 'flattened_invoices.csv'

    data_extractor = DataExtractor(csv_file_path, expired_ids_file_path)
    data_extractor.run(save_path)

