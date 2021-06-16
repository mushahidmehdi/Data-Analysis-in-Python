import psycopg2
import json

def create_user(brands):

	records = []
	headers = ['id', 'barcode', 'category', 'categorycode', 'cpg_id', 'cpg_ref', 'name', 'topbrand']
	
	for brand in brands:
		list = []
		try:
			list.append(str(brand['_id']['$oid']))
		except KeyError:
			list.append(str(None))

		try:
			list.append(str(brand['barcode']))
		except KeyError:
			list.append(str(None))

		try:
			list.append(str(brand['category']))
		except KeyError:
			list.append(str(None))

		try:
			list.append(str(brand['categoryCode']))
		except KeyError:
			list.append(str(None))

		try:
			list.append(str(brand['cpg']['$id']['$oid']))
		except KeyError:
			list.append(str(None))

		try:
			list.append(str(brand['cpg']['$ref']))

		except KeyError:
			list.append(str(None))

		try:
			list.append(str(brand['name']))
		except KeyError:
			list.append(str(None))

		try:
			list.append(str(brand['topBrand']))
		except KeyError:
			list.append(str(None))

		records.append(list)

	print('Receipts Table Prepared')
	return records, headers
	


def create_table(records, headers, file_path):
	with open('brands.csv', 'w', encoding='utf-8') as f:
		row_length = len(headers)
		f.write(format_list(headers, row_length, ',', '\"'))

		for record in records:
			f.write(format_list(record, row_length, ',', '\"'))
				
	print('CSV file successfully created: {}'.format(file_path))


def format_list(list, length, delimiter, quote):
	counter = 1
	strings = ''

	for record in list:
		if counter == length:
			strings += quote + record + quote + '\n'
		
		else:
			strings += quote + record + quote + delimiter
		counter += 1
	return strings

	

	# Load data into postgres database
def pg_load_table(file_path, table_name):
	try:
		connec = psycopg2.connect(dbname="fetchreward", user='root', host="localhost", password="21901085")
		print("Connecting to Database")
		cur = connec.cursor()
		saved_data = open(file_path, "r", encoding='utf-8')
		cur.execute("Truncate {} Cascade;".format(table_name))
		print("Truncated {}".format(table_name))
		cur.copy_expert("copy {} from STDIN CSV HEADER".format(table_name), saved_data)
		cur.execute("commit;")
		print("Loaded data into {}".format(table_name))
		connec.close()
		print("DB connection closed.")
	except Exception as e:
		 print("Error: {}".format(str(e)))

def main():

	# Parsing json data from local memory and converting into python dict
	# Iter over all the python dict and add into a list
	saved_data = []
	with open('brands.json', 'r', encoding="utf-8") as f:	
		for line in f:
			json_line = json.loads(line)
			saved_data.append(json_line)
		brand_tup = create_user(saved_data)
		brand_records = brand_tup[0]		
		brand_headers = brand_tup[1]
		create_table(brand_records, brand_headers, './brands.csv')
		pg_load_table("./brands.csv", 'brands')

	
if __name__ == "__main__":
    main()






