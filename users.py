import psycopg2
import json

def create_user(users):

	records = []
	headers = ['id', 'active', 'createddate', 'lastlogin', 'role', 'signupsource', 'state']
	
	for user in users:
		list = []
		try:
			list.append(str(user['_id']['$oid']))
		except KeyError:
			list.append(str(None))

		try:
			list.append(str(user['active']))
		except KeyError:
			list.append(str(None))

		try:
			list.append(str(user['createdDate']['$date']))
		except KeyError:
			list.append(str(None))

		try:
			list.append(str(user['lastLogin']['$date']))
		except KeyError:
			list.append(str(None))

		try:
			list.append(str(user['role']))
		except KeyError:
			list.append(str(None))

		try:
			list.append(str(user['signUpSource']))

		except KeyError:
			list.append(str(None))

		try:
			list.append(str(user['state']))
		except KeyError:
			list.append(str(None))

		records.append(list)

	print('Receipts Table Prepared')
	return records, headers
	


def create_table(records, headers, file_path):
	with open('users.csv', 'w', encoding='utf-8') as f:
		row_length = len(headers)
		f.write(format_list(headers, row_length, ',', '\"'))
		
		# Removing the duplicates lines
		rec = []
		for record in records:
			if record not in rec:
				rec.append(record)

		for r in rec:
			f.write(format_list(r, row_length, ',', '\"'))
				
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
	with open('users.json', 'r', encoding="utf-8") as f:	
		for line in f:
			json_line = json.loads(line)
			saved_data.append(json_line)
		user_tup = create_user(saved_data)
		user_records = user_tup[0]		
		user_headers = user_tup[1]
		create_table(user_records, user_headers, './users.csv')
		pg_load_table("./users.csv", 'users')

if __name__ == "__main__":
    main()






