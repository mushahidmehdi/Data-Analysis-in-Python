import psycopg2
import json

def create_receipts(receip_data):
	# Take the query and convert into a list along headers

	records = []

	headers = ['id','bonusPointsEarned','bonusPointsEarnedReason','createDate_date','dateScanned_date','finishedDate_date','modifyDate_date','pointsAwardedDate_date','pointsEarned','purchaseDate_date', 'purchasedItemCount','rewardsReceiptStatus','totalSpent','userId']

	for key in receip_data:

		list = []

		list.append(key["_id"]['$oid'])

		try:
			list.append(key['bonusPointsEarned'])
		except KeyError:
			list.append(None)
		
		try:
			list.append(key['bonusPointsEarnedReason'])
		except KeyError:
			list.append(None)

		try:
			list.append(key['createDate']['$date'])
		except KeyError:
			list.append(None)
		
		try:
			list.append(key['dateScanned']['$date'])
		except KeyError:
			list.append(None)

		try:
			list.append(key['finishedDate']['$date'])
		except KeyError:
			list.append(None)

		try:
			list.append(key['modifyDate']['$date'])
		except KeyError:
			list.append(None)

		try:
			list.append(key['pointsAwardedDate']['$date'])
		except KeyError:
			list.append(None)

		try:
			list.append(key['pointsEarned'])
		except KeyError:
			list.append(None)
		
		try:
			list.append(key['purchaseDate']['$date'])
		except KeyError:
			list.append(None)

		try:
			list.append(key['purchasedItemCount'])
		except KeyError:
			list.append(None)

		try:
			list.append(key['rewardsReceiptStatus'])
		except KeyError:
			list.append(None)

		try:
			list.append(key['totalSpent'])
		except KeyError:
			list.append(None)

		try:
			list.append(key['userId'])
		except KeyError:
			list.append(None)

		records.append(list)

	print('Receipts Table Prepared')

	return records, headers


def receipts_reward_list(receip_data):

	# Take the query and convert into a list along headers
	records = []
	
	headers = ['id	','barcode', 'description', 'finalPrice', 'itemPrice', 'needsfetchreview', 'partneritemid','originalreceiptitemtext', 'preventtargetgappoints', 'quantitypurchased', 'userflaggedbarcode', 'userflaggednewitem', 'userflaggedprice', 'userflaggedquantity']
	for rewards in receip_data:
		li = []
		try:
			for reward in rewards['rewardsReceiptItemList']:
				try:
					li.append(rewards["_id"]['$oid'])
				except KeyError:
					li.append(None)

				try:
					li.append(reward['barcode'])
				except KeyError:
					li.append(None)
				
				try:
					li.append(reward['description'])
				except KeyError:
					li.append(None)

				try:
					li.append(reward['finalPrice'])
				except KeyError:
					li.append(None)
				
				try:
					li.append(reward['itemPrice'])
				except KeyError:
					li.append(None)

				try:
					li.append(reward['needsFetchReview'])
				except KeyError:
					li.append(None)

				try:
					li.append(reward['partnerItemId'])
				except KeyError:
					li.append(None)

				try:
					li.append(reward['originalReceiptItemText'])
				except KeyError:
					li.append(None)

				try:
					li.append(reward['preventTargetGapPoints'])
				except KeyError:
					li.append(None)

				try:
					li.append(reward['quantityPurchased'])
				except KeyError:
					li.append(None)
				
				try:
					li.append(reward['userFlaggedBarcode'])
				except KeyError:
					li.append(None)

				try:
					li.append(reward['userFlaggedNewItem'])
				except KeyError:
					li.append(None)

				try:
					li.append(reward['userFlaggedPrice'])
				except KeyError:
					li.append(None)

				try:
					li.append(reward['userFlaggedQuantity'])
				except KeyError:
					li.append(None)

		except KeyError:
				li.append(None)

		records.append(li)
		
	print("Recepient Reward list Prepared")

	return records, headers




def create_table(records, headers, file_path):
	# Take a list of records and headers and generate csv from it
	
    f = open(file_path, 'w', encoding='utf-8')
    row_len = len(headers)
    f.write(format_list(headers, row_len, ',', '\"'))

    for record in records:
        f.write(format_list(record, row_len, ',', '\"'))
    f.close()
    print('CSV file successfully created: {}'.format(file_path))


def format_list(list, length, delimiter, quote):
    counter = 1
    string = ""
    for record in list:
        if counter == length:
            string += quote + str(record) + quote + '\n'
        else:
            string += quote + str(record) + quote + delimiter
        counter += 1
    return string



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
	saved_data = []
	# Parsing json data from local memory and converting into python dict
	# Iter over all the python dict and add into a list
	with open('receipts.json', 'r', encoding="utf-8") as f:
		for line in f:
			json_line = json.loads(line)
			saved_data.append(json_line)

		receipt_tup = create_receipts(saved_data)
		receipt_records = receipt_tup[0]		
		receipt_headers = receipt_tup[1]
		create_table(receipt_records, receipt_headers, './receipts.csv')


		reward_tuple = receipts_reward_list(saved_data)
		reward_records = reward_tuple[0]
		reward_headers = reward_tuple[1]
		create_table(reward_records, reward_headers, "./rewards.csv")

		pg_load_table("./receipts.csv", 'receipts')
	
	
if __name__ == "__main__":
    main()



