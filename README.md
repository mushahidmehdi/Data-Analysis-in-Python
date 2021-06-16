# JSON-Data-Analysis-in-Python

### Data Analysis - Fetch Rewards Coding Exercise 
In this project we will demonstrate how to understand data and relate one data set to other to answer predetermine bussiness question.

There are various methods we load the data into database:


###### 1- Traditional way of uploading data into database using Python 
###### 2- Writing SQL dialect patriculay related to the RBMS in used in my case PostgreSQL.
(Data-Analysis-Using-PostgreSQL-Dialects https://github.com/mushahidmehdi/Data-Analysis-Using-PostgreSQL-dialects in this repo we will perform exactly same task but using PostgreSQL-Dialects)


## In This Project we will use Python function to create a RDBMS 

### STEPS:

1- Review unstructure json data and build a new structure relational data base in PostgreSQL using Postgres Dilects.                                                 
2- Generate query to answer predetermine bussiness question.                                                                                          
3- Write a short Email to bussiness stack holder about predertmined questions.                                                                                         

We have three different json data.

### 1- user.json

![user-json](https://user-images.githubusercontent.com/66418035/122202570-dbf31a00-cea5-11eb-9c1c-a09b60f2de04.png)

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
          
          
![user-csv](https://user-images.githubusercontent.com/66418035/122203011-4906af80-cea6-11eb-815a-6633f3c74b01.png)



###### Load data into postgres database


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

###### Creating User Table 

		fetchreward=# CREATE TABLE users(
		id VARCHAR(100) PRIMARY KEY,
		active VARCHAR(100),
		createddate VARCHAR(100),
		lastlogin VARCHAR(100),
		role VARCHAR(1000),
		signupscore VARCHAR(100),
		state VARCHAR(100));

            
 ![user-database](https://user-images.githubusercontent.com/66418035/122211755-bff47600-ceaf-11eb-9c89-9694ec29c0d5.png)

            
### 2- brands.json

![brands-json](https://user-images.githubusercontent.com/66418035/122209582-5a9f8580-cead-11eb-984f-a57ffd2c1490.png)


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


![brands-csv](https://user-images.githubusercontent.com/66418035/122209772-93d7f580-cead-11eb-8a1f-02b954541771.png)


###### loading csv file into Database
	
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
                    
![brands-database](https://user-images.githubusercontent.com/66418035/122211818-d1d61900-ceaf-11eb-9bf3-70bf5712c21e.png)


### 3- receipts.json 



![receipts-json](https://user-images.githubusercontent.com/66418035/122210078-e87b7080-cead-11eb-8e02-7b08b5151d9d.png)

Receipts data contain lists init which will be a diffent table having forign key reference of receipts:

                def create_receipts(receip_data):
                        # Take the query and convert into a list along headers

                        records = []

                        headers = ['id','bonusPointsEarned','bonusPointsEarnedReason','createDate_date',
                        'dateScanned_date','finishedDate_date','modifyDate_date','pointsAwardedDate_date',
                        'pointsEarned','purchaseDate_date', 'purchasedItemCount','rewardsReceiptStatus','totalSpent','userId']

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


![receipts-csv](https://user-images.githubusercontent.com/66418035/122210108-f0d3ab80-cead-11eb-8e8c-a79486f5cc4e.png)


                def receipts_reward_list(receip_data):

                        # Take the query and convert into a list along headers
                        records = []

                        headers = ['id	','barcode', 'description', 'finalPrice', 'itemPrice', 'needsfetchreview',
                        'partneritemid','originalreceiptitemtext', 'preventtargetgappoints', 'quantitypurchased',
                        'userflaggedbarcode', 'userflaggednewitem', 'userflaggedprice', 'userflaggedquantity']
                        
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



![rewards-csv](https://user-images.githubusercontent.com/66418035/122210097-ee715180-cead-11eb-9d7f-aee280d7a035.png)



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



###### Load data into postgres database
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

![receipts-database](https://user-images.githubusercontent.com/66418035/122211850-dbf81780-ceaf-11eb-9e0f-12fac20623d7.png)




