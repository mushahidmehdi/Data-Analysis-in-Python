import psycopg2
import json


def pg_load(table_name, file_path):
    try:
        conn = psycopg2.connect(dbname="fetchreward", user='root', host="localhost", password="21901085")

        print("Connecting to Database")
        cur = conn.cursor()
        f = open(file_path, "r")

        cur.execute("Truncate {} Cascade;".format(table_name))
        print("Truncated {}".format(table_name))
		
        cur.copy_expert("copy {} FROM STDIN WITH CSV quote e'\x01' delimiter e'\x02'".format(table_name), f)
        cur.execute("commit;")
        print("Loaded data into {}".format(table_name))

        conn.close()
        print("DB connection closed.")
    except Exception as e:
        print('Error {}'.format(str(e)))

def main():

		pg_load('_receipts', 'receipts.json')
	
if __name__ == "__main__":
    main()



