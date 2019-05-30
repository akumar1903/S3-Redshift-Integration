import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
 """ Description: This function is used to load staging tables prior to moving the data to DIM and FACT
		Arguments:
			cur: the cursor object
			conn: connection to staging tables 
		Returns:
			None
	"""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

""" Description: This function is used to insert data to FACT and DIM tables created in Redshift
		Arguments:
			cur: the cursor object
			conn: connection to staging tables 
		Returns:
			None
	"""

#def count_tables(cur, conn):
 ##   for query in count_tables_queries:
   ##     cur.execute(query)
     ##   conn.commit()
        
def main():
""" Description: Function connecting to redshift cluster and loading staging and inserting data to dim and fact tables
		Arguments:
			cur: the cursor object
			conn: connection to redshift cluster
		Returns:
			None
	"""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn = psycopg2.connect("host=dwhcluster1.cxytgf3wqhyp.us-west-2.redshift.amazonaws.com dbname=dwh1 user=dwhuser1 password=Passw0rd port=5439".format(*config['DB'].values()))
    cur = conn.cursor()
   
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    ##count_tables(cur,conn)
    conn.close()


if __name__ == "__main__":
    main()