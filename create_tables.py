import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """ Description: This function is used to drop existing tables helping in multiple iterations of testing
		Arguments:
			cur: the cursor object. 
			conn: connection to tables for drop. 
		Returns:
			None
	"""
	for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
 """ Description: This function is used to create tables helping in multiple iterations of testing
		Arguments:
			cur: the cursor object. 
			conn: connection to tables for create. 
		Returns:
			None
	"""

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
def main():

 """ Description: main function to parse the .cfg file, connect to Redshift Cluster and create tables
		Arguments:
			cur: the cursor object. 
			conn: connection to tables for drop and create. 
		Returns:
			None
	"""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host=dwhcluster1.cxytgf3wqhyp.us-west-2.redshift.amazonaws.com dbname=dwh1 user=dwhuser1 password=Passw0rd port=5439".format(*config['DB'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()