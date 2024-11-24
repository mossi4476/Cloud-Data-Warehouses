import configparser
import psycopg2
import logging
from sql_queries import copy_table_queries, insert_table_queries

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

def load_staging_tables(cur, conn):
    """
    Load data into staging tables using COPY command.
    """
    for query in copy_table_queries:
        try:
            logger.info(f"Executing COPY query: {query}")
            cur.execute(query)
            conn.commit()
        except Exception as e:
            logger.error(f"Error executing COPY query: {query}")
            logger.error(f"Error: {str(e)}")
            conn.rollback()

def insert_tables(cur, conn):
    """
    Insert data into the final tables from staging tables.
    """
    for query in insert_table_queries:
        try:
            logger.info(f"Executing INSERT query: {query}")
            cur.execute(query)
            conn.commit()
        except Exception as e:
            logger.error(f"Error executing INSERT query: {query}")
            logger.error(f"Error: {str(e)}")
            conn.rollback()

def main():
    """
    Main function to run the ETL process.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')  # Read configuration file

    # Establish connection to Redshift
    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DB'].values()))
        cur = conn.cursor()
        logger.info("Connected to Redshift successfully")
    except Exception as e:
        logger.error(f"Error connecting to Redshift: {str(e)}")
        return

    # Run the ETL process
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    # Close connection
    conn.close()
    logger.info("Connection closed")

if __name__ == "__main__":
    main()
