from config import config
import psycopg2
import argparse


def _fname_arg():
    parser = argparse.ArgumentParser()
    parser.add_argument('--filepath', dest="filepath", metavar="-f", type=str, required=True)
    return parser.parse_args()


def postgres_execute(commands):
    conn = None

    try:
        params = config()

        print('Connecting to PostgreSQL datababse....')
        conn = psycopg2.connect(**params)

        crsr = conn.cursor()

        for command in commands:
            crsr.execute(command)
        
        print('Closing connection..')
        crsr.close()
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as e:
        print(e)
    
    finally:
        if conn is not None:
            conn.close()
