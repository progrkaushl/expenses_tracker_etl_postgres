from utils import postgres_execute
from utils import _fname_arg
import sys



def construct_commands(filename=None) -> tuple:


    assert filename is not None

    drop_raw_tbl = "DROP TABLE IF EXISTS expenses_raw CASCADE"

    create_raw_tbl = """
    CREATE TABLE IF NOT EXISTS expenses_raw
    (trans_id char(10), 
    trans_dt char(26), 
    trans_type char(26), 
    trans_amt varchar, 
    category VARCHAR, 
    trans_cmt varchar, 
    trans_src varchar)
    """

    tbl_load_data = """
    COPY expenses_raw FROM '{0}' DELIMITER ',' CSV HEADER
    """.format(filename)

    create_raw_final_vw = """
    CREATE MATERIALIZED VIEW IF NOT EXISTS expenses_final_vw AS
    SELECT 
        trans_id::numeric, 
        TO_DATE(trans_dt, 'mm/dd/yyyy') AS trans_dt, 
        trans_type, 
        REPLACE(trans_amt,',','')::numeric AS trans_amt, 
        category, 
        trans_cmt, 
        trans_src
    FROM expenses_raw
    """

    create_cate_summary_vw = """
    CREATE MATERIALIZED VIEW expenses_cate_summary_vw AS
    WITH expenses_summary_1 AS (
        SELECT 
        category, 
        sum(CASE WHEN trans_type = 'Credit' THEN trans_amt ELSE 0 END) as credit,
        sum(CASE WHEN trans_type = 'Debit' THEN trans_amt ELSE 0 END) as debit
        FROM expenses_final_vw 
        GROUP BY category
    )
    SELECT 
        category,
        credit,
        debit,
        (credit - debit) AS diff
    FROM expenses_summary_1 
    ORDER BY category ASC
    """

    create_src_summary_vw = """
    CREATE MATERIALIZED VIEW expenses_src_summary_vw AS
    WITH expenses_summary_1 AS (
        SELECT 
        trans_src, 
        sum(CASE WHEN trans_type = 'Credit' THEN trans_amt ELSE 0 END) as credit,
        sum(CASE WHEN trans_type = 'Debit' THEN trans_amt ELSE 0 END) as debit
        FROM expenses_final_vw 
        GROUP BY trans_src
    )
    SELECT 
        trans_src,
        credit,
        debit,
        (credit - debit) AS diff
    FROM expenses_summary_1 
    ORDER BY trans_src ASC
    """

    return (
        drop_raw_tbl, 
        create_raw_tbl, 
        tbl_load_data, 
        create_raw_final_vw, 
        create_cate_summary_vw,
        create_src_summary_vw
        )


def execute_commands(filename=None) -> None:
    
    try:
        print('Generating Commands...')
        commands = construct_commands(filename)

        if len(commands) > 0:
            print('Loading data...')
            postgres_execute(commands=commands)

    except Exception as e:
        print(e)



if __name__ == "__main__":

    try:

        argument = _fname_arg()
        print("File path: {0}".format(argument.filepath))

        if len(argument.filepath) > 0:
            print("Executing the ETL code...")
            execute_commands(filename=argument.filepath)
        else:
            print("Check and provide correct full file path, current file path {0}".format(argument.filepath))

    except Exception as e:
        print(e)



