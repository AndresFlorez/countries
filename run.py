from table import Table
from tabulate import tabulate

if __name__ == '__main__':
    table = Table()
    table.start_database('database.db')
    table.create_db_table()
    table.build_data()
    table.insert_dataframe_db()
    table.generate_json_file()
    print(tabulate(table.table_df, headers = 'keys', tablefmt = 'psql'))
    table.show_times()
