import psycopg2
import pymysql
from psycopg2 import sql

pg_config = {
    "dbname": "POSTGRES_ENV_DATABASE_DBNAME",
    "user": "DATABASE_USER",
    "password": "DATABASE_PASS",
    "host": "DATABASE_HOST",
    "port": 5432
}

mysql_config = {
    "host": "DATABASE_HOST",
    "user": "DATABASE_USER",
    "password": "DATABASE_PASS",
    "database": "DATABASE_DBNAME",
    "port": 3306
}

BATCH_SIZE = 5000

def map_postgres_to_mysql(data_type, char_length):
    """Maps PostgreSQL data types to MySQL data types."""
    if data_type == "integer":
        return "INT"
    elif data_type == "bigint":
        return "BIGINT"
    elif data_type == "boolean":
        return "TINYINT(1)"
    elif data_type in ("character varying", "text"):
        return f"VARCHAR({char_length if char_length else 255})"
    elif data_type == "timestamp without time zone":
        return "DATETIME"
    elif data_type == "numeric":
        return "DECIMAL(10, 2)"
    else:
        return "TEXT"

def create_mysql_table(mysql_cursor, table_name, columns):
    """Creates a MySQL table based on the given PostgreSQL columns."""
    create_table_query = f"CREATE TABLE IF NOT EXISTS `{table_name}` ("
    for column in columns:
        col_name = column[0]
        data_type = column[1]
        char_length = column[2]
        mysql_type = map_postgres_to_mysql(data_type, char_length)
        create_table_query += f"`{col_name}` {mysql_type}, "
    create_table_query = create_table_query.rstrip(", ") + ")"
    mysql_cursor.execute(create_table_query)

def migrate_data(pg_cursor, mysql_cursor, table_name, total_rows):
    """Migrates data from PostgreSQL to MySQL in batches."""
    offset = 0
    while offset < total_rows:
        try:
            query = sql.SQL("SELECT * FROM {} LIMIT %s OFFSET %s").format(sql.Identifier(table_name))
            pg_cursor.execute(query, (BATCH_SIZE, offset))
            rows = pg_cursor.fetchall()
            if rows:
                col_names = [desc[0] for desc in pg_cursor.description]
                col_names_escaped = [f"`{col}`" for col in col_names]
                placeholders = ", ".join(["%s"] * len(col_names))
                insert_query = f"INSERT INTO `{table_name}` ({', '.join(col_names_escaped)}) VALUES ({placeholders})"
                try:
                    mysql_cursor.executemany(insert_query, rows)
                    mysql_conn.commit()
                except pymysql.MySQLError as e:
                    print(f"Error during insert into {table_name}: {e}")
                    mysql_conn.rollback()
            offset += BATCH_SIZE
            print(f"Migrated {offset}/{total_rows} rows for {table_name}")
        except Exception as e:
            print(f"Error during migration of {table_name} at offset {offset}: {e}")
            break

try:
    pg_conn = psycopg2.connect(**pg_config)
    pg_cursor = pg_conn.cursor()

    mysql_conn = pymysql.connect(**mysql_config)
    mysql_cursor = mysql_conn.cursor()

    pg_cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
    """)
    tables = pg_cursor.fetchall()

    for table in tables:
        table_name = table[0]
        print(f"Migrating table: {table_name}")

        try:
            pg_cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
            total_rows = pg_cursor.fetchone()[0]
            print(f"Total rows to migrate for {table_name}: {total_rows}")
        except Exception as e:
            print(f"Skipping table {table_name} due to error: {e}")
            continue

        pg_cursor.execute("""
            SELECT column_name, data_type, character_maximum_length
            FROM information_schema.columns
            WHERE table_name = %s
        """, (table_name,))
        columns = pg_cursor.fetchall()

        try:
            create_mysql_table(mysql_cursor, table_name, columns)
            mysql_conn.commit()
        except Exception as e:
            print(f"Skipping table {table_name} due to error during table creation: {e}")
            continue

        migrate_data(pg_cursor, mysql_cursor, table_name, total_rows)

    print("Migration completed successfully!")

except Exception as e:
    print("Error during migration:", e)

finally:
    if pg_cursor: pg_cursor.close()
    if pg_conn: pg_conn.close()
    if mysql_cursor: mysql_cursor.close()
    if mysql_conn: mysql_conn.close()