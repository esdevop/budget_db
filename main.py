import os
import psycopg2
from psycopg2 import sql
import dotenv

CREATE_TABLE_APPUSER = """CREATE TABLE IF NOT EXISTS app_user (
    appuser_login VARCHAR(50) PRIMARY KEY,
    appuser_pass VARCHAR(50) NOT NULL
    );"""

CREATE_APPUSER = "INSERT INTO app_user (appuser_login, appuser_pass) VALUES (%s, %s);"
GET_APPUSER = "SELECT * from app_user WHERE appuser_login = %s"

CREATE_TABLE_EXPENCESGROUP = """CREATE TABLE IF NOT EXISTS expences_group (
    expGroup_id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    expGroup_name VARCHAR(50) NOT NULL,
    appuser_login VARCHAR(50),
    CONSTRAINT fk_expGroup
        FOREIGN KEY(appuser_login)
        REFERENCES app_user(appuser_login)
        ON DELETE CASCADE
    );"""

CREATE_EXPENCESGROUP = "INSERT INTO expences_group (appuser_login, expGroup_name) VALUES (%s, %s);"
GET_EXPENCESGROUP = "SELECT * from expences_group WHERE appuser_login = %s AND expGroup_name = %s"

CREATE_TABLE_SOURCE = """CREATE TABLE IF NOT EXISTS source (
    source_id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    source_name VARCHAR(50) NOT NULL,
    appuser_login VARCHAR(50),
    CONSTRAINT fk_source
        FOREIGN KEY(appuser_login)
        REFERENCES app_user(appuser_login)
        ON DELETE CASCADE
    );"""

CREATE_SOURCE = "INSERT INTO source (appuser_login, source_name) VALUES (%s, %s);"
GET_SOURCE = "SELECT * from source WHERE appuser_login = %s AND source_name = %s"
 
CREATE_TABLE_EXPENCES = """CREATE TABLE IF NOT EXISTS expences (
    expences_id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    expences_name VARCHAR(50) NOT NULL,
    expences_value DECIMAL(10,2) NOT NULL,
    expences_date DATE NOT NULL,
    appuser_login VARCHAR(50),
    expgroup_id INT,
    source_id INT,
    FOREIGN KEY(appuser_login) REFERENCES app_user(appuser_login) ON DELETE CASCADE,
    FOREIGN KEY(expgroup_id) REFERENCES expences_group(expgroup_id) ON DELETE CASCADE,
    FOREIGN KEY(source_id) REFERENCES source(source_id) ON DELETE CASCADE
    );"""

GET_ID = "SELECT * from {table} WHERE appuser_login = %s AND {column} = %s;"
CREATE_EXPENCES = """INSERT INTO expences (
    expences_name,
    expences_value,
    expences_date,
    appuser_login,
    expgroup_id,
    source_id) 
    VALUES (%s, %s, %s, %s, %s, %s);"""

def connect():    
    dotenv.load_dotenv()
    DATABASE = os.getenv('DATABASE')
    DBUSER = os.getenv('DBUSER')
    DBPASS = os.getenv('DBPASS')
    DBHOST = os.getenv('DBHOST')
    DBPORT = os.getenv('DBPORT')
    return psycopg2.connect(database=DATABASE, user=DBUSER, password=DBPASS, host="localhost", port=DBPORT)

def create_table_appuser(cursor):
    cursor.execute(CREATE_TABLE_APPUSER)
    #print("APP_USER table is created")

def appuser_exsists(cursor, login):
    cursor.execute(GET_APPUSER, (login,))
    data = cursor.fetchall()
    if not data:
        return False
    else:
        return True

def create_appuser(cursor, login, password):
    if appuser_exsists(cursor, login):
        raise Warning("This login already exsists. Please try again.")
    else:
        cursor.execute(CREATE_APPUSER, (login, password))
        print("user {} has been added".format(login))

def create_table_expencesgroup(cursor):
    cursor.execute(CREATE_TABLE_EXPENCESGROUP)

def expencesgroup_exsists(cursor, login, expGroup_name):
    cursor.execute(GET_EXPENCESGROUP, (login, expGroup_name))
    data = cursor.fetchall()
    if not data:
        return False
    else:
        return True

def create_expencesgroup(cursor, login, expGroup_name):
    if appuser_exsists(cursor, login):
        if not(expencesgroup_exsists(cursor, login, expGroup_name)):
            cursor.execute(CREATE_EXPENCESGROUP, (login, expGroup_name))
            print("ExpencesGroup {} has been added".format(expGroup_name))
        else:
            print("ExpencesGroup {} already exsists for the user {}".format(expGroup_name, login))
    else:
        raise Warning("This login does not exsist. Please try again.")

def create_table_source(cursor):
    cursor.execute(CREATE_TABLE_SOURCE)

def source_exsists(cursor, login, source_name):
    cursor.execute(GET_SOURCE, (login, source_name))
    data = cursor.fetchall()
    if not data:
        return False
    else:
        return True

def create_source(cursor, login, source_name):
    if appuser_exsists(cursor, login):
        if not(source_exsists(cursor, login, source_name)):
            cursor.execute(CREATE_SOURCE, (login, source_name))
            print("Source {} has been added".format(source_name))
        else:
            print("Source {} already exsists for the user {}".format(source_name, login))
    else:
        raise Warning("This login does not exsist. Please try again.")

def create_table_expences(cursor):
    cursor.execute(CREATE_TABLE_EXPENCES)

def get_id(cursor, table_name=None, login=None, column_name=None, column_value=None):
    query = sql.SQL(GET_ID).format(
        table=sql.Identifier(table_name),
        column=sql.Identifier(column_name),
        )
    cursor.execute(query, (login, column_value))
    data = cursor.fetchall()
    return data[0][0]

def create_expences(cursor, 
        expences_name = None, 
        expences_value = None,
        expences_date = None,
        expGroup_name = None,
        source_name = None,
        login=None
    ):
    if not(appuser_exsists(cursor, login)):
        raise Warning("This login does not exsist. Please try again.")
    elif not(source_exsists(cursor, login, source_name)):
        raise Warning("{} {} does not exsist for user {}. Please try again.".format("Source", source_name, login))
    elif not(expencesgroup_exsists(cursor, login, expGroup_name)):
        raise Warning("{} {} does not exsist for user {}. Please try again.".format("ExpencesGroup", expGroup_name, login))
    else:
        source_id = get_id(cursor, login=login, table_name="source", column_name="source_name", column_value=source_name)
        expGroup_id = get_id(cursor, login=login, table_name="expences_group", column_name="expgroup_name", column_value=expGroup_name)

        cursor.execute(CREATE_EXPENCES,
        (
               expences_name,
               expences_value,
               expences_date,
               login,
               expGroup_id,
               source_id 
        )
        )
        print("Expences {} has been added".format(expences_name))

# to see the total expences per source:
# SELECT s.source_name, SUM(e.expences_value) FROM expences e INNER JOIN source s ON e.source_id = s.source_id GROUP BY s.source_name;

# to see the total expences per month:
#SELECT EXTRACT(month from expences_date) AS date, SUM(expences_value) AS total_value FROM expences GROUP BY date;


