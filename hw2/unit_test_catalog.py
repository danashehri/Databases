import CSVCatalog
import json
import pymysql

# Example test, you will have to update the connection info
# Implementation Provided

batting_columns = ["playerID","yearID","stint","teamID","lgID","G","AB","R","H","2B","3B","HR","RBI","SB","CS","BB",
                   "SO","IBB","HBP","SH","SF","GIDP"]

people_columns = ["playerID","birthYear","birthMonth","birthDay","birthCountry","birthState","birthCity","deathYear",
                  "deathMonth","deathDay","deathCountry","deathState","deathCity","nameFirst","nameLast","nameGiven",
                  "weight","height","bats","throws","debut","finalGame","retroID","bbrefID"]

def create_table_test():
    conn = pymysql.connect(host="localhost",
                           port=3306,
                           user="root",
                           password="56781234",
                           db="CSVCatalog")
    cat = CSVCatalog.CSVCatalog()
    cat.create_table("Batting", "./Batting.csv")
    cat.create_table("People", "./People.csv")
    cat.create_table("test_table", "file_path_test.woo")
#create_table_test()

def drop_table_test():

    cat = CSVCatalog.CSVCatalog()
    cat.drop_table("test_table")
    cat.drop_table("Batting")
    cat.drop_table("People")

#drop_table_test()

def add_column_test():

    cat = CSVCatalog.CSVCatalog()

    #adding column definition to test_table table
    t = cat.get_table("test_table")
    columns = ["firstName", "lastName"]
    dict = {
            "column_name": "firstName",
            "column_type": "text",
            "not_null": False
        }
    for c in columns:
        col = CSVCatalog.ColumnDefinition(c, "text", False)
        t.add_column_definition(col)

    #Adding columns to Batting table
    t2 = cat.get_table("Batting")
    for c in batting_columns:
        if c in ["playerID", "teamID", "lgID"]:
            tt = "text"
        else:
            tt = "number"

        if c in ["playerID", "yearID", "stint"]:
            n_null = True
        else:
            n_null = False

        new_c = CSVCatalog.ColumnDefinition(c, tt, n_null)  # dict type
        if t2.columns is {}: t2.columns = []
        t2.add_column_definition(new_c)
    t2 = cat.get_table("Batting")

    #Adding columns to People Table
    t3 = cat.get_table("People")
    for c in people_columns:
        if c in ["playerID", "birthCountry", "birthState", "birthCity", "deathCountry", "deathState",
                 "deathCity", "nameFirst", "nameLast", "nameGiven",
                 "bats", "throws", "debut", "finalGame", "retroID", "bbrefID"]:
            tt = "text"
        else:
            tt = "number"

        if c in ["playerID"]:
            n_null = True
        else:
            n_null = False

        new_c = CSVCatalog.ColumnDefinition(c, tt, n_null)  # dict type
        t3.add_column_definition(new_c)

#add_column_test()

# Implementation Provided
# Fails because no name is given
def column_name_failure_test():
    cat = CSVCatalog.CSVCatalog()
    col = CSVCatalog.ColumnDefinition(None, "text", False)
    t = cat.get_table("test_table")
    t.add_column_definition(col)

#column_name_failure_test()

# Implementation Provided
# Fails because "canary" is not a permitted type
def column_type_failure_test():
    cat = CSVCatalog.CSVCatalog(
        dbhost="localhost",
        dbport=3306,
        dbuser="root",
        dbpw="56781234",
        db="CSVCatalog")
    col = CSVCatalog.ColumnDefinition("bird", "canary", False)
    t = cat.get_table("test_table")
    t.add_column_definition(col)

#column_type_failure_test()

# Implementation Provided
# Will fail because "happy" is not a boolean
def column_not_null_failure_test():
    cat = CSVCatalog.CSVCatalog(
        dbhost="localhost",
        dbport=3306,
        dbuser="root",
        dbpw="56781234",
        db="CSVCatalog")
    col = CSVCatalog.ColumnDefinition("name", "text", "happy")
    t = cat.get_table("test_table")
    t.add_column_definition(col)

#column_not_null_failure_test()

def add_index_test():

    cat = CSVCatalog.CSVCatalog()

    #Adding index definition to test_table
    t = cat.get_table("test_table")
    t.define_index("primary", ["firstName", "lastName"], "PRIMARY")

    #Adding index definition to Batting table
    t1 = cat.get_table("Batting")
    t1.define_index("primary", ["playerID", "teamID", "stint"], "PRIMARY")

    #Adding index definition to People table
    t2 = cat.get_table("People")
    t2.define_index("primary", ["playerID"], "PRIMARY")
    t2.define_index("name", ["nameFirst", "nameLast"], "INDEX")

#add_index_test()


def col_drop_test():

    #dropping column from test_table
    cat = CSVCatalog.CSVCatalog()
    cn = "firstName"
    t = cat.get_table("test_table")
    t.drop_column_definition(cn)
    t.drop_col_in_sql(cn)

    #dropping column from Batting table
    col = "GIDP"
    t2 = cat.get_table("Batting")
    t2.drop_column_definition(col)
    t2.drop_col_in_sql(col)

    # dropping column from People table
    col = "height"
    t3 = cat.get_table("People")
    t3.drop_column_definition(col)
    t3.drop_col_in_sql(col)

#col_drop_test()

def index_drop_test():
    #dropping index from test_table
    cat = CSVCatalog.CSVCatalog()
    idx_name = "primary"
    t = cat.get_table("test_table")
    t.drop_index(idx_name)
    t.drop_indx_in_sql(idx_name)

#index_drop_test()

# Implementation provided
def describe_table_test():
    cat = CSVCatalog.CSVCatalog()
    t = cat.get_table("batting")
    desc = t.describe_table()
    print("DESCRIBE People = \n", json.dumps(desc, indent = 2))

#describe_table_test()

