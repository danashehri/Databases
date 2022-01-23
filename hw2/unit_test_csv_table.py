import CSVTable
import CSVCatalog
import json
import csv
import pymysql


batting_columns = ["playerID","yearID","stint","teamID","lgID","G","AB","R","H","2B","3B","HR","RBI","SB","CS","BB",
                   "SO","IBB","HBP","SH","SF","GIDP"]

people_columns = ["playerID","birthYear","birthMonth","birthDay","birthCountry","birthState","birthCity","deathYear",
                  "deathMonth","deathDay","deathCountry","deathState","deathCity","nameFirst","nameLast","nameGiven",
                  "weight","height","bats","throws","debut","finalGame","retroID","bbrefID"]

appearances_columns = ["yearID","teamID","lgID","playerID","G_all","GS","G_batting","G_defense","G_p","G_c","G_1b","G_2b",
                       "G_3b","G_ss","G_lf","G_cf","G_rf","G_of","G_dh","G_ph","G_pr"]

smart_join_time = 0
dumb_join_time = 0

#Must clear out all tables in CSV Catalog schema before using if there are any present
#Please change the path name to be whatever the path to the CSV files are
#First methods set up metadata!! Very important that all of these be run properly

# Only need to run these if you made the tables already in your CSV Catalog tests
# You will not need to include the output in your submission as executing this is not required
# Implementation is provided
def drop_tables_for_prep():
    cat = CSVCatalog.CSVCatalog()
    cat.drop_table("People")
    cat.drop_table("Batting")
    cat.drop_table("Appearances")

#drop_tables_for_prep()

# Implementation is provided
# You will need to update these with the correct path
def create_lahman_tables():
    cat = CSVCatalog.CSVCatalog()
    cat.create_table("People", "/Users/danaalshehri/Documents/W4111-Databases/W4111_HW2_Programming/Short_Lahman(HW2)/NewPeople.csv")
    cat.create_table("Batting","/Users/danaalshehri/Documents/W4111-Databases/W4111_HW2_Programming/Short_Lahman(HW2)/NewBatting.csv")
    cat.create_table("Appearances", "./Short_Lahman(HW2)/NewAppearances.csv")

#create_lahman_tables()

# Note: You can default all column types to text
def update_people_columns():
    cat = CSVCatalog.CSVCatalog()
    t = cat.get_table('People')
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
        t.add_column_definition(new_c)

#update_people_columns()

def update_appearances_columns():
    cat = CSVCatalog.CSVCatalog()
    t = cat.get_table('Appearances')
    for c in appearances_columns:
        if c in ["playerID", "teamID", "lgID"]:
            tt = "text"
        else:
            tt = "number"

        if c in ["playerID"]:
            n_null = True
        else:
            n_null = False

        new_c = CSVCatalog.ColumnDefinition(c, tt, n_null)  # dict type
        t.add_column_definition(new_c)

#update_appearances_columns()

def update_batting_columns():

    cat = CSVCatalog.CSVCatalog()
    t = cat.get_table('Batting')

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
        t.add_column_definition(new_c)

#update_batting_columns()

#Add primary key indexes for people, batting, and appearances in this test
def add_index_definitions():

    cat = CSVCatalog.CSVCatalog()
    t1 = cat.get_table('Batting')
    t1.define_index("primary", ["playerID", "lgID", "yearID"], "PRIMARY")

    t2 = cat.get_table('People')
    t2.define_index("primary", ["playerID"], "PRIMARY")

    t3 = cat.get_table('Appearances')
    t3.define_index("primary", ["playerID"], "PRIMARY")

#add_index_definitions()


def test_load_info():
    table = CSVTable.CSVTable("People")
    print(table.__description__.file_name)

#test_load_info()

def test_get_col_names():
    table = CSVTable.CSVTable("People")
    names = table.__get_column_names__()
    print(names)

#test_get_col_names()

def add_other_indexes():
    """
    We want to add indexes for common user stories
    People: nameLast, nameFirst
    Batting: teamID
    Appearances: None that are too important right now
    :return:
    """

    cat = CSVCatalog.CSVCatalog()
    t1 = cat.get_table('Batting')
    t1.define_index("team", ["teamID"], "INDEX")

    t2 = cat.get_table('People')
    t2.define_index("name", ["nameFirst", "nameLast"], "INDEX")

#add_other_indexes()

def load_test():
    batting_table = CSVTable.CSVTable("Batting")
    print(batting_table)

#load_test()


def dumb_join_test():
    batting_table = CSVTable.CSVTable("Batting")
    people_table = CSVTable.CSVTable("People")
    result = batting_table.dumb_join(people_table, ["playerID"], {"playerID" : "baxtemi01", "lgID": "NL"},
                                     ["playerID", "yearID", "teamID", "AB", "H", "lgID", "stint"])
    global dumb_join_time
    dumb_join_time = batting_table.dumb_time
    print(result)

#dumb_join_test()

def get_access_path_test():
    batting_table = CSVTable.CSVTable("Batting", load=True)
    template = {"teamID": "NL", "playerID": "aardsda01", "yearID": "2009", "lgID": "NL"}
    index_result, count = batting_table.__get_access_path__(template)
    print("Most selective index for batting table = ", index_result)
    print("Count = ", count)
    print()

    people_table = CSVTable.CSVTable("People", load=True)
    temp = {"playerID": "aardsda01", "nameFirst": "Hank", "nameLast": "Aaron"}
    index_result, count = people_table.__get_access_path__(temp)
    print("Most selective index for people table = ", index_result)
    print("Count = ", count)

#get_access_path_test()

def sub_where_template_test():
    batting_table = CSVTable.CSVTable('Batting', load=True)
    where_template = {"lgID": "SEA", "playerID": "aardsda01", "yearID": "2009", "nameFirst": "Hank", "nameLast": "Aaron"}
    sub_template = batting_table.__get_sub_where_template__(where_template)
    print(sub_template)

#sub_where_template_test()


def test_find_by_template_index():

    batting_table = CSVTable.CSVTable("Batting", load=True)
    template1 = {"teamID": "SEA", "playerID": "aardsda01", "yearID": "2009"}
    index_name, count = batting_table.__get_access_path__(template1)
    result = batting_table.__find_by_template_index__(template1, index_name)

    for row in result:
        print(row)

#test_find_by_template_index()

def smart_join_test():

    batting_table = CSVTable.CSVTable("Batting")
    people_table = CSVTable.CSVTable("People")
    result = batting_table.__smart_join__(people_table, ["playerID"], {"playerID": "baxtemi01", "lgID": "NL"},
                                     ["playerID", "yearID", "teamID", "AB", "H", "lgID", "stint"])
    global smart_join_time
    smart_join_time = batting_table.smart_time
    print(result)

#smart_join_test()


def timer():

    print("How long it took to execute dumb join = ", dumb_join_time)
    print("How long it took to execute smart join = ", smart_join_time)

#timer()




