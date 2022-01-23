import csv  # Python package for reading and writing CSV files.
import tabulate

# You MAY have to modify the path to match your project's structure.
import DataTableExceptions
import CSVCatalog
import time




# We won't use this but feel free to implement!
max_rows_to_print = 10



class CSVTable:
    # Table engine needs to load table definition information.
    __catalog__ = CSVCatalog.CSVCatalog()

    def __init__(self, t_name, load=True):
        """
        Constructor.

        Implementation is provided.
        :param t_name: Name for table.
        :param load: Load data from a CSV file. If load=False, this is a derived table and engine will
            add rows instead of loading from file.
        """

        self.__table_name__ = t_name

        # Holds loaded metadata from the catalog. You have to implement the called methods below.
        self.__description__ = None
        if load:
            self.__load_info__()  # Load metadata
            self.__rows__ = []
            self.__load__()  # Load rows from the CSV file.

        else:
            self.__file_name__ = "DERIVED"


    def __load_info__(self):
        """
        Loads metadata from catalog and sets __description__ to hold the information.
        :return:
        """

        cat = CSVCatalog.CSVCatalog()
        t = cat.get_table(self.__table_name__)
        self.__description__ = t


    def __get_file_name__(self):
        """
        Gets the file name from the description
        :return: string containing the file name
        """
        return self.__description__.file_name



    def __add_row__(self, row):
        """
        Adds a row to the table definition self.rows.

        Implementation is provided.
        :param row: The row to be added
        :return: Returns nothing
        """

        self.__rows__.append(row)
        defined_indexes = self.__description__.indexes[0] #list of index definition objects
        index_obj = set()
        index_names = []
        for index,v in defined_indexes.items():
            index_names.append(index)
            index_obj.add(v)


            for name in index_obj: #name is index definition object
                key_string = self.__get_key__(name,
                                              row)  # returns a string that is the concatentated version for the index

                if key_string in self.__keys_added__:  # means that key index already exists, row must be added to the list of rows
                    self.__indexes__[name][key_string].append(row)
                else:
                    if name not in self.__indexes__.keys():
                        self.__indexes__[name] = {}

                    self.__indexes__[name][key_string] = []
                    self.__indexes__[name][key_string].append(row)
                    self.__keys_added__.append(key_string)
        return


    def __get_key__(self, index, row):
        """
        Gets the key for the row based off the index columns.

        Implementation is provided.
        :param index: the index that we are creating the key for
        :param row: the row we are creating the key with, will also work for a template because a template is
                essentially a shortened row
        :return: a string
        """

        key = []
        column_names = index.column_names #index is index def object
        for i in range(len(column_names)):
            key.append(row[column_names[i]])

        kstring = "_".join(key)
        return kstring


    def __load__(self):
        """
        Load a table from a file into a CSVTable object.

        Implementation is provided.
        :return:
        """
        self.__indexes__ = {}  # initialized indexes dictionary
        given_indexes = self.__description__.indexes[0]
        self.__keys_added__ = []
        for k in given_indexes.keys():
            self.__indexes__[given_indexes[k]] = {}

        try:
            fn = self.__get_file_name__()
            with open(fn, "r") as csvfile:
                # CSV files can be pretty complex. You can tell from all of the options on the various readers.
                # The two params here indicate that "," separates columns and anything in between " " (double quotes)
                # should parse as a single string, even if it has things like "," in it.
                reader = csv.DictReader(csvfile, delimiter=",", quotechar='"')

                # Get the names of the columns defined for this table from the metadata.
                column_names = self.__get_column_names__()

                # Loop through each line (each dictionary to be precise) in the input file.
                for r in reader:
                    # Only add the defined columns into the in-memory table.
                    # The CSV file may contain columns that are not relevant to the definition.
                    projected_r = self.project([r], column_names)[0]

                    self.__add_row__(projected_r)


        except IOError as e:
            raise DataTableExceptions.DataTableException(
                code=DataTableExceptions.DataTableException.invalid_file,
                message="Could not read file = " + fn)


    def __get_column_names__(self):
        """
        Retrieves the column names from the table description.

        Implementation is provided.
        :return: a list with the column names
        """
        column_names = []
        column_list = self.__description__.columns
        for column in column_list:
            for k in column.keys(): #k is the column name
                column_names.append(k)
        return column_names


    def __get_access_path__(self, fields):
        """
        Returns best index matching the set of keys in the template. Best is defined as the most selective index, i.e.
        the one with the most distinct index entries.

        An index name is of the form "colname1_colname2_coluname3" The index matches if the
        template references the columns in the index name. The template may have additional columns, but must contain
        all of the columns in the index definition.

        There is a general overview of the function provided that you can use as guidance when implementing this method.

        :param tmp: Query template.
        :return: Two values, the index and the count, or none and None
        """
        # Do some initial error checking to make sure there are indexes for the table and there are fields passed

        # examine/loop through each index
            # If an applicable index is found
                # If there are no applicable indexes yet,save it
                # If there is another applicable index, compare the selectivity
        # Return the most selective index with its count or None and None

        indexes = self.__description__.indexes[0]
        keystrings = self.__indexes__
        if indexes is None or indexes == {}:
            raise ValueError('there are no indexes for the table!!')
        if fields is None:
            raise ValueError('there are no fields passed!!')

        max = 0
        no_of_rows = len(self.__rows__)
        avg = 0
        no_of_keys = 0
        most_selective = None
        idx_count = {}
        #defined columns in template
        temp_columns = [col for col in fields.keys()]
        for idx in indexes: #idx = index definition object

            # check no duplicate columns within index definitions
            unique_columns = set()
            columns = indexes[idx].column_names
            for c in columns:
                unique_columns.add(c)


            index_name = idx
            if not all(c in temp_columns for c in unique_columns):
                #not exact match
                continue

            for kk in list(keystrings.keys()):
                if kk.index_name == index_name:
                    no_of_keys = len(keystrings.get(kk))

            idx_count[index_name] = no_of_keys

        for k,v in idx_count.items():
            if v > max:
                max = v
                most_selective = k

        return most_selective, max


    def matches_template(self, row, t):
        """
        A helper function that returns True if the row matches the template.
        Similar to matches template from HW1

        Implementation for matches_template is provided.
        :param row: A single dictionary representing a row in the table.
        :param t: A template as a dictionary
        :return: True if the row matches the template, False if not
        """

        # Basically, this means there is no where clause.
        if t is None:
            return True

        try:
            c_names = list(t.keys())
            for n in c_names:
                if row[n] != t[n]:
                    return False
            else:
                return True
        except Exception as e:
            raise (e)


    def project(self, rows, fields):
        """
        Performs the project. Returns a new table with only the requested columns.
        Example: if fields is [playerID, teamID]
            the project would return a table equivalent to "SELECT playerID, teamID FROM tablename"

        Implementation of the project function is provided.
        :param fields: A list of column names.
        :return: A new table derived from this table by PROJECT on the specified column names.
        """
        try:
            if fields is None:  # If there is not project clause, return the base table
                return rows  # Should really return a new, identical table but am lazy.
            else:
                result = []

                if type(rows) == dict:  # one dictionay with column names as keys
                    # rows =  {'2B': '0', '3B': '0'...
                    tmp = {}
                    for j in range(0, len(fields)):
                        v = rows[fields[j]]  # value of the column name key # ex. r['yearID'] = 2004 = v
                        tmp[fields[j]] = v  # ex. tmp['yearID'] = v

                    else:
                        result.append(tmp)

                else:
                    for r in rows:  # For every row in the table.
                        tmp1 = {}
                        for j in range(0, len(fields)):  # Make a new row with just the requested columns/fields.
                            if type(r) == dict:
                                v = r[fields[j]] #value of the column name key # ex. r['yearID'] = 2004 = v
                                tmp1[fields[j]] = v # ex. tmp['yearID'] = v

                        else:
                            result.append(tmp1)  # Insert into new list of rows when done.
                return result

            # If the requested field not in rows.
        except KeyError as ke:
            raise DataTableExceptions.DataTableException(-2, "Invalid field in project")

    def __find_by_template_scan__(self, t, fields=None):
        """
        Returns a new, derived table containing rows that match the template and the requested fields if any.
        Returns all rows if template is None and all columns if fields is None.

        Hint: Use find_by_template from HW1's CSVDataTable for guidance

        :param t: The template representing a select predicate.
        :param fields: The list of fields (project fields)
        :return: New table containing the result of the select and project.
        """
        result = []
        rows = self.__get_row_list__()
        if t is None:
            return self.__rows__

        for r in reversed(rows):
            if self.matches_template(r, t):
                if fields is None:
                    columns = self.__get_column_names__()
                    new_r = self.project(r, columns)
                    result.append(new_r)
                else:
                    new_r = self.project(r, fields)
                    result.append(new_r)

        return result

    def __find_by_template_index__(self, t, idx_name, fields=None):
        """
        Find by template using a selected index.

        An example of an index is:
         {"TeamID": {"BOS":[list of dictionary rows with BOS]}, {"CL1":[list of dictionary rows with CL1]}}

        An index allows you to select rows much faster.

        :param t: Template representing a where clause/
        :param idx_name: Name of index to use.
        :param fields: Fields to return. #deciding not to push
        :return: Matching tuples.
        """
        result = []
        rows = []
        columns = self.__get_column_names__()
        indexes = self.__indexes__
        no_of_rows = 0

        for key in list(indexes.keys()):
            if key.index_name == idx_name:
                for keystring in indexes[key].keys():
                    rows.append(indexes[key].get(keystring))

                for i in range(0,len(rows)):
                    r = rows[i][0]
                    if self.matches_template(r, t):
                        if fields is None:
                            new_r = self.project(r, fields=None)
                            result.append(new_r)
                        else:
                            new_r = self.project(r, fields)
                            result.append(new_r)

        return result


    def __find_by_template__(self, template, fields=None, limit=None, offset=None):
        """
        # 1. Validate the template values relative to the defined columns.
        # 2. Determine if there is an applicable index, and call __find_by_template_index__ if one exists.
        # 3. Call __find_by_template_scan__ if not applicable index.

        Implementation is provided but you will need to complete the methods that __find_by_template__() calls
        :param template: Dictionary. The template that you search by
        :param fields: Fields that you want to return for the table
        :param limit: limit is not supported
        :param offset: offset is not supported
        :return: returns new list of rows that have the template and the fields applied
        """

        try:
            indexes = self.__indexes__
        except:
            indexes = None

        if indexes is None or indexes == {}:  # also should mean a derived table or some other issue
            result_rows = self.__find_by_template_scan__(template, fields)
            return result_rows

        else:
            result_index, count = self.__get_access_path__(template)
            if result_index is not None:
                result_rows = self.__find_by_template_index__(template, result_index, fields)
                return result_rows
            else:
                result_rows = self.__find_by_template_scan__(template, fields)
                return result_rows


    def dumb_join(self, right_r, on_fields, where_template=None, project_fields=None):
        """
        Implements a 'dumb' JOIN on two CSV Tables. Support equi-join only on a list of common
        columns names.

        No optimizations and is just straightforward iteration.
        Use this method as some general guidance for when you implement __smart_join__()

        Implementation is provided.
        :param right_r: The right table, or second input table.
        :param on_fields: A list of common fields used for the equi-join.
        :param where_template: Select template to apply to the result to determine what to return.
        :param project_fields: List of fields to return from the result.
        :return: CSVTable object that is the joined and filtered rows
        """
        start = time.time()
        left_r = self
        left_rows = left_r.__get_row_list__()
        right_rows = right_r.__get_row_list__()
        result_rows = []

        left_rows_processed = 0
        for lr in left_rows:
            on_template = self.__get_on_template__(lr, on_fields)
            for rr in right_rows:
                if self.matches_template(rr, on_template):
                    new_r = {**lr, **rr}  # appends two dictionaries together
                    result_rows.append(new_r)
            left_rows_processed += 1
            if left_rows_processed % 10 == 0:
                print("Processed", left_rows_processed, "left rows.")

        join_result = self.__table_from_rows__("JOIN:" + left_r.__table_name__ + ":" + right_r.__table_name__, result_rows)
        result = join_result.__find_by_template__(where_template, project_fields)  # join table won't have indexes so it will use template_scan
        final_table = self.__table_from_rows__("Filtered JOIN(" + left_r.__table_name__ + "," + right_r.__table_name__ + ")", result)

        end = time.time()
        self.dumb_time = end - start
        return final_table

    def __smart_join__(self, right_r, on_fields, where_template=None, project_fields=None):
        """
        Implements a JOIN on two CSV Tables. Support equi-join only on a list of common
        columns names.

        If no optimizations are possible, do a simple nested loop join and then apply where_clause and project to result
        At least two vastly different optimizations are possible, you must choose two different optimizations
            and implement them.

        :param right_r: The right table, or second input table.
        :param on_fields: A list of common fields used for the equi-join.
        :param where_template: Select template to apply to the result to determine what to return.
        :param project_fields: List of fields to return from the result.
        :return: List of dictionary elements, each representing a row.
        """
        # ************************ TO DO ***************************

        #Steps for Optimization
        # 1. Find the sub-template of the where_template for the table (self) and the input table (right_r) to help in determining the required rows faster
        # 2. Call find_by_template() using the sub_template for each table to use an applicable index (if any) to select rows faster
        # 3. Check if the new left table and the new right table (the returned rows from find_by_template()) are not None
        #   3.1. if they are not None (i.e. each is a list of rows):
        #        -> join the new left table and the right table by creating nested loop
        #        to find row matches between them with the on condition template
        #        -> call the project() function to implement the select clause on the desired
        #        project_fields from the list of rows (i.e. newly generated table as a  result of join)
        #        -> return final table after filtering
        #   3.2. if the new left table is None and/or the new right table is None:
        #        -> optimization is not possible, thus the self table calls dumb join on the input table
        start = time.time()
        left_r = self
        left_sub_template = left_r.__get_sub_where_template__(where_template)
        right_sub_template = right_r.__get_sub_where_template__(where_template)

        new_left = left_r.__find_by_template__(left_sub_template)
        new_right = right_r.__find_by_template__(right_sub_template)

        if new_right is not None and new_left is not None:
            result_rows = []
            left_rows_processed = 0
            for lr in new_left:
                on_template = self.__get_on_template__(lr[0], on_fields)
                for rr in new_right:
                    if self.matches_template(rr, on_template):
                        new_r = {**lr[0], **rr}  # appends two dictionaries together
                        result_rows.append(new_r)
                left_rows_processed += 1
                print("Processed", left_rows_processed, "left rows.")


            join_result = self.__table_from_rows__("JOIN:" + left_r.__table_name__ + ":" + right_r.__table_name__,
                                                   result_rows)
            result = join_result.project(result_rows, fields=project_fields)
            final_table = self.__table_from_rows__(
                "Filtered JOIN(" + self.__table_name__ + "," + right_r.__table_name__ + ")", result)
        else:
            final_table = left_r.dumb_join(right_r, on_fields, where_template, project_fields)

        end = time.time()
        self.smart_time = end - start
        return final_table

    def __get_sub_where_template__(self, where_template):
        """
        Gets the where template fields that are applicable to the table
        This means that someone could technically pass a template that references fields that do not exist in the table
        Not a real sql thing because sql would throw an error but we have implemented it for error checking.

        Implementation for __get_sub_where_template__ is provided.
        :param where_template:
        :return: where template dictionary
        """
        sub_template = {}
        table_columns = self.__get_column_names__()

        # Go through each key in the where template and see if it is a column in the table
        for key_name in where_template.keys():
            if key_name in table_columns:
                sub_template[key_name] = where_template[key_name]

        return sub_template

    def __get_on_template__(self, row, on_fields):
        """
        Gets the on clause as a template for an individual row to easily compare to other table

        Implementation is provided.
        :param row: the row that you are creating the template
        :param on_fields: list of fields to join ex: ['playerID', 'teamID']
        :return:
        """
        template = {}
        for field in on_fields:
            value = row[field]
            template[field] = value

        return template

    def __get_row_list__(self):
        """
        Gets all rows of the table

        Implementation is provided.
        :return: List of row dictionaries
        """
        return self.__rows__


    def __table_from_rows__(self, table_name, rows):
        """
        Creates a new instance of CSVTable with a table name and rows passed through (from the join)

        Implementation is provided.
        :param table_name: String that is the name of the table
        :param rows: a list of dictionaries that contain row info for the table
        :return: the new table
        """
        new_table = CSVTable(table_name, False)
        new_table.__rows__ = rows
        new_table.__description__ = None

        return new_table

    # Table formatting method from:
    # https://stackoverflow.com/questions/40056747/print-a-list-of-dictionaries-in-table-form
    def __str__(self):
        data = self.__rows__
        rows = []
        if type(data[0]) == dict:
            header = data[0].keys()
            rows = [x.values() for x in data]
        else:
            header = data[0][0].keys()
            for list in data:
                r = list[0]
                rows.append(r.values())

        return tabulate.tabulate(rows, header, tablefmt='grid')

    def insert(self, r):
        raise DataTableExceptions.DataTableException(
            code=DataTableExceptions.DataTableException.not_implemented,
            message="Insert not implemented"
        )

    def delete(self, t):
        raise DataTableExceptions.DataTableException(
            code=DataTableExceptions.DataTableException.not_implemented,
            message="Delete not implemented"
        )

    def update(self, t, change_values):
        raise DataTableExceptions.DataTableException(
            code=DataTableExceptions.DataTableException.not_implemented,
            message="Updated not implemented"
        )


