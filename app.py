"""
Lucas Ancieta
lancieta@caltech.edu

This is a supporting program for the Climate Null Study database,
a study on the effect of, and how to resolve nulls in Climate data that
focuses on machine learning datasets and the impact of Nulls in their data.
This program also implements various strategies for Null removal via calculation
and mirroring values from other sources as well as Null estimation via mean, 
regression, and K nearest neighbor.
"""

#Make sure you have these installed with pip3 if needed
import sys  # to print error messages to sys.stderr
import mysql.connector
# To get error codes from the connector, useful for user-friendly
# error-handling
import mysql.connector.errorcode as errorcode

# To handle sql tables as dataframes
import pandas as pd
from sqlalchemy import create_engine

# To preform data imputation
from sklearn.impute import KNNImputer, SimpleImputer
import numpy as np
from scipy.sparse import lil_matrix

# To export tables as CSVs
import os

# Debugging flag to print errors when debugging that shouldn't be visible
# to an actual client. ***Set to False when done testing.***
DEBUG = False

HOST = '' # host name
USER = '' # SQL username
PORT = '' # Find port in MAMP or MySQL Workbench GUI or with 
          # SHOW VARIABLES WHERE variable_name LIKE 'port'. This may change!
PASSWORD = '' # SQL password
DATABASE = '' # replace this with your database name


# ----------------------------------------------------------------------
# SQL Utility Functions
# ----------------------------------------------------------------------
def get_conn():
    """"
    Returns a connected MySQL connector instance, if connection is successful.
    If unsuccessful, exits.
    """
    try:
        conn = mysql.connector.connect(
          host=HOST,
          user=USER,
          port=PORT,
          password=PASSWORD,
          database=DATABASE 
        )
        print('Successfully connected.')
        return conn
    except mysql.connector.Error as err:
        # Remember that this is specific to _database_ users, not
        # application users. So is probably irrelevant to a client in your
        # simulated program. Their user information would be in a users table
        # specific to your database; hence the DEBUG use.
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR and DEBUG:
            sys.stderr('Incorrect username or password when connecting to DB.')
        elif err.errno == errorcode.ER_BAD_DB_ERROR and DEBUG:
            sys.stderr('Database does not exist.')
        elif DEBUG:
            sys.stderr(err)
        else:
            # A fine catchall client-facing message.
            sys.stderr('An error occurred, please contact the administrator.')
        sys.exit(1)

# ----------------------------------------------------------------------
# Functions for Command-Line Options/Query Execution
# ----------------------------------------------------------------------
def example_query():
    param1 = ''
    cursor = conn.cursor()
    # Remember to pass arguments as a tuple like so to prevent SQL
    # injection.
    sql = 'SELECT col1 FROM table WHERE col2 = \'%s\';' % (param1, )
    try:
        cursor.execute(sql)
        # row = cursor.fetchone()
        rows = cursor.fetchall()
        for row in rows:
            (col1val) = (row) # tuple unpacking!
            # do stuff with row data
    except mysql.connector.Error as err:
        # If you're testing, it's helpful to see more details printed.
        if DEBUG:
            sys.stderr(err)
            sys.exit(1)
        else:
            # TODO: Please actually replace this :) 
            sys.stderr('An error occurred, give something useful for clients...')



# ----------------------------------------------------------------------
# Functions for Logging Users In
# ----------------------------------------------------------------------
# Note: There's a distinction between database users (admin and client)
# and application users (e.g. members registered to a store). You can
# choose how to implement these depending on whether you have app.py or
# app-client.py vs. app-admin.py (in which case you don't need to
# support any prompt functionality to conditionally login to the sql database)


# ----------------------------------------------------------------------
# Command-Line Functionality
# ----------------------------------------------------------------------
def df_from_name(name):
    if name == 'aggregate':
        return aggregate_df
    
    return pd.read_sql(f'SELECT * FROM `{name}`', con = db_connection) 

def df_to_table(df, name):
    df.to_sql(name, con = db_connection, if_exists='replace')

def list_imputables():
    df = df_from_name('imputables')
    return df['name'].to_list()

def imputables_add(name):
    try:
        mySql_insert_query = f"""
            INSERT INTO imputables (name)
            VALUES ('{name}') """

        cursor = conn.cursor()
        cursor.execute(mySql_insert_query)
        conn.commit()
        cursor.close()

    except mysql.connector.Error as error:
        print("Failed to insert record into imputable table {}".format(error))
    

def df_to_imputable_table(df, name):
    df_to_table(df, name)
    imputables_add(name)
    print()
    print(f"New imputable table {name} created!")
    print()

def print_lst(lst):
    print("----------------")
    for x in lst:
        print(x)
    print("----------------")

def lst_select(lst):
    print_lst(lst)
    name = input('Select one of the above: ')
    while name not in lst:
        print()
        print(f"{name} is not a valid option")

        print_lst(lst)

        name = input('Select one of the above: ')
    
    return name

def df_to_lil(df):
    """
    Converts a sparse pandas data frame to sparse scipy csr_matrix.
    :param df: pandas data frame
    :return: csr_matrix
    """
    arr = lil_matrix(df.shape, dtype=np.float32)
    for i, col in enumerate(df.columns):
        ix = df[col] != 0
        arr[np.where(ix), i] = 1

    return arr.tocsr()

def categorical_impute(data, imp):
    categorical_columns = []
    numeric_columns = []
    for c in data.columns:
        #check if there are any strings in column
        if data[c].map(type).eq(str).any(): 
            categorical_columns.append(c)
        else:
            numeric_columns.append(c)

    #create two DataFrames, one for each data type
    data_numeric = data[numeric_columns]
    data_categorical = pd.DataFrame(data[categorical_columns])

    #only apply imputer to numeric columns
    data_numeric = pd.DataFrame(imp.fit_transform(data_numeric), 
                                columns = data_numeric.columns, 
                                index=data_numeric.index)

    #join the two masked dataframes back together
    return pd.concat([data_numeric, data_categorical], axis = 1)

def one_hot_impute(data, imp):
    categorical_columns = []
    numeric_columns = []
    for c in data.columns:
        #check if there are any strings in column
        if data[c].map(type).eq(str).any(): 
            categorical_columns.append(c)
        else:
            numeric_columns.append(c)

    #create two DataFrames, one for each data type
    data_numeric = data[numeric_columns]
    data_categorical = pd.DataFrame(data[categorical_columns])

    one_hot_data = pd.get_dummies(data_categorical, sparse=True)
    lil_one_hot = df_to_lil(one_hot_data)
    one_hot_fit = pd.DataFrame(imp.fit_transform(lil_one_hot))   

    #apply imputer to numeric columns
    data_numeric = pd.DataFrame(imp.fit_transform(data_numeric), 
                                columns = data_numeric.columns, 
                                index=data_numeric.index)

    #join the two masked dataframes back together
    return pd.concat([data_numeric, data_categorical], axis = 1)

def impute(data, imp, one_hot):
    #Filter out station and datetime as they will cause problems
    st_dt = data[['station', 'date']]
    new_data = data.drop(['station', 'date'], axis=1)
    
    if one_hot:
        new_data = one_hot_impute(new_data, imp)
    else:
        new_data = categorical_impute(new_data, imp)

    return pd.concat([st_dt, new_data], axis = 1)

def analysis_display(df, table_name):
    pd.set_option('display.max_rows', None)
    null_cnt = df.isnull().sum()
    null_pcnt = (1 - df.count() / len(df))
    types = df.dtypes
    print()
    print(f"{table_name} analysis:")
    print("Column Name     |  Null Count | Null Percent | Column Type")
    print("----------------------------------------------------------")
    print(pd.concat([null_cnt, null_pcnt, types], axis = 1))
    print("----------------------------------------------------------")
    print()
    pd.set_option('display.max_rows', 20)

def ui_init():
    print("Interface Loading...")
    db_connection_str = f'mysql+pymysql://{USER}:{PASSWORD}@{HOST}/{DATABASE}'

    global db_connection
    db_connection = create_engine(db_connection_str)

    global aggregate_df
    aggregate_df = pd.read_sql(f'SELECT * FROM aggregate', con = db_connection)

    global imputables
    imputables = ['aggregate']

    show_options(aggregate_df, 'aggregate')

def show_options(df, table_name):
    """
    Displays options users can choose in the application
    """
    print()
    print(f'<Currently accessing {table_name}>')
    print('What would you like to do?')
    print('(analyze)  - Analyzes the prevalence of nulls in the database')
    if table_name == 'aggregate':
        print('(pf)       - Purge and Flag')
    print('(impute)   - Applies different methods of mitigating null data')
    print('(list)     - Lists available tables')
    print('(select)   - Switches to selected table')
    print('(export)   - Exports the selected table as a csv')
    print('(q)        - quit')
    print()
    ans = input('Enter an option: ').lower()

    if ans == 'q':
        quit_ui()

    elif ans == 'analyze':
        analysis_display(df, table_name)
        input('Done? (press enter): ')
        show_options(df, table_name)

    elif ans == 'pf' and table_name == 'aggregate':
        print("Purge and Flag is a special function available only for the aggregate table.")
        print("This function will create a new imputable table that can be used with (impute)")

        new_df = df.copy(deep = True)
        new_name = ""
        
        pfing = True
        while pfing:
            analysis_display(new_df, new_name)
            print("(purge)       - Allows you to purge individual columns from the table")
            print("(purge_per)   - Allows you to purge all columns containing >=x% Nulls")
            print("(flag)        - Allows you to transform individual columns into \"contains data\" boolean columns")
            print("(flag_per)    - Allows you to flag all columns containing >=x% Nulls")
            print("(exit)        - Exits the pf process and creates the new imputable table")
            print()
            ans = input('Enter an option: ').lower()
            
            if ans == 'purge':
                col = lst_select(list(new_df.columns))
                new_df = new_df.drop(columns = [col])
                new_name += f"p-{col}_"
            
            elif ans == 'purge_per':
                print()
                per = input('Enter a threshhold percent between 0 and 100: ').lower()
                while not (100>= float(per) >=0):
                    print()
                    print("invalid percentage")
                    per = input('Enter a threshhold percent between 0 and 100: ').lower()
                
                # Below code gives percentage of null in every column
                null_percentage = new_df.isnull().sum()/new_df.shape[0]*100

                # Below code gives list of columns having more than 60% null
                cols_to_drop = null_percentage[null_percentage>float(per)].keys()

                new_df = new_df.drop(cols_to_drop, axis=1)
                new_name += f"p-{per}_"

            elif ans == 'flag':
                col = lst_select(list(new_df.columns))
                new_df[[col]] = new_df[[col]].notnull().astype(int)
                new_name += f"f-{col}_"
            
            elif ans == 'flag_per':
                print()
                per = input('Enter a threshhold percent between 0 and 100: ').lower()
                while not (100>= float(per) >=0):
                    print()
                    print("invalid percentage")
                    per = input('Enter a threshhold percent between 0 and 100: ').lower()
                
                # Below code gives percentage of null in every column
                null_percentage = new_df.isnull().sum()/new_df.shape[0]*100

                # Below code gives list of columns having more than 60% null
                cols_to_flag = null_percentage[null_percentage>float(per)].keys()

                new_df[cols_to_flag] = new_df[cols_to_flag].notnull().astype(int)
                new_name += f"f-{per}_"

            elif ans == 'exit':
                print("working...")
                pfing = False

            else:
                print('Invalid option')
                print()

        if new_name == "":
            show_options(aggregate_df, "aggregate")
        else:
            df_to_imputable_table(new_df, new_name[:-1])
            show_options(new_df, new_name[:-1])


    elif ans == 'impute':
        print()
        if table_name in list_imputables():
            print(f'<Currently accessing {table_name}>')
        else: 
            print(f'<Currently accessing {table_name}>')
            print("Imputation can only be performed on an imputable table")

            ans = input('Switch? (y/n): ').lower()

            if ans == 'y':
                name = lst_select(list_imputables())
                show_options(df_from_name(name), name)

            else:
                show_options(df, table_name)
            
        print("What Null imputation technique would you like to apply?")
        print('(mean)           - Replaces nulls with the mean of the column')
        print('(most_frequent)  - Replaces nulls with the most frequent value of the column')
        print('(median)         - Replaces nulls with the median value of the column')
        print('(nearest)        - Replaces nulls with k-nearest neighbors approach')
        print('(exit)           - Cancels operation and returns to main menu')
        ans = input('Enter an option: ').lower()
        print('Working...')

        if ans == 'exit':
            show_options(df, table_name)
        
        elif ans == 'mean':
            one_hot = False
            ans = input("Use one-hot encoding? (y/n): ").lower()
            if ans == 'y':
                one_hot = True
            print()
            imp = SimpleImputer(missing_values = np.NaN, strategy='mean')
            new_df = impute(df, imp, one_hot)
            
            new_name = table_name + "_mean-imputed"
            if one_hot:
                new_name += "_one-hot"

            df_to_table(new_df, new_name)            
            show_options(new_df, new_name)
        
        elif ans == 'most_frequent':
            one_hot = False
            ans = input("Use one-hot encoding? (y/n): ").lower()
            if ans == 'y':
                one_hot = True
            print()
            imp = SimpleImputer(missing_values = np.NaN, strategy='most_frequent')
            new_df = impute(df, imp, one_hot)

            new_name = table_name + "_most-frequent-imputed"
            if one_hot:
                new_name += "_one-hot"

            df_to_table(new_df, new_name)            
            show_options(new_df, new_name)

        elif ans == 'median':
            one_hot = False
            ans = input("Use one-hot encoding? (y/n): ").lower()
            if ans == 'y':
                one_hot = True
            print()
            imp = SimpleImputer(missing_values = np.NaN, strategy='median')
            new_df = impute(df, imp, one_hot)
            
            new_name = table_name + "_median-imputed"
            if one_hot:
                new_name += "_one-hot"

            df_to_table(new_df, new_name)            
            show_options(new_df, new_name)

        elif ans == 'nearest':
            ans = input('Enter a k: ').lower()

            imp = KNNImputer(missing_values = np.NaN, n_neighbors=int(ans), weights="uniform")
            new_df = impute(df, imp, False)
            
            new_name = table_name + f"_{int(ans)}-nearest-imputed"

            df_to_table(new_df, new_name)            
            show_options(new_df, new_name)
        

    elif ans == 'list' or ans == 'select':
        mycursor = conn.cursor()
        mycursor.execute("Show tables;")
        myresult = mycursor.fetchall()
        result_string_arr = []

        print("----------------")
        for x in myresult:
            x = str(x)
            x = x.strip("(',)")
            if x not in ['agg_blackhole', 'expeditions', 'sensors', 'imputables']:
                result_string_arr.append(x)
                print(x)
        print("----------------")

        if ans == 'select':
            name = lst_select(result_string_arr)

            if name == table_name:
                show_options(df, table_name)

            else:
                print('Working...')
                show_options(df_from_name(name), name)

        else:
            input('Done? (press enter): ')
            show_options(df, table_name)

    elif ans == 'export':
        print()
        print(f"Exporting {table_name}.csv to csv_output...")
        os.makedirs('csv_output', exist_ok=True)
        df.to_csv(f'csv_output/{table_name}.csv')
        show_options(df, table_name)

    else:
        print('Invalid option')
        show_options(df, table_name)



# Another example of where we allow you to choose to support admin vs. 
# client features  in the same program, or
# separate the two as different app_client.py and app_admin.py programs 
# using the same database.
def show_admin_options():
    """
    Displays options specific for admins, such as adding new data <x>,
    modifying <x> based on a given id, removing <x>, etc.
    """
    print('What would you like to do? ')
    print('  (x) - something nifty for admins to do')
    print('  (x) - another nifty thing')
    print('  (x) - yet another nifty thing')
    print('  (x) - more nifty things!')
    print('  (q) - quit')
    print()
    ans = input('Enter an option: ').lower()
    if ans == 'q':
        quit_ui()
    elif ans == '':
        pass


def quit_ui():
    """
    Quits the program, printing a good bye message to the user.
    """
    print('Good bye!')
    exit()


def main():
    """
    Main function for starting things up.
    """
    ui_init()


if __name__ == '__main__':
    # This conn is a global object that other functions can access.
    # You'll need to use cursor = conn.cursor() each time you are
    # about to execute a query with cursor.execute(<sqlquery>)
    conn = get_conn()
    main()
