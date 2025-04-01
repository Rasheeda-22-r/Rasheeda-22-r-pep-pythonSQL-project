import csv
import sqlite3
import pandas as pd
import os

# Connect to the SQLite in-memory database
conn = sqlite3.connect(':memory:')

# A cursor object to execute SQL commands
cursor = conn.cursor()


def main():

    # users table
    cursor.execute('''CREATE TABLE IF NOT EXISTS users (
                        userId INTEGER PRIMARY KEY AUTOINCREMENT,
                        firstName TEXT,
                        lastName TEXT
                      )'''
                   )

    # callLogs table (with FK to users table)
    cursor.execute('''CREATE TABLE IF NOT EXISTS callLogs (
        callId INTEGER PRIMARY KEY,
        phoneNumber TEXT,
        startTime INTEGER,
        endTime INTEGER,
        direction TEXT,
        userId INTEGER,
        FOREIGN KEY (userId) REFERENCES users(userId)
    )''')

    # You will implement these methods below. They just print TO-DO messages for now.
    
    base_dir = os.path.dirname(os.path.abspath(__file__))
    resources_dir = os.path.join(base_dir, '../../resources')
    load_and_clean_users(os.path.join(resources_dir, 'users.csv'))
    load_and_clean_call_logs(os.path.join(resources_dir, 'callLogs.csv'))
    write_user_analytics(os.path.join(resources_dir, 'userAnalytics.csv'))
    write_ordered_calls(os.path.join(resources_dir, 'orderedCalls.csv'))

    # Helper method that prints the contents of the users and callLogs tables. Uncomment to see data.
    select_from_users_and_call_logs()

    # Close the cursor and connection. main function ends here.
    cursor.close()
    conn.close()


# TODO: Implement the following 4 functions. The functions must pass the unit tests to complete the project.


# This function will load the users.csv file into the users table, discarding any records with incomplete data
def load_and_clean_users(file_path):
    df = pd.read_csv(file_path , error_bad_lines = 0)
    df.dropna(inplace=True)
    df_1 = df[df['firstName'].str.strip() != '']
    df = df_1[df_1['lastName'].str.strip() != '']
    #userid_counter = 1
    for _, row in df.iterrows():
        #print(row)
        cursor.execute('''INSERT INTO users (firstname, lastname) VALUES (?, ?)''', (row['firstName'], row['lastName']))
        #userid_counter+=1


# This function will load the callLogs.csv file into the callLogs table, discarding any records with incomplete data
def load_and_clean_call_logs(file_path):
    df = pd.read_csv(file_path , error_bad_lines = 0)
    df.dropna(inplace=True)
    callid_counter = 1
    for _, row in df.iterrows():
        cursor.execute('''INSERT INTO callLogs (callId,phoneNumber,startTime,endTime,direction,userId) VALUES (?, ?, ?, ?, ?, ?)''', (callid_counter, row['phoneNumber'],row['startTime'],row['endTime'],row['direction'],row['userId']))
        callid_counter+=1


# This function will write analytics data to testUserAnalytics.csv - average call time, and number of calls per user.
# You must save records consisting of each userId, avgDuration, and numCalls
# example: 1,105.0,4 - where 1 is the userId, 105.0 is the avgDuration, and 4 is the numCalls.
def write_user_analytics(csv_file_path):
    cursor.execute("select * FROM callLogs")
    rows = cursor.fetchall()
    analyticsDict, numCalls = {}, {}
    for row in rows:
        userId, startTime, endTime = row[5], row[2], row[3]
        if(userId in analyticsDict):
            analyticsDict[userId] = (analyticsDict[userId]+ (endTime - startTime))
            numCalls[userId] += 1
        else:
            analyticsDict[userId] = endTime - startTime
            numCalls[userId] = 1
    with open(csv_file_path, mode ='w', newline='') as file:
        writer = csv.writer(file)
        columns = ['userId', 'avgDuration', 'numCalls']
        writer.writerow(columns)
        for userId in analyticsDict:
            writer.writerow([userId,analyticsDict[userId]/numCalls[userId],numCalls[userId]])




# This function will write the callLogs ordered by userId, then start time.
# Then, write the ordered callLogs to orderedCalls.csv
def write_ordered_calls(csv_file_path):
    cursor.execute("select callId,phoneNumber,startTime,endTime,direction,userId FROM callLogs order by userId,startTime")
    rows = cursor.fetchall()
    with open(csv_file_path, mode ='w', newline='') as file:
        writer = csv.writer(file)
        columns = [description[0] for description in cursor.description]
        writer.writerow(columns)
        writer.writerows(rows)



# No need to touch the functions below!------------------------------------------

# This function is for debugs/validation - uncomment the function invocation in main() to see the data in the database.
def select_from_users_and_call_logs():

    print()
    print("PRINTING DATA FROM USERS")
    print("-------------------------")

    # Select and print users data
    cursor.execute('''SELECT * FROM users''')
    for row in cursor:
        print(row)

    # new line
    print()
    print("PRINTING DATA FROM CALLLOGS")
    print("-------------------------")

    # Select and print callLogs data
    cursor.execute('''SELECT * FROM callLogs''')
    for row in cursor:
        print(row)


def return_cursor():
    return cursor


if __name__ == '__main__':
    main()