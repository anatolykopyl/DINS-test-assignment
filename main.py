import csv
import datetime
import time
import MySQLdb

from anomaly import is_anomaly

TIME_SPAN = 900  # 15 minutes
result = {}
with open('raw_data2.csv', 'rt', encoding="UTF-8") as csvfile:
    reader = csv.reader(csvfile, quotechar='"')

    for row in reader:
        if row[0] != "ts":
            timestamp = time.mktime(datetime.datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S,%f").timetuple())  # generating a unix timestamp

            name = row[1] + "*" + row[2]  # combining api_name and http_method into a pair name

            if 'start_time2' not in locals():
                start_time = timestamp
            start_time = int(min(start_time, timestamp))  # earliest request
            if 'end_time' not in locals():
                end_time = timestamp
            end_time = int(max(end_time, timestamp))  # latest request

            if name not in result:
                result[name] = []

    for t in range(start_time, end_time, TIME_SPAN):
        result[name].append([t, 0, 0])  # [timestamp, num of requests, is anomaly]

    csvfile.seek(0)

    for row in reader:
        if (row[3])[:1] == "5":
            timestamp = time.mktime(datetime.datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S,%f").timetuple())

            for name in result:
                for item in result[name]:
                    if item[0] <= timestamp < item[0] + TIME_SPAN:
                        item[1] += 1

is_anomaly(result)

try:
    connection = MySQLdb.connect(host="127.0.0.1", user="user1", passwd="testserver", db="mydb")
    cursor = connection.cursor()

    cursor.execute("SHOW TABLES LIKE 'result'")
    if not cursor.fetchone():  # check if table already exists
        cursor.execute("CREATE TABLE result ( timeframe_start TIMESTAMP, api_name TEXT, http_method TEXT, \
                        count_http_code_5xx INT, is_anomaly TINYINT(1) )")

    for name in result:
        for item in result[name]:
            cursor.execute("INSERT INTO result (timeframe_start, api_name, http_method, count_http_code_5xx, is_anomaly)"
                           "VALUES (\"{0}\", \"{1}\", \"{2}\", {3}, {4})"
                           .format(str(datetime.datetime.fromtimestamp(item[0]).strftime("%Y-%m-%d %H:%M:%S")),
                                   name.split("*")[0], name[(name.find("*")+1):], item[1], item[2]))

    connection.commit()
    cursor.close()
    connection.close()
except MySQLdb.Error as err:
    print("Connection error: {}".format(err))
