import os
import csv
import psycopg2
import sys

base_dir = os.path.dirname(os.path.realpath(__file__))
input_file = os.path.join(base_dir, 'input.csv')
output_file = os.path.join(base_dir, 'output.csv')

con = None

def read_csv(input_file):
    with open(input_file) as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        records = []
        records.append(reader.fieldnames)
        for row in reader:
            records.append(row)

        return records


def output_csv(output_file, fieldnames, result):
    print(fieldnames)
    with open(output_file, 'w', newline='') as output:
        writer = csv.DictWriter(output, fieldnames=fieldnames, delimiter=';', quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for row in result:
            writer.writerow(row)


records = read_csv(input_file)
fieldnames = records.pop(0)
fieldnames.append('Movie-Id')
fieldnames.append('Watch-Url')

print(fieldnames)

try:
    con = psycopg2.connect(database='upload', user='upload')

    for record in records:
        if record['Order-ID'] is not None:
            print ('Processing Order-ID %s' % record['Order-ID'])
            cur = con.cursor()
            cur.execute("SELECT video_id from upload_ticket where token LIKE '%" + record['Order-ID'] + "%'")
            tickets = cur.fetchall()

            tokens = []
            for ticket in tickets:
                tokens.append(ticket[0])

            if len(tokens) > 1:
                print('Warning found multiple matches for token %s' % record['Order-ID'])
            record['Movie-Id'] = ', '.join(str(token) for token in tokens)
            if len(tokens) == 1:
                record['Watch-Url'] = 'http://www.myvideo.de/watch/' + str(tokens[0])
        else:
            print(record)

    output_csv(output_file, fieldnames, records)
except psycopg2.DatabaseError as e:
    print('Error %s' % e)
    sys.exit(1)


finally:

    if con:
        con.close()


