import sys
from cassandra.cluster import Cluster

INSERT_QUERY = "insert into airport_airline_departure (Origin, UniqueCarrier, AirlineID, DepDelayMinutes) values ('{}', '{}', '{}', {})"
KEYSPACE = "aviation"

def main():
    if len(sys.argv) != 2:
        print 'usage: python insert_cassandra.py <input-file-path>'

    filename = sys.argv[1]

    cluster = Cluster()
    session = cluster.connect(KEYSPACE)

    for line in open(filename):
        line = line.rstrip()
        row = line.split("\t")

        insert_query = INSERT_QUERY.format(row[0], row[1], row[2], row[3])
        session.execute(insert_query);

if __name__ == '__main__':
    main()