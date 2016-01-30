from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect('demo')

session.execute("insert into table1 (name, id) values ('James', 10)")

result = session.execute("select * from table1")

print result