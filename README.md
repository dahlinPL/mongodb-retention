mongodb retention script
========================

Script to:
1. remove old data from all database collections (performed on primary)
2. rebuild indexes on slaves (performed on secondaries)

To make it work, user in database should be created with **dbAdmin** role
in given database.

To check options, run _**python mongodb_retention.py -h**_