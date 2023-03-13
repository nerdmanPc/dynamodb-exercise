# Dynamodb Exercise

A python library to support accessing a DynamoDB database. It's implemented as a thin abstraction layer on top of Boto3.

The library is in the db_adapter.py file and you can see the intend usage in example.ipynb.

The migration.py file is not a full migration. It assumes the tables were created correctly.

Most of the string data has a leading space. For exampla, the name "Fortaleza" may be stored as " Fortaleza"
