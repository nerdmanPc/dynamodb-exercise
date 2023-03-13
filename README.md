# Dynamodb Exercise

A python library to support accessing a DynamoDB database. It's implemented as a thin abstraction layer on top of Boto3.

The migration.py file is not a full migration. It assumes the tables were created correctly.

Most of the string data has a leading space. For exampla, the name "Fortaleza" may be stored as " Fortaleza"
