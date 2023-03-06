import boto3 as aws
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal
import pandas as pd
#from pprint import pprint

class Connection:
    #Credentials need to be set up beforehand
    def __init__(self):
        self._dynamodb = aws.resource('dynamodb')

    def get_table(self, name: str):
        return Table(self._dynamodb.Table(name))
    
    def get_tables(self):
        tables = self._dynamodb.tables.all()
        return {table.name: Table(table) for table in tables}

class Table:
    #Use on this module only
    def __init__(self, dynamo_table):
        self._dynamo_table = dynamo_table

    def key_schema(self):
        schema = self._dynamo_table.key_schema
        schema = {key['KeyType'] : key['AttributeName'] for key in schema}
        return schema

    def put_item(self, item: dict):
        item = {k: _convert_float(v) for k, v in item.items()}
        return self._dynamo_table.put_item(Item=item)
    
    def get_item(self, key: dict):
        return self._dynamo_table.get_item(Key=key)['Item']

    def set_attributes(self, key: dict, attr: dict):
        update_expression = [f"{name} = :val_{name}" for name, value in attr.items()]
        update_expression = f"SET {', '.join(update_expression)}"
        expression_values = {f":val_{name}": value for name, value in attr.items()}
        #print(update_expression)
        return self._dynamo_table.update_item(
            Key=key,
            UpdateExpression= update_expression,
            ExpressionAttributeValues=expression_values, 
            ReturnValues='UPDATED_NEW'
        )
    
    def delete_item(self, key: dict):
        return self._dynamo_table.delete_item(Key=key)

    def query(self, key_expr, filter_expr=None, index=None,  as_df=True):
        query_args = {
            'KeyConditionExpression': key_expr
        }
        if filter_expr is not None:
            query_args['FilterExpression'] = filter_expr
        if index is not None:
            query_args['IndexName'] = index

        response = self._dynamo_table.query(**query_args)
        items = response['Items']

        while 'LastEvaluatedKey' in response:
            query_args['ExclusiveStartKey'] = response['LastEvaluatedKey']
            response = self._dynamo_table.query(**query_args)
            items.extend(response['Items'])

        if not as_df:
            return items
        return to_df(items, self.key_schema())
    
    def scan(self, filter_expr=None, as_df=True):
        query_args = dict()
        if filter_expr is not None:
            query_args['FilterExpression'] = filter_expr

        response = self._dynamo_table.scan(**query_args)
        items = response['Items']

        while 'LastEvaluatedKey' in response:
            query_args['ExclusiveStartKey'] = response['LastEvaluatedKey']
            response = self._dynamo_table.scan(**query_args)
            items.extend(response['Items'])

        if not as_df:
            return items
        return to_df(items, self.key_schema())
        
def to_df(query_results, key_schema={}):
    if not key_schema:
        return pd.DataFrame(query_results)
    df_indices = [key_schema['HASH']]
    if 'RANGE' in key_schema:
        df_indices.append(key_schema['RANGE'])
    return pd.DataFrame(query_results).set_index(df_indices)


def _convert_float(x):
    if isinstance(x, float):
        return Decimal(x)
    else:
        return x


#conn = Connection()
#table = conn.get_table('Fornecedores_Produtos')
#pprint(table.query(
#    Key('fornecedor').eq(0) & Key('produto').eq(4)
#))