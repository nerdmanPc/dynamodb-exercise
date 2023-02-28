import boto3 as aws
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal
from pprint import pprint

class Connection:
    #Credentials need to be set up beforehand
    def __init__(self):
        self._dynamodb = aws.resource('dynamodb')

    def get_table(self, name: str):
        return Table(self._dynamodb.Table(name))

class Table:
    #Use on this module only
    def __init__(self, dynamo_table):
        self._dynamo_table = dynamo_table

    def put_item(self, item: dict):
        item = {k: _convert_float(v) for k, v in item.items()}
        return self._dynamo_table.put_item(Item=item)
    
    def get_item(self, key: dict):
        return self._dynamo_table.get_item(Key=key)['Item']

    def set_attributes(self, key: dict, attr: dict):
        update_expression = [f"{name} = :val_{name}" for name, value in attr.items()]
        update_expression = f"SET {', '.join(update_expression)}"
        expression_values = {f":val_{name}": value for name, value in attr.items()}
        print(update_expression)
        return self._dynamo_table.update_item(
            Key=key,
            UpdateExpression= update_expression,
            ExpressionAttributeValues=expression_values, 
            ReturnValues='UPDATED_NEW'
        )
    
    def delete_item(self, key: dict):
        return self._dynamo_table.delete_item(Key=key)

    def query(self, key_expr, filter_expr=None):
        query_args = {
            'KeyConditionExpression': key_expr
        }
        if filter_expr is not None:
            query_args['FilterExpression'] = filter_expr

        response = self._dynamo_table.query(**query_args)
        items = response['Items']

        while 'LastEvaluatedKey' in response:
            query_args['ExclusiveStartKey'] = response['LastEvaluatedKey']
            response = self._dynamo_table.query(**query_args)
            items.extend(response['Items'])

        return items
    
    def scan(self, filter_expr=None):
        query_args = dict()
        if filter_expr is not None:
            query_args['FilterExpression'] = filter_expr

        response = self._dynamo_table.scan(**query_args)
        items = response['Items']

        while 'LastEvaluatedKey' in response:
            query_args['ExclusiveStartKey'] = response['LastEvaluatedKey']
            response = self._dynamo_table.scan(**query_args)
            items.extend(response['Items'])

        return items

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