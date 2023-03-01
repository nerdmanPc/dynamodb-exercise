import pandas as pd
from pprint import pprint
from db_adapter import *

tables = {
    'Clientes': pd.read_csv('data/clientes.csv'),
    #'Produtos': pd.read_csv('data/produtos.csv'),
    #'Fornecedores': pd.read_csv('data/fornecedores.csv'),
    #'Lojas': pd.read_csv('data/lojas.csv'),
    #'Vendas': pd.read_csv('data/vendas.csv'),
    #'Fornecedores_Produtos': pd.read_csv('data/fornecedores_produtos.csv'),
    #'Vendas_Produtos': pd.read_csv('data/vendas_produtos.csv')
}

if __name__ == '__main__':
    conn = Connection()
    for name, contents in tables.items():
        table = conn.get_table(name)
        contents.columns = contents.columns.str.strip()

        for index, row in contents.iterrows():
            item = dict()
            for key, value in row.items():
                item[key] = value
            response = table.put_item(item)
            print(f'Table: {name}')
            pprint(response, sort_dicts=False)
