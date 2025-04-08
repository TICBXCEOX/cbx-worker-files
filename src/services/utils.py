from sqlalchemy import create_engine
from configs import *
from psycopg2 import connect

def get_db_connection():
    connection = connect(
        dbname=PG_DATABASE,
        host=PG_HOST,
        user=PG_USER,
        password=PG_PASSWORD
    )
    return connection

def get_engine():
    engine = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}")
    return engine

def format_json(arr: list, field: str, field_json: str, comparison_operator: str = '<>', logical_operator: str = 'or', is_str: bool = False):
    #field = "filtered.elem->>'id_client'"    
    #field_json = 'id_client'
    #for cli in entity.clients:
    #  xx += f"{field} <> '{cli['id_client']}'"
    
    ors = ""
    for val in arr:
        value = val[field_json]
        if val == "":
            continue
        if ors == "":
            ors += f" ({field} {comparison_operator} '{value}' " if is_str else f" ({field} {comparison_operator} {value} "
        else:
            ors += f" {logical_operator} {field} like '{value}' " if is_str else f" {logical_operator} {field} like {value} "
    return ors + ')' if ors != "" else ""
        
def format_in(field, values_arr, is_str = False):
    inn = ""
    for index, val in enumerate(values_arr):
        if val == "":
            continue
        if index == 0:
            inn += f" {field} in ('{val}'" if is_str else f" {field} in ({val}"
        else:
            inn += f",'{val}'" if is_str else f",{val}"
    return inn + ')' if inn != "" else "" 

    
    


    

def format_person_doc(doc: str):
    # remove dots and hyphens
    if doc:        
        new_doc = ''.join(filter(str.isdigit, str(doc)))
        return new_doc
    return ''

def is_number(value):
    if isinstance(value, bool):
        return False    
    if isinstance(value, int):
        value = int(value)
        return value
    if isinstance(value, float):
        value = float(value)
        return value
    try:
        float(value)
        return True
    except ValueError:
        return False