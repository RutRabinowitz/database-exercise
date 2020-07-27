import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Type
import os
import os.path
import db_api

# from dataclasses_json import dataclass_json

DB_ROOT = Path('db_files')


# @dataclass_json
@dataclass
class DBField(db_api.DBField):
    def __init__(self, name, type):
        self.name = name
        self.type = type


# @dataclass_json
@dataclass
class SelectionCriteria(db_api.SelectionCriteria):
    def __init__(self, field_name, operator, value):
        self.field_name = field_name
        self.operator = operator
        self.value = value

    def get_fun_name(self):
        fun_names = {
            '=': 'eq',
            '<': 'lt',
            '>': 'gt'
        }
        return fun_names[self.operator]


# @dataclass_json
@dataclass
class DBTable(db_api.DBTable):
    def __init__(self, name, fields, key_field_name):
        self.name = name
        self.fields = fields

        if key_field_name in self.get_fields_name():
            self.key_field_name = key_field_name
            with open(self.get_file_name(), 'w') as file:
                json.dump({}, file)
            return
        else:
            raise ValueError  # Primary key Error...

    def count(self) -> int:
        with open(self.name + '.json', 'r') as json_file:
            return len(json.load(json_file))

    def insert_record(self, values: Dict[str, Any]) -> None:
        with open(self.get_file_name(), 'r') as json_file:
            records = json.load(json_file)

            if str(values[self.key_field_name]) not in records:
                records[values[self.key_field_name]] = [str(values.get(key)) for key in \
                                                        self.get_fields_name() if key != self.key_field_name]
                self.write_to_file(records)
            else:
                raise TypeError

    def delete_record(self, key: Any) -> None:
        with open(self.get_file_name(), 'r') as json_file:
            records = json.load(json_file)

            if str(key) in records:
                del records[str(key)]
                self.write_to_file(records)
            else:
                raise ValueError

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        records = read(self.get_file_name())
        for record in records:
            for cr in criteria:
                boolean = True

                if cr.field_name == self.key_field_name:
                    fun_name = cr.get_fun_name()

                    if not getattr(self, fun_name)(cr, records[record]):
                        boolean = False
            if boolean:
                del records[record]
                self.write_to_file(records)

    def get_record(self, key: Any) -> Dict[str, Any]:
        with open(self.get_file_name(), 'r') as json_file:
            records = json.load(json_file)

            if str(key) in records:
                result = {self.key_field_name: key}
                for idx, field in enumerate(records[str(key)]):
                    result[self.fields[idx + 1].name] = field
                return result
            else:
                raise ValueError

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        with open(self.get_file_name(), 'r') as json_file:
            records = json.load(json_file)

            if str(key) in records:
                records[str(key)] = self.get_updated_record(str(key), values, records[str(key)])
                self.write_to_file(records)
            else:
                raise ValueError

    def query_table(self, criteria: List[SelectionCriteria]) \
            -> List[Dict[str, Any]]:
        records = read(self.get_file_name())
        result = []
        for cr in criteria:
            result += self.get_criteria_records(cr, records)
        return result

    def create_index(self, field_to_index: str) -> None:
        raise NotImplementedError

    def get_file_name(self):
        return self.name + ".json"

    def get_fields_name(self):
        return [field_name.name for field_name in self.fields]

    def write_to_file(self, records_dict):
        with open(self.get_file_name(), 'w') as json_file:
            json.dump(records_dict, json_file)

    def get_updated_record(self, key: Any, values: Dict[str, Any], record):
        updated_record = record
        for field in values:

            if field in self.get_fields_name():
                updated_record[self.get_fields_name().index(field) - 1] = values[field]
        return updated_record

    def eq(self, criteria, record):
        return str(record[self.get_fields_name().index(criteria.field_name) - 1]) == criteria.value

    def lt(self, criteria, record):
        return str(record[self.get_fields_name().index(criteria.field_name) - 1]) < criteria.value

    def gt(self, criteria, record):
        return str(record[self.get_fields_name().index(criteria.field_name) - 1]) > criteria.value

    def get_record_not_throw(self, criteria):
        try:
            return self.get_record(criteria.value)
        except ValueError:
            return []

    def get_criteria_records(self, criteria, records):

        if criteria.field_name == self.key_field_name:
            return self.get_record_not_throw(criteria)

        result = []
        if criteria.field_name in self.get_fields_name():
            for record in records:
                fun_name = criteria.get_fun_name()
                if getattr(self, fun_name)(criteria, records[record]):
                    result.append(self.get_record(record))
        return result


def write(file_name, data):
    with open(file_name, 'w') as json_file:
        json.dump(data, json_file)


def read(file_name):
    with open(file_name, 'r') as json_file:
        json_data = json.load(json_file)
    return json_data


# @dataclass_json
@dataclass
class DataBase(db_api.DataBase):
    def __init__(self):
        self.name = "my_db"
        self.file_name = "my_db.json"
        if not os.path.isfile(self.file_name):
            with open(self.file_name, 'w') as file:
                json.dump({}, file)

    def create_table(self,
                     table_name: str,
                     fields: List[DBField],
                     key_field_name: str):

        new_db = DBTable(table_name, fields, key_field_name)

        with open(self.file_name, 'r') as json_file:
            tables_name = json.load(json_file)
            if table_name in tables_name:
                raise KeyError('BAD_KEY')

            tables_name[table_name] = [[field.name for field in fields], key_field_name, table_name + ".json"]
            write(self.file_name, tables_name)
        return new_db

    def num_tables(self) -> int:
        with open(self.file_name, "r") as json_file:
            tables_name = json.load(json_file)
            return len(tables_name)

    def get_table(self, table_name: str) -> DBTable:
        with open(self.file_name, "r") as json_file:
            tables_name = json.load(json_file)
            if tables_name.get(table_name):
                return DBTable(table_name, [DBField(x, str) for x in tables_name[table_name][0]],
                               tables_name[table_name][1])  # TODO
            else:
                raise Exception

    def delete_table(self, table_name: str):
        with open(self.file_name, "r") as json_file:
            tables_name = json.load(json_file)
            if table_name in tables_name:
                del tables_name[table_name]
                os.remove(table_name + ".json")
            write(self.file_name, tables_name)

    def get_tables_names(self) -> List[Any]:
        with open(self.file_name, "r") as json_file:
            tables_name = json.load(json_file)
            return list(tables_name.keys())

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError
