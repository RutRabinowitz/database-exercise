import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Type

# import dataclasses_json
#
# from dataclasses_json import dataclass_json

DB_ROOT = Path('db_files')


# @dataclass_json
@dataclass
class DBField:
    name: str  # "id"
    type: Type  # int

    def __init__(self, name, type):
        self.name = name
        self.type = type


# @dataclass_json
@dataclass
class SelectionCriteria:
    field_name: str
    operator: str
    value: Any

    def __init__(self, field_name, operator, value):
        self.field_name = field_name
        self.operator = operator
        self.value = value

    def get_fun_name(self):
        fun_names ={
            '=': 'eq',
            '<': 'lt',
            '>': 'gt'
        }
        return fun_names[self.operator]


# @dataclass_json
@dataclass
class DBTable:  # sick
    name: str  # sick
    fields: List[DBField]  # [id, name, is_sympomatic...]
    key_field_name: str  # "id"

    def __init__(self, name, fields, key_field_name):
        self.name = name
        self.fields = fields

        if key_field_name in self.get_fields_name():
            self.key_field_name = key_field_name
            with open(self.get_file_name(), 'w') as file:
                json.dump({}, file)
            return
        else:
            raise NotImplementedError  # Primary key Error...

    def count(self) -> int:
        with open(self.name + '.json', 'r') as json_file:
            return len(json.load(json_file))

    def insert_record(self, values: Dict[str, Any]) -> None:
        with open(self.get_file_name(), 'r') as json_file:
            records = json.load(json_file)

            if str(values[self.key_field_name]) not in records:
                records[values[self.key_field_name]] = [values.get(key) for key in \
                                                        self.get_fields_name() if key != self.key_field_name]
                self.write_to_file(records)
            else:
                raise TypeError

    def delete_record(self, key: Any) -> None:
        with open(self.get_file_name(), 'r') as json_file:
            records = json.load(json_file)

            if str(key) in records:
                del records[key]
                self.write_to_file(records)
            else:
                raise ValueError

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        raise NotImplementedError

    def get_record(self, key: Any) -> Dict[str, Any]:
        with open(self.get_file_name(), 'r') as json_file:
            records = json.load(json_file)

            if str(key) in records:
                result = {self.key_field_name: key}
                for idx, field in enumerate(records[key]):
                    result[self.fields[idx + 1].name] = field
                return result
            else:
                raise ValueError

    # students.update_record(1_000_111, dict(First='Jane', Last='Doe'))
    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        with open(self.get_file_name(), 'r') as json_file:
            records = json.load(json_file)

            if key in records:
                records[key] = self.get_updated_record(key, values, records[key])
                self.write_to_file(records)
            else:
                raise ValueError

    def query_table(self, criteria: List[SelectionCriteria]) \
            -> List[Dict[str, Any]]:
        with open(self.get_file_name(), 'r') as json_file:
            records = json.load(json_file)
            result = []

            for cr in criteria:
                result += self.get_criteria_records(cr, records)
            return result
        raise NotImplementedError

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


# @dataclass_json
class NoTable(object):
    pass


def write(file_name, data):
    with open(file_name, 'w') as json_file:
        json.dump(data, json_file)


@dataclass
class DataBase:
    def __init__(self):
        self.name = "my_db"
        self.file_name = "my_db.json"
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
            # TODO: to think about it!
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
                return DBTable(...)  # TODO
            else:
                raise Exception

    def delete_table(self, table_name: str):
        with open(self.file_name, "r") as json_file:
            tables_name = json.load(json_file)
            if table_name in tables_name:
                del tables_name[table_name]
                # table_name + ".json".seek(0)TODO delete the table file
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


my_db = DataBase()

Person = my_db.create_table("Person", [DBField("id", int), DBField("name", str), DBField("address", str), DBField("age", int)], "id")

Doctor = my_db.create_table("Doctor", [DBField("id", int), DBField("name", str), DBField("clinic", str)], "id")

# my_db.delete_table("Person")
# my_db.create_table("Person", [DBField("id", int), DBField("name", str), DBField("address", str)], "id")

print(my_db.get_tables_names())

print(my_db.num_tables())

Person.insert_record({"name": "Ruth", "id": 1, "address": "Jerusalem", "age": 30})

Person.insert_record({"name": "Miri", "id": 2, "address": "Tel - Aviv", "age": 40})

print(Person.get_record('2'))
Person.update_record('2', dict(address='Bne - Brak', name='Jane'))
print(Person.get_record('2'))
print(Person.query_table([SelectionCriteria("name", "=", "Ruth"), SelectionCriteria("age", "<", '50'), SelectionCriteria("id", ">", '50')]))