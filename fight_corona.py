import db_api as db
import os


def print_record(record_dict):
    return str(record_dict).replace("{", "[").replace("}", "]")


sick_fields = [db.DBField('ID', int), db.DBField('name', str),
               db.DBField('symptomatic', bool), db.DBField('age', int)]
scientist_fields = [db.DBField('ID', int), db.DBField('name', str), db.DBField('address', str)]

sick_to_scientist_fields = [db.DBField('sick_ID', int), db.DBField('scientist_ID', str)]

address = ["Jerusalem", "Tel - Aviv", "Haifa"]

fight_corona_db = db.DataBase()

assert fight_corona_db.num_tables() == 0

# Create tables
sick_table = fight_corona_db.create_table("Sick", sick_fields, 'ID')
scientists_table = fight_corona_db.create_table("Scientists", scientist_fields, 'ID')
sick_to_scientist_table = fight_corona_db.create_table("Sick_to_scientist",
                                                       sick_to_scientist_fields, 'sick_ID')

assert fight_corona_db.num_tables() == 3

for idx in range(100):
    sick_table.insert_record({'ID': idx, 'name': "sick" + str(idx),
                              'symptomatic': True if idx % 2 else False, 'age': idx + idx % 4})

for idx in range(10):
    scientists_table.insert_record({'ID': 111_00 + idx, 'name': "scientist" + str(idx),
                                    'address': address[idx % 3]})
    for i in range(1, 11):
        sick_to_scientist_table.insert_record({'sick_ID': idx * 10 + i - 1, 'scientist_ID': 111_00 + idx})

assert sick_table.count() == 100
assert sick_to_scientist_table.count() == 100
assert scientists_table.count() == 10

print("\n-----------Getting some records in the Sick table-----------")
print("sick_table.get_record(1): ", print_record(sick_table.get_record(1)))
print("sick_table.get_record(10): ", print_record(sick_table.get_record(10)))
print("sick_table.get_record(46): ", print_record(sick_table.get_record(46)))
print("sick_table.get_record(99): ", print_record(sick_table.get_record(99)))

print("\n-----------Getting some records in the Scientist table-----------")
print("scientists_table.get_record(111_00 + 1): ", print_record(scientists_table.get_record(111_00 + 1)))
print("scientists_table.get_record(111_00 + 3): ", print_record(scientists_table.get_record(111_00 + 3)))
print("scientists_table.get_record(111_00 + 6): ", print_record(scientists_table.get_record(111_00 + 6)))
print("scientists_table.get_record(111_00 + 9): ", print_record(scientists_table.get_record(111_00 + 9)))

print("\n-----------Getting some records in the Sick_to_scientist table-----------")
print("sick_to_scientist_table.get_record(61): ", print_record(sick_to_scientist_table.get_record(61)))
print("sick_to_scientist_table.get_record(33): ", print_record(sick_to_scientist_table.get_record(33)))
print("sick_to_scientist_table.get_record(16): ", print_record(sick_to_scientist_table.get_record(16)))
print("sick_to_scientist_table.get_record(89): ", print_record(sick_to_scientist_table.get_record(89)))

fight_corona_db.delete_table("Sick")
fight_corona_db.delete_table("Scientists")
fight_corona_db.delete_table("Sick_to_scientist")

os.remove("my_db.json")
