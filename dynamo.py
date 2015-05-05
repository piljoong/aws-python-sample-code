import sys
import time

from boto import dynamodb2
from boto.dynamodb2.table import Table
from boto.dynamodb2.fields import HashKey, RangeKey, GlobalAllIndex

DEFAULT_REGION = 'ap-northeast-1'

class DynamoDBSample():
    def __init__(self, region=DEFAULT_REGION):
        try:
            self.conn = dynamodb2.connect_to_region(region)
        except Exception as e:
            print 'Failed to connect to region: {err}'.format(err=e)

    def check_table_status(self, name):
        try:
            while True:
                info = self.conn.describe_table(name)
                status = info['Table']['TableStatus']
                if status == 'CREATING' or status == 'DELETING':
                    print 'Table [{name}] is currently not usable: STATUS - {status}'.format(
                        name=name, status=status)
                    time.sleep(1)
                    continue
                else:
                    return status
        except Exception as e:
            if e.code == 'ResourceNotFoundException':
                print 'Table [{name}] is not exist'.format(name=name)
            else:
                print 'Failed to lookup the table: {err}'.format(err=e)

    def create_table_if_not_exists(self, name, schema):
        status = self.check_table_status(name)
        if status == 'ACTIVE':
            print 'Table [{name}] is already exists'.format(name=name)
            return Table(name, connection=self.conn)
        elif status == None:
            raw_input('> Press enter to create the table...')
            return self.create_table(name, schema)

    def create_table(self, name, schema):
        try:
            tbl = Table.create(name, schema=schema, throughput={
                'read': 5,
                'write': 15,
            }, global_indexes=[
                GlobalAllIndex('EverythingIndex', parts=[
                    HashKey('account_type'),
                ], throughput={
                    'read': 1,
                    'write': 1,
                })
            ], connection=self.conn)
            return tbl
        except Exception as e:
            print 'Failed to create table: {err}'.format(err=e)

    def delete_table(self, name, tbl):
        try:
            self.check_table_status(name)
            tbl.delete()
        except Exception as e:
            print 'Failed to delete table: {err}'.format(err=e)

    def print_item(self, item):
        for x in item:
            sys.stdout.write(x + ', ')
        print ''

if __name__ == '__main__':
    dynamo = DynamoDBSample()

    table_name = 'users'
    schema = [
        HashKey('username'),
        RangeKey('last_name')
    ]

    tbl = dynamo.create_table_if_not_exists(table_name, schema)

    raw_input('> Press enter to put the item...')
    dynamo.check_table_status(table_name)
    data = {
        'username': 'User1',
        'first_name': 'First',
        'last_name': 'Last',
        'account_type': 'AccountType01'
    }
    tbl.put_item(data=data)

    raw_input('> Press enter to get the item...')
    item = tbl.get_item(username='User1', last_name='Last')
    dynamo.print_item(item)

    raw_input('> Press enter to update the item using save function...')
    overwrite = True
    item['first_name'] = 'First2'
    item.save(overwrite=overwrite)
    dynamo.print_item(item)

    raw_input('> Press enter to update the item using partial_save function...')
    item['account_type'] = 'AccountType02'
    item.partial_save()
    dynamo.print_item(item)

    raw_input('> Press enter to delete the item...')
    item.delete()

    raw_input('> Press enter to delete the table...')
    dynamo.delete_table(table_name, tbl)
