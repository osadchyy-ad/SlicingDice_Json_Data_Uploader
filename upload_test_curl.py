import ujson
import json
import datetime
import multiprocessing.dummy as mp
import requests
import argparse

parser = argparse.ArgumentParser()

parser.add_argument('-f', '--file', action='store', type=str,
                    help='Path to file with data in json format. Tip: put script to folder with file')
parser.add_argument('-m', '--masterkey', action='store', type=str,
                    help='Path to file with master key. Get it from website. '
                         'Required txt file with master key writen in one line')
parser.add_argument('-d', '--dimension', action='store', type=str,
                    help='Name of dimension.')
parser.add_argument('-c', '--chunksize', action='store', type=int,
                    help='Determines how many records will be in one payload')
parser.add_argument('-p', '--numberofpools', action='store', type=int,
                    help='Determines how many pools will be used')
parser.add_argument('-a', '--autocreate', action='store_true', help='Autocreate')
parser.add_argument('-id', '--uuid', action='store', type=str, help='')

args = parser.parse_args().__dict__

if args['masterkey'] is None or args['file'] is None or args['uuid'] is None:
    print('Error: No master key, uuid or file detected!')
    quit(-1)
else:
    try:
        with open(args['masterkey'], 'r') as master_key_file:
            master_key = master_key_file.readline()
    except FileNotFoundError:
        print('File not found!')
        quit(-1)
    filename = args['file']
    uuid = args['uuid']

dimension = args['dimension'] if args['dimension'] is not None else 'default'
chunk_size = args['chunksize'] if args['chunksize'] is not None else 1
number_of_pools = args['numberofpools'] if args['numberofpools'] is not None else 1
auto_create = {"auto-create": ["dimension", "column"]}


def convert_to_format(record):
    insert_data = dict()
    insert_data['UUID-' + str(record[args['uuid']])] = {'dimension': dimension}
    for elem in record:
        if elem != args['uuid']:
            insert_data['UUID-' + str(record[args['uuid']])].update({elem: record[elem]})
    return insert_data


def prepare_data(file_json):
    data = list()
    for id_, record in enumerate(file_json):
        insert_data = convert_to_format(record)
        data.append(insert_data)
    return data


def prepare_payloads(data):
    payloads = list()
    pointer = 0

    range_max = round(data.__len__() % chunk_size, 0)
    print(range_max)
    for elem in range(0, range_max):
        try:
            elem = data[pointer]
        except IndexError:
            break
        for n in range(1, chunk_size):
            try:
                elem.update(data[pointer + n])
            except IndexError:
                break
        if args['autocreate'] is True:
            elem.update(auto_create)
        payloads.append(elem)
        pointer += chunk_size

    return payloads


def upload(payload):
    payload = ujson.dumps(payload)
    headers = {"Content-Type": "application/json", "Authorization": master_key}
    r = requests.post(url="https://api.slicingdice.com/v1/insert", data=payload, headers=headers)
    try:
        r = r.json()
        print(r.json())
    except json.decoder.JSONDecodeError:
        r = r.text
    while str(r) != '<Response [200]>':
        print(r.json(), payload)
        r = requests.post(url="https://api.slicingdice.com/v1/insert", data=payload, headers=headers)
    print(r, payload)


def logger(func):
    def wrapper():
        with open('log.txt', 'a+') as log:
            start = datetime.datetime.now()
            log.write('Start ' + func.__name__ + ': ' + str(start))
            func()
            end = datetime.datetime.now()
            log.write('End ' + func.__name__ + ': ' + str(end))
            log.write('Total ' + func.__name__ + ': ' + str(end - start))
    return wrapper


@logger
def main():
    with open(filename, 'r') as file:
        test_str = file.read()

    loaded_json = ujson.loads(test_str)
    data = prepare_data(loaded_json)
    payloads = prepare_payloads(data)
    print(payloads)
    print("_______________________")
    pool2 = mp.Pool(number_of_pools)
    pool2.map(upload, payloads)
    pool2.close()
    pool2.join()
    print("Done")


if __name__ == '__main__':
    main()
