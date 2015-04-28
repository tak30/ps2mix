#!/usr/bin/env python
import argparse
import csv
import ntpath
import os
import datetime
import glob
import sys


def parse_args():
    parser = argparse.ArgumentParser(
        description='Migrate stuff from postgresql to informix')
    parser.add_argument("-c", "--csv", type=str,
                        help='Usage: -c file|directory')
    args = parser.parse_args()
    if args.csv is not None:
        migrate_csvs(args.csv)
    else:
        parser.print_help()
        sys.exit(1)


def date_to_informix(date_text):
    try:
        ps_date = datetime.datetime.strptime(date_text, '%d/%m/%Y')
        return ps_date.strftime('%Y-%m-%d 00:00:00')
    except ValueError:
        try:
            ps_date = datetime.datetime.strptime(date_text, '%d/%m/%Y %I:%M')
            return ps_date.strftime('%Y-%m-%d %I:%M:%S')
        except ValueError:
            return None


def get_informix_row(ps_row, row_number):
    pos = 0
    for item in ps_row:
        if item.lower() == 'true':
            ps_row[pos] = 't'
        elif item.lower() == 'false':
            ps_row[pos] = 'f'
        elif item.lower() == 'null':
            ps_row[pos] = 'NULL'
        else:
            date = date_to_informix(item)
            if date is not None:
                ps_row[pos] = date
        pos += 1
    return [row_number] + ps_row


def get_informix_header(ps_header, file_name):
    table_name, extension = os.path.splitext(file_name)
    column = table_name[4:] + "_id"
    return [column] + ps_header


def ps_to_mix_csv(src_path, dst_path):
    row_number = 0
    file_name = ntpath.basename(src_path)
    with open(dst_path, "wb") as write_file:
        writer = csv.writer(write_file, delimiter=';')
        with open(src_path, "rb") as read_file:
            reader = csv.reader(read_file, delimiter=';')
            for row in reader:
                if row_number == 0:
                    new_row = get_informix_header(row, file_name)
                else:
                    new_row = get_informix_row(row, row_number)
                writer.writerow(new_row)
                row_number += 1


def convert_csv(path):
    dir_path = os.path.dirname(path)
    new_dir = dir_path + "/" + 'informix'
    if os.path.isfile(new_dir):
        print 'Error: Existence of an informix file'
        sys.exit(1)
    if not os.path.exists(new_dir):
            os.makedirs(new_dir)
    new_file = new_dir + "/" + ntpath.basename(path)
    ps_to_mix_csv(path, new_file)


def migrate_csvs(path):
    abs_path = os.path.abspath(path)
    if os.path.isfile(abs_path):
        convert_csv(abs_path)
    elif os.path.isdir(abs_path):
        for each_file in glob.glob(abs_path + "/*.csv"):
            convert_csv(each_file)


if __name__ == '__main__':
    parse_args()
