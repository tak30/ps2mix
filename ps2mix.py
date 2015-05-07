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
        excluded_files_path = os.path.abspath('excluded_files.txt')
        with open(excluded_files_path, 'rb') as file:
            excluded_files = file.read().splitlines()
            migrate_csvs(args.csv, excluded_files)
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


def get_informix_row(ps_row, row_number, add_id):
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
    if add_id:
        new_row = [row_number] + ps_row
    else:
        new_row = ps_row
    return new_row


def get_informix_header(ps_header, file_name):
    table_name, extension = os.path.splitext(file_name)
    column = table_name[4:] + "_id"
    add_id = any(column in s for s in unicode(ps_header))
    if add_id:
        new_row = [column] + ps_header
    else:
        new_row = ps_header
    return new_row, add_id


def ps_to_mix_csv(src_path, dst_path, excluded_files):
    row_number = 0
    file_name = ntpath.basename(src_path)
    print "Migrating file: " + file_name
    add_id = file_name not in excluded_files
    with open(dst_path, "wb") as write_file:
        writer = csv.writer(write_file, delimiter=';')
        with open(src_path, "rb") as read_file:
            reader = csv.reader(read_file, delimiter=';')
            for row in reader:
                if row_number == 0:
                    if add_id:
                        new_row, add_id = get_informix_header(row, file_name)
                    else:
                        new_row = row
                else:
                    new_row = get_informix_row(row, row_number, add_id)
                writer.writerow(new_row)
                row_number += 1


def convert_csv(path, excluded_files):
    dir_path = os.path.dirname(path)
    new_dir = dir_path + "/" + 'informix'
    if os.path.isfile(new_dir):
        print 'Error: Existence of a file named informix'
        sys.exit(1)
    if not os.path.exists(new_dir):
            os.makedirs(new_dir)
    new_file = new_dir + "/" + ntpath.basename(path)
    ps_to_mix_csv(path, new_file, excluded_files)


def migrate_csvs(path, excluded_files):
    abs_path = os.path.abspath(path)
    if os.path.isfile(abs_path):
        convert_csv(abs_path, excluded_files)
    elif os.path.isdir(abs_path):
        for each_file in glob.glob(abs_path + "/*.csv"):
            convert_csv(each_file, excluded_files)


if __name__ == '__main__':
    parse_args()