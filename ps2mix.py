#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import csv
import ntpath
import os
import datetime
import glob
import sys
from ps2mix_scripts import migrate_scripts


def parse_args():
    parser = argparse.ArgumentParser(
        description='Migrate stuff from postgresql to informix')
    parser.add_argument("-c", "--csv", type=str,
                        help='Usage: -c (file|directory)')
    parser.add_argument("-o", "--out", type=str,
                        help='Usage: -o (directory)')
    parser.add_argument("-ef", "--excluded", type=str,
                        help='Usage: -e (line separated excluded file)')
    parser.add_argument("-s", "--scripts", type=str,
                        help="Usage: -s (path to config file)")
    args = parser.parse_args()
    if args.csv is not None and args.excluded is not None and \
            args.out is not None:
        excluded_files_path = os.path.abspath(args.excluded)
        with open(excluded_files_path, 'rb') as file:
            excluded_files = file.read().splitlines()
            migrate_csvs(args.csv, excluded_files, args.out)
    elif args.scripts is not None:
        migrate_scripts(args.scripts)
    else:
        parser.print_help()
        sys.exit(1)


def date_to_informix(date_text):
    try:
        ps_date = datetime.datetime.strptime(date_text, '%d/%m/%Y')
        return ps_date.strftime('%Y-%m-%d')
    except ValueError:
        try:
            ps_date = datetime.datetime.strptime(date_text, '%d/%m/%Y %H:%M')
            return ps_date.strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            try:
                ps_date = datetime.datetime.strptime(date_text, '%d/%m/%Y %H:%M:%S')
                return ps_date.strftime('%Y-%m-%d %H:%M:%S')
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
    add_id = all(column != s for s in ps_header)
    if add_id:
        new_row = [column] + ps_header
    else:
        new_row = ps_header
    return new_row, add_id


def ps_to_mix_csv(src_path, dst_path, excluded_files):
    row_number = 0
    file_name = ntpath.basename(src_path)
    print "Migrating file: " + file_name
    add_id = all(file_name != s for s in excluded_files)
    with open(dst_path, "wb") as write_file:
        writer = csv.writer(write_file, delimiter=';')
        with open(src_path, "rb") as read_file:
            reader = csv.reader(read_file, delimiter=';')
            for row in reader:
                if not row: #If empty row, dont write it
                    continue
                if row_number == 0:
                    if add_id:
                        new_row, add_id = get_informix_header(row, file_name)
                    else:
                        new_row = row
                else:
                    new_row = get_informix_row(row, row_number, add_id)
                writer.writerow(new_row)
                row_number += 1


def convert_csv(path, excluded_files, out_dir):
    if os.path.isfile(out_dir):
        print 'Error: Existence of a file named ' + os.path.basename(out_dir)
        sys.exit(1)
    if not os.path.exists(out_dir):
            os.makedirs(out_dir)
    new_file = out_dir + "/" + ntpath.basename(path)
    ps_to_mix_csv(path, new_file, excluded_files)


def migrate_csvs(path, excluded_files, out_dir):
    abs_path = os.path.abspath(path)
    if os.path.isfile(abs_path):
        convert_csv(abs_path, excluded_files, out_dir)
    elif os.path.isdir(abs_path):
        for each_file in glob.glob(abs_path + "/*.csv"):
            convert_csv(each_file, excluded_files, out_dir)


if __name__ == '__main__':
    parse_args()
