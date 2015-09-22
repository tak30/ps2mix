#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import os
import sys
from ps2mix_scripts import migrate_scripts
from ps2mix_join_sql import join_sql
from ps2mix_join_data import join_data
from ps2mix_csvs import migrate_csvs


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
    parser.add_argument("-jsql", "--join_sql", type=str,
                        help="Usage: -jsql (path to config file)")
    parser.add_argument("-jdata", "--join_data", type=str,
                        help="Usage: -jdata (path to config file)")
    parser.add_argument("--debug", action='store_true',
                        help='print debug messages to stderr')
    args = parser.parse_args()
    if args.csv is not None and args.excluded is not None and \
            args.out is not None:
        excluded_files_path = os.path.abspath(args.excluded)
        with open(excluded_files_path, 'rb') as file:
            excluded_files = file.read().splitlines()
            migrate_csvs(args.csv, excluded_files, args.out)
    elif args.scripts is not None:
        migrate_scripts(args.scripts, args.debug)
    elif args.join_sql is not None:
        join_sql(args.join_sql, args.debug)
    elif args.join_data is not None:
        join_data(args.join_data, args.debug)
    else:
        parser.print_help()
        sys.exit(1)



if __name__ == '__main__':
    parse_args()
