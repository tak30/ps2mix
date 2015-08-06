#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ConfigParser
import logging
import os


settings = {}
logger = logging.getLogger(__name__)


def validate_parameters():
    logger.debug('-*' * 10 + 'BEGIN: validating parameters' + '-*' * 10 + '\n')
    global settings
    logger.debug('in_dirs: ' + str(settings['in_dirs']))
    logger.debug('out_file_path: ' + settings['out_file_path'])
    logger.debug('out_alter_table_sql_name: ' +
                 settings['alter_table_sql_name'])
    logger.debug('-*' * 10 + 'END: validating parameters' + '-*' * 10 + '\n')
    # TODO: Validation


def parse_config_file(config_file_path):
    logger.debug('-*' * 10 + 'BEGIN: parse conf' + '-*' * 10 + '\n')
    specific_settings = ConfigParser.ConfigParser()
    config_file_path = os.path.abspath(config_file_path)
    with open(config_file_path, 'rb') as config_file:
        specific_settings.readfp(config_file)

    global settings
    in_dirs_string = specific_settings.get('join_sql', 'input_directory_paths')
    settings['in_dirs'] = in_dirs_string.split()
    settings['out_file_path'] = specific_settings.get(
        'join_sql', 'output_file_path')
    settings['alter_table_sql_name'] = specific_settings.get(
        'scripts', 'out_alter_table_sql_name')
    logger.debug('-*' * 10 + 'END: parse conf' + '-*' * 10 + '\n')


def prepare_output_file():
    out_file_path = os.path.abspath(settings['out_file_path'])
    if os.path.isfile(out_file_path):
        os.remove(out_file_path)


def append_sql_file(sql_file_path, out_file):
    with open(sql_file_path, 'rb') as sql_file:
        out_file.write("\n\n--" + "==" * 10)
        out_file.write("--" + "Source file name: " +
                       os.path.basename(sql_file_path) + "==" * 10)
        out_file.write(sql_file.read())


def append_module(module, out_file):
    logger.debug('-*' * 10 + 'BEGIN: append module' + '-*' * 10 + '\n')
    out_file.write("--" + "==" * 20 + "New Module:" + "==" * 20)
    for f in os.listdir(module):
        if os.path.splitext(f)[1] != ".sql":
            continue
        if f == settings['alter_table_sql_name']:
            continue
        sql_file_abs_path = os.path.join(module, f)
        append_sql_file(sql_file_abs_path, out_file)
    sql_file_abs_path = os.path.join(module, settings['alter_table_sql_name'])
    if os.path.isfile(sql_file_abs_path):
        append_sql_file(sql_file_abs_path, out_file)
    logger.debug('-*' * 10 + 'END: append module' + '-*' * 10 + '\n')


def join_sql(config_file_path, debug):
    global logger
    logging.basicConfig()
    if debug:
        logger.setLevel(logging.DEBUG)
    parse_config_file(config_file_path)
    validate_parameters()
    prepare_output_file()
    with open(settings['out_file_path'], 'ab') as out_file:
        for module in settings['in_dirs']:
            append_module(module, out_file)
