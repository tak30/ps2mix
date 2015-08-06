#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ConfigParser
import logging
import os
import subprocess
import re


settings = {}
logger = logging.getLogger(__name__)


def validate_parameters():
    logger.debug('-*' * 10 + 'BEGIN: validating parameters' + '-*' * 10 + '\n')
    global settings
    logger.debug('in_dirs: ' + str(settings['in_dirs']))
    logger.debug('out_file_path: ' + settings['out_file_path'])
    logger.debug('mvn_path: ' + settings['mvn_path'])
    logger.debug('mvn_profile: ' + settings['mvn_profile'])
    logger.debug('-*' * 10 + 'END: validating parameters' + '-*' * 10 + '\n')
    # TODO: Validation


def parse_config_file(config_file_path):
    logger.debug('-*' * 10 + 'BEGIN: parse conf' + '-*' * 10 + '\n')
    specific_settings = ConfigParser.ConfigParser()
    config_file_path = os.path.abspath(config_file_path)
    with open(config_file_path, 'rb') as config_file:
        specific_settings.readfp(config_file)

    global settings
    in_dirs_string = specific_settings.get('join_data', 'input_directory_paths')
    settings['in_dirs'] = in_dirs_string.split()
    settings['out_file_path'] = specific_settings.get('join_data', 'output_file_path')
    settings['mvn_path'] = specific_settings.get('join_data', 'mvn_path')
    settings['mvn_profile'] = specific_settings.get('join_data', 'mvn_profile')
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
        sql_file_data = re.sub(".*DATABASECHANGELOG.*", "", sql_file.read())
        out_file.write(sql_file_data)


def generate_sql_file(module):
    old_path = os.getcwd()
    os.chdir(module)
    logger.debug(os.getcwd())
    profile = '-P' + settings['mvn_profile']
    subprocess.call([settings['mvn_path'], 'liquibase:updateSQL', profile])
    os.chdir(old_path)


def append_module(module, out_file):
    logger.debug('-*' * 10 + 'BEGIN: append module' + '-*' * 10 + '\n')
    out_file.write("--" + "==" * 20 + "New Module:" + "==" * 20)
    generate_sql_file(module)
    sql_file_abs_path = os.path.abspath(module + '\\target\\liquibase\\migrate.sql')
    append_sql_file(sql_file_abs_path, out_file)
    logger.debug('-*' * 10 + 'END: append module' + '-*' * 10 + '\n')


def join_data(config_file_path, debug):
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
