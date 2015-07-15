#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ConfigParser
import logging
import os
import sys
import shutil
import sqlparse
import re


settings = {}
logger = logging.getLogger(__name__)


def validate_parameters():
    logger.debug('-*' * 10 + 'BEGIN: validating parameters' + '-*' * 10 + '\n')
    global settings
    logger.debug('in_dir: ' + settings['in_dir'])
    logger.debug('out_dir: ' + settings['out_dir'])
    logger.debug('in_script_create_table_name : ' +
                 settings['create_table_file_name'])
    logger.debug('in_script_create_sequence_name: ' +
                 settings['create_sequence_file_name'])
    logger.debug('in_script_alter_sequence_name: ' +
                 settings['alter_sequence_file_name'])
    logger.debug('out_liquibase_master_xml_name: ' +
                 settings['liquibase_master_xml_name'])
    logger.debug('out_liquibase_alter_table_xml_name: ' +
                 settings['liquibase_alter_table_xml_name'])
    logger.debug('common_config_file_path: ' +
                 settings['common_config_file_path'])
    logger.debug('alter_sequence_tail: ' + settings['alter_sequence_tail'])
    logger.debug('-*' * 10 + 'END: validating parameters' + '-*' * 10 + '\n')
    # TODO: Validation


def parse_config_file():
    logger.debug('-*' * 10 + 'BEGIN: parse conf' + '-*' * 10 + '\n')
    specific_settings = ConfigParser.ConfigParser()
    specific_settings.read(['config'])

    common_settings = ConfigParser.ConfigParser()

    global settings
    settings['in_dir'] = specific_settings.get('scripts', 'dir_in')
    settings['out_dir'] = specific_settings.get('scripts', 'dir_out')
    settings['create_table_file_name'] = specific_settings.get(
        'scripts', 'in_script_create_table_name')
    settings['create_sequence_file_name'] = specific_settings.get(
        'scripts', 'in_script_create_sequence_name')
    settings['alter_sequence_file_name'] = specific_settings.get(
        'scripts', 'in_script_alter_sequence_name')
    settings['liquibase_master_xml_name'] = specific_settings.get(
        'scripts', 'out_liquibase_master_xml_name')
    settings['liquibase_alter_table_xml_name'] = specific_settings.get(
        'scripts', 'out_liquibase_alter_table_xml_name')
    settings['common_config_file_path'] = specific_settings.get(
        'scripts', 'common_config_file_path')

    # Common parameters
    common_settings.read([settings['common_config_file_path']])
    settings['alter_sequence_tail'] = common_settings.get(
        'scripts', 'alter_sequence_tail')
    logger.debug('-*' * 10 + 'END: parse conf' + '-*' * 10 + '\n')


def migrate_create_sequence():
    logger.debug('-*' * 10 + 'BEGIN: migrate create sequence' + '-*' * 10 +
                 '\n')
    out_dir_path = os.path.abspath(settings['out_dir'])
    in_file_path = os.path.abspath(settings['in_dir'] + '/' +
                                   settings['create_sequence_file_name'])
    file_name = os.path.basename(in_file_path)
    out_file_path = out_dir_path + '/' + file_name
    with open(in_file_path, 'r') as in_file:
        raw_data = in_file.read()
        parsed = sqlparse.parse(raw_data)
        with open(out_file_path, 'a') as out_file:
            for item in parsed:
                if re.match("[\r\n?|\n]*\s*[set]+", unicode(item).lower())  \
                        is not None:
                    continue
                out_file.write(unicode(item))

    logger.debug('-*' * 10 + 'END: migrate create sequence' + '-*' * 10 +
                 '\n')


def migrate_alter_sequence():
    logger.debug('-*' * 10 + 'BEGIN: migrate alter sequence' + '-*' * 10 +
                 '\n')
    out_dir_path = os.path.abspath(settings['out_dir'])
    in_file_path = os.path.abspath(settings['in_dir'] + '/' +
                                   settings['alter_sequence_file_name'])
    file_name = os.path.basename(in_file_path)
    out_file_path = out_dir_path + '/' + file_name
    with open(in_file_path, 'r') as in_file:
        raw_data = in_file.read()
        parsed = sqlparse.parse(raw_data)
        with open(out_file_path, 'a') as out_file:
            for item in parsed:
                if re.match("[\r\n?|\n]*\s*[set]+", unicode(item).lower())  \
                        is not None:
                    continue
                item = re.sub("owned(?i).*$", settings['alter_sequence_tail'],
                              unicode(item))
                out_file.write(unicode(item))

    logger.debug('-*' * 10 + 'END: migrate alter sequence' + '-*' * 10 +
                 '\n')


def convert_varchars(statement):
    for match in re.findall("varchar\([0-9]*(?i)", statement):
        num = match[8:]
        if int(num) > 256:
            statement = re.sub("varchar\(" + num + "(?i)",
                               "LVARCHAR(" + num, statement)
    return statement


def write_create_statement(statement, create_file):
    statement_unicode = unicode(statement)
    statement_unicode = re.sub("timestamp(?i)", "DATETIME YEAR TO SECOND",
                               statement_unicode)
    statement_unicode = re.sub("\sdate\s(?i)", "DATETIME YEAR TO DAY",
                               statement_unicode)
    statement_unicode = convert_varchars(statement_unicode)
    statement_unicode = re.sub("constraint(?i).*primary key(?i)", "PRIMARY KEY",
                               statement_unicode)
    statement_unicode = re.sub("default nextval\(.*\:\:regclass\)(?i)",
                               "", statement_unicode)
    create_file.write(statement_unicode)


def parse_create_statement(statement, create_file, alter_file):
    if re.match("[\r\n?|\n]*\s*[set]+", unicode(statement).lower())  \
            is not None:
        return
    if re.search("create\s*table", unicode(statement).lower()) is not None:
        write_create_statement(statement, create_file)


def migrate_create_table():
    logger.debug('-*' * 10 + 'BEGIN: migrate create table' + '-*' * 10 +
                 '\n')
    out_dir_path = os.path.abspath(settings['out_dir'])
    in_file_path = os.path.abspath(settings['in_dir'] + '/' +
                                   settings['create_table_file_name'])
    create_file_name = os.path.basename(in_file_path)
    alter_file_name = settings['liquibase_alter_table_xml_name']
    out_create_file_path = out_dir_path + '/' + create_file_name
    out_alter_file_path = out_dir_path + '/' + alter_file_name
    with open(in_file_path, 'r') as in_file:
        raw_data = in_file.read()
        parsed = sqlparse.parse(raw_data)
        with open(out_alter_file_path, 'a') as out_alter_file:
            with open(out_create_file_path, 'a') as out_create_file:
                for item in parsed:
                    parse_create_statement(item, out_create_file,
                                           out_alter_file)
    logger.debug('-*' * 10 + 'END: migrate create table' + '-*' * 10 +
                 '\n')


def prepare_output_dir():
    out_dir_path = os.path.abspath(settings['out_dir'])
    if os.path.isfile(out_dir_path):
        logger.error('Existence of file: ' + out_dir_path)
        sys.exit(1)
    if os.path.exists(out_dir_path):
        shutil.rmtree(out_dir_path, ignore_errors=True)
    os.makedirs(out_dir_path)


def migrate_scripts():
    global logger
    logging.basicConfig()
    logger.setLevel(logging.DEBUG)
    parse_config_file()
    validate_parameters()
    prepare_output_dir()
    migrate_create_sequence()
    migrate_alter_sequence()
    migrate_create_table()
