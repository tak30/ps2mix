# -*- coding: utf-8 -*-

import ConfigParser
import logging
import os
import sys
import shutil
import math
import sqlparse
import re
import csv

settings = {}
customizations = {}
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
    logger.debug('out_alter_table_sql_name: ' +
                 settings['alter_table_sql_name'])
    logger.debug('common_config_file_path: ' +
                 settings['common_config_file_path'])
    logger.debug('alter_sequence_tail: ' + settings['alter_sequence_tail'])
    logger.debug('alter_table_header: ' + settings['alter_table_header'])
    logger.debug('alter_table_tail: ' + settings['alter_table_tail'])
    logger.debug('alter_table_template: ' + settings['alter_table_template'])
    logger.debug('liquibase_master_header: ' +
                 settings['liquibase_master_header'])
    logger.debug('liquibase_master_tail: ' + settings['liquibase_master_tail'])
    logger.debug('liquibase_master_template: ' + settings['liquibase_master_template'])
    logger.debug('liquibase_master_insert_file: ' + settings['liquibase_master_insert_file'])
    logger.debug('customizations_file: ' + settings['customizations_file'])
    logger.debug('-*' * 10 + 'END: validating parameters' + '-*' * 10 + '\n')
    # TODO: Validation


def parse_config_file(config_file_path):
    logger.debug('-*' * 10 + 'BEGIN: parse conf' + '-*' * 10 + '\n')
    specific_settings = ConfigParser.ConfigParser()
    with open(config_file_path, 'rb') as config_file:
        specific_settings.readfp(config_file)

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
    settings['customizations_file'] = specific_settings.get(
        'scripts', 'customizations_file')

    # Common parameters
    common_settings.read([settings['common_config_file_path']])
    settings['alter_sequence_tail'] = common_settings.get(
        'scripts', 'alter_sequence_tail')
    settings['alter_table_tail'] = common_settings.get(
        'scripts', 'alter_table_tail')
    settings['alter_table_header'] = common_settings.get(
        'scripts', 'alter_table_header')
    settings['alter_table_template'] = common_settings.get(
        'scripts', 'alter_table_template')
    settings['liquibase_master_header'] = common_settings.get(
        'scripts', 'liquibase_master_header')
    settings['liquibase_master_tail'] = common_settings.get(
        'scripts', 'liquibase_master_tail')
    settings['liquibase_master_template'] = common_settings.get(
        'scripts', 'liquibase_master_template')
    settings['liquibase_master_insert_file'] = common_settings.get(
        'scripts', 'liquibase_master_insert_file')
    settings['alter_table_sql_name'] = common_settings.get(
        'scripts', 'out_alter_table_sql_name')
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
                if re.match("[\r\n?|\n]*\s*[set]+", unicode(item).lower()) \
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
                if re.match("[\r\n?|\n]*\s*[set]+", unicode(item).lower()) \
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
            statement = re.sub("\svarchar\(" + num + "(?i)",
                               " LVARCHAR(" + num, statement)
    statement = re.sub("varchar\(256(?i)", "VARCHAR(255", statement)
    return statement


def add_customizations(statement_u):
    table_name = re.search("TABLE(?i)\s*[^\s]*", statement_u)
    if table_name is None:
        return statement_u
    table_name = table_name.group()
    table_name = re.sub("TABLE(?i)\s*", "", table_name)
    if table_name in customizations:
        if 'extent_size' in customizations[table_name] and \
                        'next_size' in customizations[table_name]:
            next_size = customizations[table_name]['next_size']
            extent_size = customizations[table_name]['extent_size']
            statement_u = re.sub(";", "EXTENT SIZE " + str(extent_size) + " NEXT SIZE " + str(next_size) + ";",
                                 statement_u)
    return statement_u


def write_create_table_statement(statement, create_file):
    statement_unicode = unicode(statement)
    statement_unicode = re.sub("\stimestamp(?i)", " DATETIME YEAR TO SECOND",
                               statement_unicode)
    statement_unicode = re.sub("\sdate(?!time )(?i)", " DATETIME YEAR TO DAY",
                               statement_unicode)
    statement_unicode = re.sub("\stime(?!stamp )(?i)",
                               " DATETIME HOUR TO SECOND", statement_unicode)
    statement_unicode = convert_varchars(statement_unicode)
    statement_unicode = re.sub("constraint(?i).*primary key(?i)", "PRIMARY KEY",
                               statement_unicode)
    statement_unicode = re.sub("default nextval\(.*(\:\:regclass)?\)(?i)",
                               "", statement_unicode)
    statement_unicode = re.sub("\sfalse(?i)",
                               " 'f'", statement_unicode)
    statement_unicode = re.sub("\strue(?i)",
                               " 't'", statement_unicode)
    statement_unicode = re.sub("\sbytea(?i)",
                               " BYTE", statement_unicode)
    statement_unicode = add_customizations(statement_unicode)
    create_file.write(statement_unicode)


def write_alter_table_statement(statement, alter_file):
    statement_unicode = unicode(statement)
    template = settings['alter_table_template']
    # Base table name
    base_table_name_match = re.search("alter\s*table\s*[^\s]*",
                                      statement_unicode.lower())
    if base_table_name_match is not None:
        base_table_name = re.search("[^\s]*$",
                                    base_table_name_match.group()).group()
        template = re.sub("token_base_table_name", base_table_name, template)
    # Base column name
    base_column_name_match = re.search("foreign\s*key\s*\([^\)]*",
                                       statement_unicode.lower())
    if base_column_name_match is not None:
        base_column_name = re.search("[^\(]*$",
                                     base_column_name_match.group()).group()
        template = re.sub("token_base_column_names", base_column_name, template)
    # Referenced table name and column
    referenced_table_match = re.search("references\s*[^\s]*\s*\([^\)]*",
                                       statement_unicode.lower())
    if referenced_table_match is not None:
        referenced = referenced_table_match.group().replace("references", "")
        referenced_table_name = re.search("[^\(]*", referenced).group()
        template = re.sub("token_referenced_table_name",
                          referenced_table_name, template)
        referenced_column_name = re.search("[^\(]*$", referenced).group()
        template = re.sub("token_referenced_column_names",
                          referenced_column_name, template)
    # Constraint name
    constraint_name_match = re.search("add\s*constraint\s*[^\s]*",
                                      statement_unicode.lower())
    if constraint_name_match is not None:
        constraint_name = re.search("[^\s]*$",
                                    constraint_name_match.group()).group()
        template = re.sub("token_constraint_name", constraint_name, template)

    alter_file.write(template)
    alter_file.write("\n\n\n")


def file_lines_close_to_limit(file_path):
    count = 0
    if not os.path.exists(file_path):
        return count
    for _ in open(file_path, 'r').xreadlines():
        count += 1
    return count > 800


def toCamelCase(mathobj):
    return mathobj.group(0)[1:].upper()


def normalize_constraint_name(matchobj):
    return re.sub("_([^\s])", toCamelCase, matchobj.group(0))


def write_alter_table_in_sql_file(statement, alter_sql_file):
    statement_u = unicode(statement)
    # statement_u = re.sub("ADD\s*CONSTRAINT(?i)\s*([^\s])*", normalize_constraint_name,
    #                           statement_u)
    statement_u = re.sub("ADD\s*CONSTRAINT(?i)\s*([^\s])*", "ADD CONSTRAINT ",
                               statement_u)
    statement_u = re.sub("ON\s*DELETE(?i).*\s", "", statement_u)
    statement_u = re.sub("ON\s*UPDATE(?i).*\s", "", statement_u)
    statement_u = re.sub("NOT\s*DEFERRABLE(?i).*", ";", statement_u)
    alter_sql_file.write("\n" + statement_u + "\n")


def parse_create_statement(statement, create_file, alter_file, alter_sql_file):
    if re.match("[\r\n?|\n]*\s*[set]+", unicode(statement).lower()) \
            is not None:
        return
    if re.search("create\s*table", unicode(statement).lower()) is not None:
        write_create_table_statement(statement, create_file)
    elif re.search("alter\s*table", unicode(statement).lower()) is not None:
        write_alter_table_statement(statement, alter_file)
        write_alter_table_in_sql_file(statement, alter_sql_file)


def write_alter_table_header(alter_file):
    alter_file.write(settings['alter_table_header'])
    alter_file.write("\n\n\n")


def write_alter_table_tail(alter_file):
    alter_file.write("\n\n\n")
    alter_file.write(settings['alter_table_tail'])


def migrate_create_table():
    logger.debug('-*' * 10 + 'BEGIN: migrate create table' + '-*' * 10 +
                 '\n')
    create_files_number = 1
    out_dir_path = os.path.abspath(settings['out_dir'])
    in_file_path = os.path.abspath(settings['in_dir'] + '/' +
                                   settings['create_table_file_name'])
    create_file_name = os.path.basename(in_file_path)
    alter_file_name = settings['liquibase_alter_table_xml_name']
    alter_sql_file_name = settings['alter_table_sql_name']
    create_file_name = re.sub(".sql", "", create_file_name)
    out_create_file_path = out_dir_path + '/' + create_file_name + '_' + str(create_files_number) + '.sql'
    out_alter_file_path = out_dir_path + '/' + alter_file_name
    out_alter_sql_file_path = out_dir_path + '/' + alter_sql_file_name
    with open(in_file_path, 'r') as in_file:
        raw_data = in_file.read()
        parsed = sqlparse.parse(raw_data)
        with open(out_alter_file_path, 'a') as out_alter_file:
            with open(out_alter_sql_file_path, 'a') as out_alter_sql_file:
                write_alter_table_header(out_alter_file)
                for item in parsed:
                    if file_lines_close_to_limit(out_create_file_path):
                        create_files_number += 1
                        out_create_file_path = out_dir_path + '/' + create_file_name + '_' + str(
                            create_files_number) + '.sql'
                    with open(out_create_file_path, 'a') as out_create_file:
                        parse_create_statement(item, out_create_file,
                                               out_alter_file, out_alter_sql_file)
                write_alter_table_tail(out_alter_file)
    logger.debug('-*' * 10 + 'END: migrate create table' + '-*' * 10 +
                 '\n')


def create_changelog_master():
    logger.debug('-*' * 10 + 'BEGIN: create changelog master' + '-*' * 10 +
                 '\n')
    out_dir_path = os.path.abspath(settings['out_dir'])
    out_file_path = os.path.abspath(settings['out_dir'] + '/' + settings['liquibase_master_xml_name'])
    template = settings['liquibase_master_template']
    with open(out_file_path, 'a') as out_file:
        out_file.write(settings['liquibase_master_header'] + '\n\n\n')
        for f in os.listdir(out_dir_path):
            if not os.path.isfile(out_dir_path + "/" + f):
                continue
            if re.search(settings['liquibase_master_xml_name'], f):
                continue
            if re.search(settings['liquibase_alter_table_xml_name'], f):
                continue
            if re.search(settings['alter_sequence_file_name'], f):
                continue
            if re.search(settings['alter_table_sql_name'], f):
                continue
            include = re.sub("token_file_name", f, template)
            out_file.write(include + '\n')
        alter_include = re.sub("token_file_name", settings['liquibase_alter_table_xml_name'], template)
        out_file.write(alter_include + '\n')
        insert_include = re.sub("token_file_name", settings['liquibase_master_insert_file'], template)
        out_file.write(insert_include + '\n')
        alter_sequence_include = re.sub("token_file_name", settings['alter_sequence_file_name'], template)
        out_file.write(alter_sequence_include + '\n')
        out_file.write('\n\n' + settings['liquibase_master_tail'])

    logger.debug('-*' * 10 + 'END: create changelog maste' + '-*' * 10 +
                 '\n')


def prepare_output_dir():
    out_dir_path = os.path.abspath(settings['out_dir'])
    if os.path.isfile(out_dir_path):
        logger.error('Existence of file: ' + out_dir_path)
        sys.exit(1)
    if os.path.exists(out_dir_path):
        shutil.rmtree(out_dir_path, ignore_errors=True)
    os.makedirs(out_dir_path)


def normalize_size(old_size):
    min_size = 8
    old_size = int(old_size)
    if old_size < min_size:
        return str(min_size)
    y = math.log(old_size, 2)
    if y.is_integer():
        return str(int(math.pow(2, y)))
    y = math.floor(y)
    return str(int(math.pow(2, y + 1)))


def parse_customizations():
    if not os.path.isfile(settings['customizations_file']):
        return
    global customizations
    with open(settings['customizations_file'], 'rb') as customizations_file:
        customizations_dict_file = csv.DictReader(customizations_file, delimiter=';')
        for row in customizations_dict_file:
            new_row = {}
            new_row['extent_size'] = normalize_size(row['initial_size'])
            new_row['next_size'] = normalize_size(row['increment_size'])
            customizations[row['table_name']] = new_row


def migrate_scripts(config_file_path, debug):
    global logger
    logging.basicConfig()
    if debug:
        logger.setLevel(logging.DEBUG)
    parse_config_file(config_file_path)
    validate_parameters()
    parse_customizations()
    prepare_output_dir()
    migrate_create_sequence()
    migrate_alter_sequence()
    migrate_create_table()
    create_changelog_master()
