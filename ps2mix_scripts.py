#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ConfigParser


settings = {}


def validate_parameters():
    print '-*' * 10 + 'BEGIN: validating parameters' + '-*' * 10 + '\n'
    global settings
    print settings['in_dir']
    print settings['out_dir']
    print '-*' * 10 + 'END: validating parameters' + '-*' * 10 + '\n'


def parse_config_file():
    print '-*' * 10 + 'BEGIN: parse conf' + '-*' * 10 + '\n'
    specific_settings = ConfigParser.ConfigParser()
    specific_settings.read(['config'])

    common_settings = ConfigParser.ConfigParser()
    common_settings.read(['common'])

    global settings
    settings['in_dir'] = specific_settings.get('scripts', 'input_directory')
    settings['out_dir'] = specific_settings.get('scripts', 'output_directory')
    print '-*' * 10 + 'END: parse conf' + '-*' * 10 + '\n'


def migrate_scripts():
    parse_config_file()
    validate_parameters()
