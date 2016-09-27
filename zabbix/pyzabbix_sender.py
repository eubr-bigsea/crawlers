#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2011 Enrico Tröger <enrico(dot)troeger(at)uvena(dot)de>
# License: GNU GPLv2

"""
Minimal implementation of the Zabbix sender protocol in Python.
It should just show the basic idea, there is no error handling at all.
"""
import json
import socket
import struct
import re

_expr_info = re.compile(r'Processed \d+ Failed (\d+).+')

def get_zabbix_server(config_file='/etc/zabbix/zabbix_agentd.conf'):
	server = None
	with open(config_file) as f:
		for line in f:
			if line[:7] == 'Server=':
				server = line[7:].strip()
	return server

def send(host, key, value, zabbix_server = '127.0.0.1', port = 10051, ):
	HEADER = '''ZBXD\1%s%s'''
	# just some data
	data = '''{{"request":"sender data", "data":[ {{ "host":"{}", "key":"{}", "value":"{}"}} ] }} '''.format(host, key, value)
	
	data_length = len(data)
	data_header = struct.pack('i', data_length) + '\0\0\0\0'
	
	data_to_send = HEADER % (data_header, data)
	
	# here really should come some exception handling
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.connect((zabbix_server, port))
	
	# send the data to the server
	sock.send(data_to_send)
	
	# read its response, the first five bytes are the header again
	response_header = sock.recv(5)
	if not response_header == 'ZBXD\1':
		raise ValueError('Got invalid response')
	
	# read the data header to get the length of the response
	response_data_header = sock.recv(8)
	response_data_header = response_data_header[:4] # we are only interested in the first four bytes
	response_len = struct.unpack('i', response_data_header)[0]
	
	# read the whole rest of the response now that we know the length
	response_raw = sock.recv(response_len)
	
	sock.close()
	
	response = json.loads(response_raw)
	return _expr_info.findall(response['info'])[0]
