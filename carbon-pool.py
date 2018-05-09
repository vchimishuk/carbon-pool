#!/bin/env python3

# Copyright 2018 Viacheslav Chimishuk <vchimishuk@yandex.ru>
#
# This file is part of carbon-pool.
#
# carbon-pool is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# carbon-pool is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with carbon-pool. If not, see <http://www.gnu.org/licenses/>.

import re
import sys
import argparse
import carbonpool


PROG_NAME = "carbon-pool"
VERSION = (0, 1, 0)


def print_version():
    v = "{}.{}.{}".format(*VERSION)
    print("{} {}".format(PROG_NAME, v))


def fatal(fmt, *args):
    print('{}: {}'.format(PROG_NAME, fmt.format(*args)), file=sys.stderr)
    sys.exit(1)


def size_to_bytes(size):
    if re.match(r'^\d+$', size):
        return int(size)
    elif re.match(r'^\d+B$', size, re.I):
        return int(size[:-1])
    elif re.match(r'^\d+K$', size, re.I):
        return int(size[:-1]) * 1024
    elif re.match(r'^\d+M$', size, re.I):
        return int(size[:-1]) * 1024 * 1024
    elif re.match(r'^\d+G$', size, re.I):
        return int(size[:-1]) * 1024 * 1024 * 1024
    else:
        raise ValueError('invalid size format')


ap = argparse.ArgumentParser(prog=PROG_NAME,
                             description='Save Graphite metrics to disk')
ap.add_argument('-c', '--config', default='/etc/carbon-pool.conf',
                help='configuration file name')
ap.add_argument('-V', '--version', action='store_true',
                help='output version information and exit')

args = ap.parse_args()

if args.version:
    print_version()
    sys.exit(0)

try:
    content = ''
    with open(args.config, 'r') as f:
        content = f.read()

    cfg = carbonpool.parse_config(content)
except OSError as e:
    fatal('failed to read configuration file: {}'.format(e))
except ValueError as e:
    fatal('failed to parse configuration file: {}'.format(e))

data_dir = cfg.get('data-dir', '/var/lib/carbon-pool')
seg_size = size_to_bytes(cfg.get('segment-size', '16M'))
max_segs = int(cfg.get('max-segments', '10'))
metrics_addr = (cfg.get('metrics-addr', '127.0.0.1'),
                int(cfg.get('metrics-port', '2003')))
api_addr = (cfg.get('api-addr', '127.0.0.1'),
            int(cfg.get('api-port', '2002')))

app = carbonpool.App(data_dir, seg_size, max_segs, metrics_addr, api_addr)
app.serve_forever()
