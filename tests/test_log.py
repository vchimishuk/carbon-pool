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

import os
import re
import uuid
import tempfile
import unittest
import carbonpool


def xor(s):
    x = 0
    for b in s.encode('utf-8'):
        x ^= b

    return x


class TestLog(unittest.TestCase):
    def test_append_read(self):
        data_dir = tempfile.mkdtemp('-carbonpool-tests')
        write_hash = 0
        log = carbonpool.Log(data_dir, 1024, 10)
        log.open()
        try:
            for i in range(100):
                s = str(uuid.uuid4()) + '\n'
                write_hash ^= xor(s)
                log.append(s)
        finally:
            log.close()

        read_hash = 0
        log = carbonpool.Log(data_dir, 1024, 10)
        log.open()
        try:
            offset = 0
            while True:
                lines, offset = log.read(offset, 10)
                self.assertTrue(offset > 0)
                if not lines:
                    break
                for l in lines:
                    read_hash ^= xor(l)
        finally:
            log.close()

        self.assertEqual(write_hash, read_hash)

    def test_delete_old_segments(self):
        data_dir = tempfile.mkdtemp('-carbonpool-tests')

        log = carbonpool.Log(data_dir, 1024, 3)
        log.open()
        try:
            for i in range(200):
                log.append(str(uuid.uuid4()) + '\n')
        finally:
            log.close()

        segs = set()
        for s in os.listdir(data_dir):
            m = re.match(r'(\d+)\.seg', s)
            if m:
                segs.add(int(m.group(1)))

        self.assertEqual(segs, {5120, 6144, 7168})
