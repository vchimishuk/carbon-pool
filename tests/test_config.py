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

import unittest
from carbonpool import parse_config


class TestParseConfig(unittest.TestCase):
    def test_empty(self):
        self.assertEqual({}, parse_config(''))

    def test_comments(self):
        content = ('# A comment.\n'
                   '\n'
                   '# Another comment line.\n')
        self.assertEqual({}, parse_config(''))

    def test_valid(self):
        content = ('# Configuration file example\n'
                   'key-1 = value-1\n'
                   'key-2=value-2\n'
                   'key-3  =  value=3')
        exp = {'key-1': 'value-1', 'key-2': 'value-2', 'key-3': 'value=3'}
        self.assertEqual(exp, parse_config(content))

    def test_invalid(self):
        content = ('# First line\n'
                   'invalid-line\n'
                   'another-invalid-line\n')

        with self.assertRaises(ValueError) as e:
            parse_config(content)
        self.assertEqual('invalid syntax at line 2', str(e.exception))
