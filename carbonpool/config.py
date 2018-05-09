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

def parse_config(content):
    cfg = {}

    for i, l in enumerate(content.split('\n'), start=1):
        n = l.find('#')
        if n != -1:
            l = l[:n]
        l = l.strip()

        if l:
            parts = l.split('=', maxsplit=1)
            if len(parts) != 2:
                raise ValueError('invalid syntax at line {}'.format(i))
            name = parts[0].strip()
            val = parts[1].strip()
            cfg[name] = val

    return cfg
