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
import sys
import threading


def falloc(name, size):
    with open(name, 'w') as f:
        while size > 0:
            n = min(size, 512)
            f.write('\0' * n)
            size -= n


class Segment:
    """Segment represents one data-file.

    The whole log is divided by segments. Every segment physically
    is a file pair: data file (.seg) and offset file (.idx).
    Segment layout:

    +-------+ <-- base
    |1111111|
    |1110000| <-- offset (saved in index file)
    |0000000|
    |0000000|
    +-------+ <-- base + size

    Where base is a log-scope offset value the segment starts at.
    Offset is a current position where next portion of data will be written at.
    """
    def __init__(self, path, base, size):
        name = '{{:0{}}}'.format(len(str(sys.maxsize))).format(base)
        self.sname = os.path.join(path, name) + '.seg'
        self.iname = os.path.join(path, name) + '.idx'
        self.base = base
        self.size = size
        self.offset = None
        self.file = None

    def open(self):
        if os.path.exists(self.iname):
            with open(self.iname, 'r') as f:
                self.offset = int(f.read())
        else:
            self.offset = 0
            self.write_offset(self.offset)

        if not os.path.exists(self.sname):
            falloc(self.sname, self.size)
        self.size = os.path.getsize(self.sname)
        self.file = open(self.sname, 'r+')
        self.file.seek(self.offset, os.SEEK_SET)

    def close(self):
        self.file.close()

    def append(self, s):
        # There is not enough space for the data.
        if self.offset + len(s) > self.size:
            return False

        self.file.write(s)
        self.offset += len(s)
        self.write_offset(self.offset)

        return True

    def read(self, offset, num):
        if offset < 0 or offset > self.offset:
            raise IndexError('offset out of range')

        lines = []
        new_offset = offset
        pos = self.file.tell()

        self.file.seek(offset, os.SEEK_SET)
        while self.file.tell() < self.offset and len(lines) < num:
            l = self.file.readline()
            if len(l):
                lines.append(l)
                new_offset += len(l)
            else:
                break
        self.file.seek(pos, os.SEEK_SET)

        return lines, new_offset

    def write_offset(self, offset):
        with open(self.iname, 'w') as f:
            f.write(str(offset))


class Log:
    def __init__(self, path, size, max_segs):
        self.path = path
        self.size = size
        self.max_segs = max_segs
        self.seg = None
        self.lock = threading.RLock()

    @property
    def offset(self):
        with self.lock:
            return self.seg.base + self.seg.offset

    def open(self):
        base = 0
        files = self.seg_files()
        if len(files):
            base = int(os.path.splitext(files[-1])[0])

        self.seg = Segment(self.path, base, self.size)
        self.seg.open()

    def close(self):
        self.seg.close()

    def append(self, s):
        with self.lock:
            ok = self.seg.append(s)
            if not ok:
                self.seg.close()
                self.seg = self.new_seg(self.seg.base + self.seg.size)
                self.seg.open()
                ok = self.seg.append(s)

        return ok

    def read(self, offset, num):
        lines = []

        with self.lock:
            while len(lines) < num:
                if self.seg.base <= offset < self.seg.base + self.seg.offset:
                    seg_offset = offset - self.seg.base
                    l, n = self.seg.read(seg_offset, num - len(lines))
                    lines += l
                    offset = self.seg.base + n
                    # This is the latest segment, so there is no more data left.
                    break
                elif offset == self.seg.base + self.seg.offset:
                    # There is no more data yet.
                    return [], offset
                elif offset > self.seg.base + self.seg.offset:
                    return [], -1
                else:
                    base = self.find_seg(offset)
                    if base is None:
                        return [], -1

                    seg = Segment(self.path, base, 0)
                    seg.open()
                    try:
                        seg_offset = offset - seg.base
                        l, n = seg.read(seg_offset, num - len(lines))
                        lines += l
                        if n < seg.offset:
                            offset = seg.base + n
                            break
                        else:
                            # The segment is an old one and we ran out of its
                            # content -- lets move to the next one then.
                            offset = seg.base + seg.size
                    finally:
                        seg.close()

        return lines, offset

    def new_seg(self, base):
        segs = self.seg_files()
        if len(segs) >= self.max_segs:
            sname = os.path.join(self.path, segs[0])
            iname = os.path.splitext(sname)[0] + '.idx'
            os.remove(sname)
            os.remove(iname)

        return Segment(self.path, base, self.size)

    def find_seg(self, offset):
        for f in reversed(self.seg_files()):
            base = int(os.path.splitext(f)[0])
            if base <= offset:
                return base

        return None

    def seg_files(self):
        files = []
        for f in os.listdir(self.path):
            if f.endswith('.seg'):
                files.append(f)

        return sorted(files)
