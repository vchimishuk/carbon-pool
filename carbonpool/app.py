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

import sys
import signal
import threading
import urllib.parse
import socketserver
import http.server
from carbonpool import Log


def term_wait():
    event = threading.Event()

    def handler(sig, frame):
        event.set()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

    event.wait()


class MetricsRequestHandler(socketserver.BaseRequestHandler):
    def handle(self):
        max_size = 4096 * 1024
        data = bytearray()

        while True:
            d = self.request.recv(4096)
            if not d:
                break
            data += d
        s = data.decode('utf-8')
        if s.find('\n') == -1:
            raise ValueError('line is too long')

        ok = self.server.log.append(s)
        if not ok:
            raise ValueError('request is too large')


class MetricsServer(socketserver.ThreadingTCPServer):
    def __init__(self, log, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.log = log


class APIRequestHandler(http.server.BaseHTTPRequestHandler):
    ROUTES = {'/metrics': {'GET': 'get'},
              '/metrics/_offset': {'GET': 'offset'}}

    def do_GET(self):
        self.route()

    def do_POST(self):
        self.route()

    def parse_req(self):
        r = urllib.parse.urlparse(self.path)
        p = {}
        for k, v in urllib.parse.parse_qsl(r.query):
            p[k] = v

        return {'method': self.command, 'path': r.path, 'params': p}

    def route(self):
        req = self.parse_req()

        if req['path'] not in self.ROUTES:
            self.response(404)
        elif self.command not in self.ROUTES[req['path']]:
            self.response(405)
        else:
            handler = self.ROUTES[req['path']][req['method']]
            try:
                code, headers, body = getattr(self, handler)(req)
                self.response(code, headers, body)
            except:
                self.response(500)
                raise

    def response(self, status, headers={}, body=None):
        self.send_response(status)
        self.send_header('Content-type','text/plain')
        for n, v in headers.items():
            self.send_header(n, v)
        self.end_headers()
        if body is not None:
            self.wfile.write(body.encode())

    def get(self, req):
        try:
            offset = int(req['params'].get('offset', -1))
            limit = min(int(req['params'].get('limit', 1000)), 1000)
            if offset < 0:
                raise ValueError('invalid offset')
            if limit < 0:
                raise ValueError('invalid limit')

            lines, new_offset = self.server.log.read(offset, limit)
            if new_offset == -1:
                raise ValueError('invalid offset')

            return 200, {'Offset': new_offset}, ''.join(lines)
        except ValueError:
            return 400, {}, None

    def offset(self, req):
        return 200, {}, str(self.server.log.offset)


class APIServer(http.server.HTTPServer):
    def __init__(self, log, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.log = log


class App:
    def __init__(self, data_dir, seg_size, max_segs, metrics_addr, api_addr):
        self.data_dir = data_dir
        self.seg_size = seg_size
        self.max_segs = max_segs
        self.metrics_addr = metrics_addr
        self.api_addr = api_addr

    def serve_forever(self):
        log = Log(self.data_dir, self.seg_size, self.max_segs)
        log.open()

        try:
            metrics_srv = MetricsServer(log, self.metrics_addr,
                                        MetricsRequestHandler)
            api_srv = APIServer(log, self.api_addr, APIRequestHandler)

            try:
                threading.Thread(target=metrics_srv.serve_forever).start()
                threading.Thread(target=api_srv.serve_forever).start()
                term_wait()
            except KeyboardInterrupt:
                pass
            finally:
                metrics_srv.shutdown()
                api_srv.shutdown()
        finally:
            log.close()
