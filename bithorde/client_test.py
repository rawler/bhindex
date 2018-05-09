from nose.tools import *

from contextlib import closing
from StringIO import StringIO
from thread_io import spawn, listen, Promise

from .client import Client, Connection
from . import message


class Server(object):
    def __init__(self, name="Server"):
        super(Server, self).__init__()
        self.s = listen(('', 0), backlog=1)
        self.name = "Server"
        self.res = Promise()
        spawn(self.run)

    def addr(self):
        return '%s:%d' % self.s.getsockname()

    def run(self):
        with closing(self.s):
            sock, _ = self.s.accept()
        self.res.set(Connection(sock, self.name))

    def wait(self):
        return self.res.wait()


def test_connection():
    s = Server()
    c1 = Connection(s.addr(), "Monkey")
    c2 = s.wait()
    assert_equals(c1.peername, "Server")
    assert_equals(c2.peername, "Monkey")


class TestClient(object):
    ids = [message.Identifier(
        type=message.TREE_TIGER,
        id='g"\xb6=\x1a8\x97i\xd9\xb3\xfd\xe0\xd0R"\xd8\xc9D\x0e\xc0"\xaeLQ')
    ]

    def setup(self):
        s = Server()
        self.c = Client(dict(
            address=s.addr(),
            myname="Client",
            pressure=1,
        ))
        self.s = s.wait()

    def test_connect(self):
        assert_equals(self.s.peername, "Client")
        assert_equals(self.c._connection.peername, "Server")

    def test_upload(self):
        def server_side():
            bind = self.s.next()
            assert_equals(bind.size, 130 * 1024)
            self.s.send(message.AssetStatus(status=message.SUCCESS))
            uploaded = 0
            for seg in self.s:
                if uploaded < 128 * 1024:
                    assert_equals(len(seg.content), 64 * 1024)
                    uploaded += len(seg.content)
                else:
                    assert_equals(len(seg.content), 2 * 1024)
                    self.s.send(message.AssetStatus(
                        status=message.SUCCESS,
                        ids=self.ids,
                        size=130 * 1024,
                    ))
                    break
        spawn(server_side)

        f = StringIO("A" * 130 * 1024)
        status = self.c.upload(f)
        assert_items_equal(status.ids, self.ids)
        assert_equal(status.size, 130 * 1024)
