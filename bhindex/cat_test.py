from nose.tools import *

from mock import Mock
from hashlib import sha1

from db import DB
from .cat import Cat, NotFoundError
from .tree import Filesystem, NotFoundError as FNotFound, Path as P
from .bithorde import proto


# 'tree:tiger:MUACEID6UTVUKTRE2MTZKOPTZTMS6A2OF6B4ZNY', or 1MB of zeroes
test_asset_ids = proto.Identifier(
    type=proto.TREE_TIGER,
    id="e\000\" ~\244\353EN$\323\'\2259\363\314\331/\003N/\203\314\267",
)


def test_catter():
    fs = Filesystem(DB(':memory:'))
    fs.root().add_file('zeroes', [test_asset_ids])

    asset = Mock()
    asset.__enter__ = Mock(return_value=asset)
    asset.__exit__ = Mock(return_value=False)
    asset.status = Mock(return_value=proto.AssetStatus(size=1024*1024, status=proto.SUCCESS))
    asset.__iter__ = Mock(return_value=('\x00'*(128*1024) for _ in range(8)))

    bithorde = Mock()
    bithorde.open = Mock(return_value=asset)
    cat = Cat(fs, bithorde)

    sha = sha1()
    for chunk in cat(P('zeroes')):
        sha.update(chunk)
    assert_equals(sha.hexdigest(), '3b71f43ff30f4b15b5cd85dd9e95ebc7e84eb5a3')

    with assert_raises(FNotFound):
        for chunk in cat(P('NON-EXISTING')):
            pass

    asset.status = Mock(return_value=proto.AssetStatus(status=proto.NOTFOUND))
    with assert_raises(NotFoundError):
        for chunk in cat(P('zeroes')):
            pass
