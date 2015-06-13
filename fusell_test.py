from nose.tools import *

from fusell import *


def test_setattr_mask_to_list():
    assert_equal(setattr_mask_to_list((1 << len(FUSE_SET_ATTR)) - 1), FUSE_SET_ATTR)
    assert_equal(setattr_mask_to_list(5), ('st_mode', 'st_gid'))
