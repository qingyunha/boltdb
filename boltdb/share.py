import struct
from collections import namedtuple


branchPageFlag = 0x01
leafPageFlag = 0x02
metaPageFlag = 0x04
freelistPageFlag = 0x10

bucketLeafFlag = 0x01


page_tuple = namedtuple('page', 'id flags count overflow')
page_struct = struct.Struct("QHHI")

meta_tuple = namedtuple('meta', 'magic version pageSize flags root_pgid sequence freelist max_pgid txid checksum')
meta_struct = struct.Struct("IIIIQQQQQQ")

leaf_elem_tuple = namedtuple("leaf_elem", "flags pos ksize vsize")
leaf_elem_struct = struct.Struct("IIII")

branch_elem_tuple = namedtuple("branch_elem", "pos ksize pgid")
branch_elem_struct = struct.Struct("IIQ")

bucket_tuple = namedtuple("bucket", "root dequence")
bucket_struct = struct.Struct("QQ")
