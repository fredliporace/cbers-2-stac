"""utils_test"""

import unittest

from sam.process_new_scene_queue.utils import get_collection_ids, \
    get_collection_s3_key

class UtilsTest(unittest.TestCase):
    """UtilsTest"""

    def test_collection_utils(self):
        """test_collection_utils"""

        collections = get_collection_ids()
        self.assertEqual(len(collections), 4)

        mux_prefix = get_collection_s3_key('CBERS4MUX')
        self.assertEqual(mux_prefix, 'CBERS4/MUX/collection.json')

if __name__ == '__main__':
    unittest.main()
