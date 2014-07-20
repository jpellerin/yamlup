import yaml
import unittest

import yamlup


LOCAL = """\
cluster_name: "My Cluster"
num_tokens: 256
data_file_directories:
 - /my/cassandra/data/
memtable_flush_queue_size: 4
#my_default_property: "foo"
my_uncommented_property: "baz"
"""

NEW = """\
cluster_name: "Test Cluster"
num_tokens: 256
data_file_directories:
 - /var/lib/cassandra/data
rpc_server_type: sync
#my_default_property: "foo"
#my_uncommented_property: "bar"
"""


class TestMergeConfigs(unittest.TestCase):
    def setUp(self):
        self.local = yaml.load(LOCAL)
        self.new = yaml.load(NEW)

    def test_no_orig(self):
        expected_merge = {
            'cluster_name': 'My Cluster',
            'num_tokens': 256,
            'data_file_directories': [
                '/my/cassandra/data/',
                '/var/lib/cassandra/data'
            ],
            'memtable_flush_queue_size': 4,
            'my_uncommented_property': 'baz',
            'rpc_server_type': 'sync'
        }
        expected_questionables = {
            'cluster_name': ['My Cluster', 'Test Cluster'],
            'data_file_directories.1': [None,
                                        '/var/lib/cassandra/data']
        }
        merge, qs = yamlup.merge_configs(self.local, self.new)
        self.assertDictEqual(merge, expected_merge)
        self.assertDictEqual(qs, expected_questionables)

    def test_orig_matches_new(self):
        orig = self.new
        expected_merge = {
            'cluster_name': 'My Cluster',
            'num_tokens': 256,
            'data_file_directories': [
                '/my/cassandra/data/'
            ],
            'memtable_flush_queue_size': 4,
            'my_uncommented_property': 'baz',
            'rpc_server_type': 'sync'
        }
        expected_questionables = {}
        merge, qs = yamlup.merge_configs(self.local, self.new, orig)
        self.assertDictEqual(merge, expected_merge)
        self.assertDictEqual(qs, expected_questionables)

    def test_orig_matches_local(self):
        orig = self.local
        expected_merge = {
            'cluster_name': 'Test Cluster',
            'num_tokens': 256,
            'data_file_directories': [
                '/var/lib/cassandra/data'
            ],
            'memtable_flush_queue_size': 4,
            'my_uncommented_property': 'baz',
            'rpc_server_type': 'sync'
        }
        expected_questionables = {}
        merge, qs = yamlup.merge_configs(self.local, self.new, orig)
        self.assertDictEqual(merge, expected_merge)
        self.assertDictEqual(qs, expected_questionables)


class TestMergeRealisticConfigs(unittest.TestCase):
    def setUp(self):
        self.old = yaml.load(open('test_support/cassandra-1.2.12.yaml'))
        self.new = yaml.load(open('test_support/cassandra-2.0.3.yaml'))

    def test_merge(self):
        merged, questionable = yamlup.merge_configs(self.old, self.new)
        # checks that identical complex values aren't duplicated
        self.assertEqual(len(merged['seed_provider']), 1)
