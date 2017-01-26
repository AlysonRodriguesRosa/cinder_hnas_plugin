import os

from tempest import config
from tempest.test_discover import plugins
from cinder_hnas_plugin import config as hnas_config


class HNASPlugin(plugins.TempestPlugin):
    def get_opt_lists(self):
        return [hnas_config.hnas_group.name,
                hnas_config.HNASGroup]

    def load_tests(self):
        base_path = os.path.split(os.path.dirname(
            os.path.abspath(__file__)))[0]
        test_dir = "cinder_hnas_plugin/tests"
        full_test_dir = os.path.join(base_path, test_dir)
        return full_test_dir, base_path

    def register_opts(self, conf):
        config.register_opt_group(
            conf,
            hnas_config.hnas_group,
            hnas_config.HNASGroup)
