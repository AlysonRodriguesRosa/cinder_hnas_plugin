from oslo_log import log as logging
from tempest import config
from tempest import test

CONF = config.CONF
LOG = logging.getLogger(__name__)


class BaseHNASPluginTest(test.BaseTestCase):

    @classmethod
    def skip_checks(cls):
        pass
