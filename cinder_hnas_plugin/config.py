from oslo_config import cfg
from oslo_config import types as oslo_types
from tempest import config

HNASGroup = [
    cfg.ListOpt(name="enabled_backends",
                item_type=oslo_types.String(),
                help="Analogous to the cinder option of the same name.")
]

hnas_group = cfg.OptGroup(name='hnas',
                          title='HNAS Cinder Tempest Backend')

CONF = config.CONF

