# Copyright 2016 Hitachi, Ltd.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from oslo_log import log as logging
from tempest import config
from tempest.lib.common.utils import test_utils
from tempest.scenario import manager

from cinder_hnas_plugin.tests.utils import data_utils
from cinder_hnas_plugin.tests.utils import waiters
from cinder_hnas_plugin.tests.utils import clients
import time

CONF = config.CONF
LOG = logging.getLogger(__name__)


class BaseHNASTest(manager.ScenarioTest):

    credentials = ['primary', 'admin']

    @classmethod
    def setup_clients(cls):
        super(BaseHNASTest, cls).setup_clients()
        if CONF.volume_feature_enabled.api_v1:
            cls.admin_volume_types_client = cls.os_adm.volume_types_client
            cls.quotas_client = cls.os.volume_quotas_client
        else:
            cls.admin_volume_types_client = cls.os_adm.volume_types_v2_client
            cls.quotas_client = cls.os.volume_quotas_v2_client

    @classmethod
    def resource_setup(cls):
        super(BaseHNASTest, cls).resource_setup()
        cls.tenant_id = cls.quotas_client.tenant_id

    def setUp(self):
        super(BaseHNASTest, self).setUp()
        backends = clients.HNASCinderBackend.create_backends_from_conf()
        self.hnas_backends = backends

        # create a volume type for each backend. The name of the volume type
        # will be the backend's volume_backend_name
        for backend in backends:
            backend.vtype = []
            for pool_name in backend.svc_pool_names:
                vtype = self.create_volume_type(
                    name=backend.name,
                    volume_backend_name=backend.volume_backend_name,
                    service_label=pool_name)
                backend.vtype.append(vtype)

    def create_volume_type(self, client=None, name=None,
                           volume_backend_name=None, service_label=None):
        if not client:
            client = self.admin_volume_types_client
        if not name:
            name = 'generic'
        randomized_name = data_utils.rand_name('scenario-type-' + name)
        LOG.debug("Creating a volume type: %s", randomized_name)
        vtype = client.create_volume_type(
            name=randomized_name)['volume_type']
        self.assertIn('id', vtype)
        self.addCleanup(client.delete_volume_type, vtype['id'])

        extra_specs_dict = {}
        if volume_backend_name:
            extra_specs_dict['volume_backend_name'] = volume_backend_name
        if service_label:
            extra_specs_dict['service_label'] = service_label
        if extra_specs_dict:
            client.create_volume_type_extra_specs(
                vtype['id'], extra_specs_dict)

        return vtype

    def vol_exists_in_some_backend(self, vol_id):
        for backend in self.hnas_backends:
            vol_ref = backend.get_volume_reference(vol_id)
            if self.retry(vol_ref.exists):
                return vol_ref
        else:
            return False

    def vol_exists_in_cinder(self, vol_id):
        vol_list = self.volumes_client.list_volumes()['volumes']
        vol_ids = [vol['id'] for vol in vol_list]
        return vol_id in vol_ids

    def create_instance_and_client(self, create_backing_vol=False,
                                   volume_size=None, source_uuid=None,
                                   source_type='image',
                                   delete_vol_on_termination=True):
        """Creates a test nova instance.

        :param create_backing_vol: Boolean. Determines whether to create a
            cinder volume to back this instance's storage.
        :param volume_size: int. Size used in cinder volume created. If it is
            None, the size declared in .conf is used.
        :param source_uuid: string. The UUID of the image that will be used.
        :param source_type: string. Source of cinder volume that will be
            created. Can be another 'volume', 'image' or from a 'snapshot'.
        :param delete_vol_on_termination: Bool. Wheter to delete the cinder
            volume that was automatically created when spawning the VM once
            such VM is destroyed.
        :returns: Tuple<instance, ssh_client>. An object representing the VM
            as well as an ssh connection to it.
        """
        self.assertTrue(source_type in ['volume', 'image', 'snapshot'])

        keypair = self.create_keypair()
        security_group = self._create_security_group()
        security_groups = [{'name': security_group['name']}]

        kwargs = {}
        bd_map_v2 = {}

        bd_map_v2['destination_type'] = (
            'volume' if create_backing_vol else 'local')

        if source_uuid is None:
            source_uuid = CONF.compute.image_ref

        # image_id must be a valid IMAGE uuid even if we're booting from a
        # volume or a snapshot, otherwise nova complains. The functions down
        # the stack will convert None to the default image if we pass None
        # in image_id
        image_id = source_uuid if source_type == 'image' else None
        bd_map_v2['uuid'] = source_uuid

        if volume_size is None:
            volume_size = CONF.volume.volume_size
        bd_map_v2['volume_size'] = volume_size

        bd_map_v2['source_type'] = source_type
        bd_map_v2['boot_index'] = 0
        bd_map_v2['delete_on_termination'] = delete_vol_on_termination
        kwargs['block_device_mapping_v2'] = [bd_map_v2]

        LOG.debug("Creating test server...")
        instance = self.create_server(
            image_id=image_id,
            flavor=CONF.compute.flavor_ref,
            key_name=keypair['name'],
            security_groups=security_groups,
            config_drive=CONF.compute_feature_enabled.config_drive,
            wait_until='ACTIVE',
            **kwargs)
        fip = self.create_floating_ip(instance)['ip']
        LOG.debug("Creating an ssh connection to vm...")
        ssh_client = self.get_remote_client(
            ip_address=fip,
            username=CONF.validation.image_ssh_user,
            private_key=keypair['private_key'])
        return instance, ssh_client

    def delete_instance(self, inst_id):
        LOG.debug("Deleting instance %s.", inst_id)
        self.servers_client.delete_server(inst_id)
        waiters.wait_for_server_termination(self.servers_client, inst_id)
        LOG.info("Deleted instance %s.", inst_id)

    def extend_volume(self, hnas_vol_ref, new_size):
        vol_id = hnas_vol_ref.uuid
        self.volumes_client.extend_volume(vol_id, new_size=new_size)
        waiters.wait_for_volume_status(self.volumes_client,
                                       vol_id, 'available')
        volume = self.volumes_client.show_volume(vol_id)['volume']
        vol_ref = hnas_vol_ref.hnas_backend.get_volume_reference(vol_id)

        return volume, vol_ref

    def create_volume(self, hnas_backend, size=None, name=None,
                      snapshot_id=None, imageRef=None, source_volid=None,
                      idx_type=0):
        """Creates a cinder volume and HNAS vol reference.

        :param hnas_backend: HNASCinderBackend. An object representing the HNAS
            backend in which the volume will be created.
        :param size: int. Size in GB of the volume to be created. Defaults to
            CONF.volume.volume_size
        :param name: string. The display name of the volume. Defaults to a
            random string.
        :param snapshot_id: string. The UUID of the volume snapshot that will
            be used to create the volume.
        :param imageRef: string. The UUID of the image that will be used to
            create the volume
        :param source_volid: string. The UUID of the volume from which this
            volume is to be cloned.
        :param idx_type: int. Since vtypes is a list of types created in
            Cinder, using idx_type it can be determined which type should be
            used. Default is 0 (so, using the first type in list).
        :returns: tuple<volume, HNASVolumeReference> A tuple containing the
            volume dict as returned by cinder and a reference to its backing
            file in HNAS.
        """
        kwargs = {}

        if name is None:
            name = data_utils.rand_name(self.__class__.__name__)
        kwargs['display_name'] = name

        if size is None:
            size = CONF.volume.volume_size
        kwargs['size'] = size

        if snapshot_id is not None:
            kwargs['snapshot_id'] = snapshot_id

        if imageRef is not None:
            kwargs['imageRef'] = imageRef

        kwargs['volume_type'] = hnas_backend.vtype[idx_type]['name']

        if source_volid is not None:
            kwargs['source_volid'] = source_volid

        volume = self.volumes_client.create_volume(**kwargs)['volume']

        self.addCleanup(self.volumes_client.wait_for_resource_deletion,
                        volume['id'])
        self.addCleanup(test_utils.call_and_ignore_notfound_exc,
                        self.volumes_client.delete_volume, volume['id'])

        # NOTE(e0ne): Cinder API v2 uses name instead of display_name
        if 'display_name' in volume:
            self.assertEqual(name, volume['display_name'])
        else:
            self.assertEqual(name, volume['name'])
        waiters.wait_for_volume_status(self.volumes_client,
                                       volume['id'], 'available')
        # The volume retrieved on creation has a non-up-to-date status.
        # Retrieval after it becomes active ensures correct details.
        volume = self.volumes_client.show_volume(volume['id'])['volume']
        vol_ref = hnas_backend.get_volume_reference(volume['id'])

        return volume, vol_ref

    def unmanage_volume(self, hnas_vol_ref, vol):
        """Unmanages a volume and checks if it still lives in HNAS"""

        self.volumes_client.unmanage_volume(vol['id'])
        self.volumes_client.wait_for_resource_deletion(vol['id'])
        hnas_vol_ref.update_volume_path()
        # Add cleanup via ssc in case the tests fails before the volume
        # gets remanaged and deleted.
        self.addCleanup(hnas_vol_ref.rm_via_ssc)
        LOG.debug("Unmanaged vol path is: %s", hnas_vol_ref.unix_path)

    def unmanage_snapshot(self, hnas_snap_ref):
        """Unmanages a snapshot and checks if it still lives in HNAS"""

        self.manager.snapshots_v3_client.unmanage_snapshot(hnas_snap_ref.uuid)
        self.manager.snapshots_v3_client.wait_for_resource_deletion(
            hnas_snap_ref.uuid)
        hnas_snap_ref.update_volume_path()
        # Add cleanup via ssc in case the tests fails before the snap
        # gets remanaged and deleted.
        self.addCleanup(hnas_snap_ref.rm_via_ssc)
        LOG.info("Unmanaged snap path is: %s.", hnas_snap_ref.unix_path)

    def manage_volume(self, hnas_backend, hnas_vol_ref, svc_idx=0):
        vol_name = data_utils.rand_name('managed-vol-')
        vol_nfs_path = hnas_vol_ref.get_nfs_url(svc_idx)
        vol_ref = {"source-name": vol_nfs_path}
        cinder_vol_type = hnas_backend.vtype[svc_idx]['id']
        LOG.debug("Trying to remanage volume with path: %s", vol_nfs_path)
        vol = self.volumes_client.manage_volume(
            host='%(hostname)s@%(backend_name)s#%(pool)s' %
                 {'hostname': hnas_backend.cinder_manage_host,
                  'backend_name': hnas_backend.name,
                  'pool': hnas_backend.svc_pool_names[svc_idx]},
            vol_reference=vol_ref,
            name=vol_name,
            volume_type=cinder_vol_type)['volume']
        LOG.debug("Volume is to be remanaged with id %s", vol['id'])
        # If the manage call has been successful, we need to delete the
        # new volume that will be registered with cinder. There will still
        # be an old cleanup function for the unmanaged volume, but that'll
        # ignore "not found exception" anyway.
        self.addCleanup(self.volumes_client.wait_for_resource_deletion,
                        vol['id'])
        self.addCleanup(test_utils.call_and_ignore_notfound_exc,
                        self.volumes_client.delete_volume, vol['id'])

        waiters.wait_for_volume_status(self.volumes_client,
                                       vol['id'], 'available')

        new_vol_ref = hnas_backend.get_volume_reference(vol['id'])
        return vol, new_vol_ref

    def manage_snapshot(self, parent_vol_id, hnas_snap_ref, svc_idx=0):
        hnas_backend = hnas_snap_ref.hnas_backend
        snap_name = data_utils.rand_name('managed-snap-')
        snap_nfs_path = hnas_snap_ref.get_nfs_url(svc_idx)
        snap_ref = {"source-name": snap_nfs_path}
        LOG.debug("Trying to remanage snapshot with path: %s", snap_nfs_path)
        snap_resp = self.manager.snapshots_v3_client.manage_snapshot(
            volume_id=parent_vol_id,
            snap_ref=snap_ref,
            name=snap_name)
        snap = snap_resp['snapshot']
        LOG.debug("Snapshot is to be remanaged with id %s", snap['id'])
        # If the manage call has been successful, we need to delete the
        # new snapshot  that will be registered with cinder. There will still
        # be an old cleanup function for the unmanaged snapshot, but that'll
        # ignore "not found exception" anyway.
        self.addCleanup(
            self.manager.snapshots_v3_client.wait_for_resource_deletion,
            snap['id'])
        self.addCleanup(test_utils.call_and_ignore_notfound_exc,
                        self.manager.snapshots_v3_client.delete_snapshot,
                        snap['id'])

        waiters.wait_for_snapshot_status(self.manager.snapshots_v3_client,
                                         snap['id'], 'available')

        new_vol_ref = hnas_backend.get_volume_reference(snap['id'])
        return snap, new_vol_ref

    def create_snapshot_from_volume(self, hnas_volume_ref):
        vol_id = hnas_volume_ref.uuid
        LOG.info("Creating snapshot from volume %s.", vol_id)

        snap = self.snapshots_client.create_snapshot(volume_id=vol_id,
                                                     force=True)['snapshot']

        self.addCleanup(self.snapshots_client.wait_for_resource_deletion,
                        snap['id'])
        self.addCleanup(test_utils.call_and_ignore_notfound_exc,
                        self.snapshots_client.delete_snapshot, snap['id'])
        LOG.info("Waiting for it to be ready...")
        waiters.wait_for_snapshot_status(self.snapshots_client,
                                         snap['id'], 'available')
        LOG.info("Snapshot %s creation done.", snap['id'])
        snap_reference = hnas_volume_ref.hnas_backend.get_volume_reference(
            snap['id'])
        self.assertTrue(self.retry(snap_reference.exists))

        return snap, snap_reference

    def snap_exists_in_cinder(self, snap_id):
        snap_list = self.snapshots_client.list_snapshots()['snapshots']
        snap_ids = [snap['id'] for snap in snap_list]
        return snap_id in snap_ids

    def delete_volume(self, hnas_vol_ref):
        return self._delete_block_entity(hnas_vol_ref, is_snapshot=False)

    def delete_snapshot(self, hnas_snap_ref):
        return self._delete_block_entity(hnas_snap_ref, is_snapshot=True)

    def _delete_block_entity(self, hnas_vol_ref, is_snapshot):
        """Deletes a volume or snapshot and checks if it disappeared from HNAS.

        Since volumes and snapshots are represented by the same entity within
        HNAS, this function handles both snapshots and volumes.
        This is not a cleanup function. It is supposed to be used to test
        proper deletion of snapshots.

        :param hnas_vol_ref: An instance of HNASVolumeReference, representing
            the cinder volume or snapshot that is to be deleted.
        :param is_snapshot: switch between snapshot or volume deletion.
        """
        if is_snapshot:
            del_func = self.snapshots_client.delete_snapshot
            del_waiter = self.snapshots_client.wait_for_resource_deletion
        else:
            del_func = self.volumes_client.delete_volume
            del_waiter = self.volumes_client.wait_for_resource_deletion

        test_utils.call_and_ignore_notfound_exc(del_func, hnas_vol_ref.uuid)
        del_waiter(hnas_vol_ref.uuid)
        LOG.info("Deleted volume %s.", hnas_vol_ref.uuid)

    def verify_volume_writable(self, ssh_client, vol_ref, test_string=None):
        """Writes something to a volume and checks for matching data in HNAS.

        :param instance: dict. An instance dictionary as returned by
            self.create_instance_and_client or self.create_server.
        :param ssh_client: RemoteClient. An ssh client as returned by
            self.get_remote_client or self.create_instance_and_client.
        :param vol_ref: HNASVolumeReference. An object representing the volume
            residing in HNAS that is to be cloned.
        :param test_string: string. Some string that will be write in cinder
            volume as test.
        :returns: string. The string that was written into the volume.
        """

        if test_string is None:
            test_string = data_utils.rand_name('hnas-write-test-')
        blk_dev_tester = clients.InstanceBlockDevTester(
            ssh_client,
            CONF.compute.volume_device_name)
        LOG.debug("Writing data to volume........")
        blk_dev_tester.write_to_top_of_block_dev(test_string)

        LOG.debug("Checking if we have, within HNAS, the same thing we "
                  "have in the vm....")
        s = vol_ref.get_first_bytes(len(test_string))
        self.assertEqual(test_string, s,
                         ('Data written to volume ("%s") from within the '
                          'instance does not match data as read from HNAS '
                          '("%s")' % (test_string, s)))
        return test_string

    def upload_volume_to_image(self, vol):
        """Creates a Glance image from a volume.

        :param vol: string. The UUID of the volume to be used.
        :returns: string. The UUID of the image that was created.
        """
        image_name = data_utils.rand_name(self.__class__.__name__ + '-Image')
        body = self.volumes_client.upload_volume(
            vol['id'], image_name=image_name,
            disk_format=CONF.volume.disk_format)['os-volume_upload_image']

        image_id = body["image_id"]

        self.addCleanup(test_utils.call_and_ignore_notfound_exc,
                        self.image_client.delete_image,
                        image_id)

        waiters.wait_for_image_status(self.image_client, image_id, 'active')
        waiters.wait_for_volume_status(self.volumes_client, vol['id'],
                                       'available')

        return image_id

    def delete_image(self, img_ref):
        self.image_client.delete_image(img_ref)
        self.image_client.wait_for_resource_deletion(img_ref)
        LOG.debug("Deleted image %s", img_ref)

    def retry(self, func, expect_success=True, num_retries=10, wait_secs=15,
              *params):
        r = not expect_success
        while True:
            try:
                r = func(*params)
                if r and expect_success:
                    return r
                if not r and not expect_success:
                    return r
                LOG.debug(("(retry) <%s> returned <%s> but we expected "
                           "a %s value"),
                          func.__name__, r, expect_success)
            except Exception as e:
                if not expect_success:
                    return False

                LOG.debug("(retry) <%s> raised an exception.", func.__name__)
                LOG.exception(e)
            num_retries -= 1
            LOG.debug("(retry) Retrying in %s sec (%s tries left)...",
                      wait_secs, num_retries)
            time.sleep(wait_secs)
            if num_retries == 0:
                LOG.debug("(retry) Giving up =================")
                return r

    def create_ssc_limit_tester(self, hnas_backend, connections=5):
        tester = clients.SSCLimitTester(hnas_backend, connections=connections)
        self.addCleanup(tester.close_connections)

        return tester

    def get_gigabytes_quota(self):
        return self.quotas_client.show_quota_set(
            self.tenant_id)['quota_set']['gigabytes']
