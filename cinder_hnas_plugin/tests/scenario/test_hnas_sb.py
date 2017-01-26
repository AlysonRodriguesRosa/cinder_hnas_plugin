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
from tempest.lib import decorators
from tempest.lib import exceptions
from tempest import test

from cinder_hnas_plugin.tests.utils.hnas import clients
from cinder_hnas_plugin.tests.scenario import base_hnas_test as base_hnas

import testtools

CONF = config.CONF
LOG = logging.getLogger(__name__)


@testtools.skipUnless(CONF.hnas.enabled_backends,
                      ("Missing HNAS backend configuration in "
                       "tempest config file."))
class TestHNASSB(base_hnas.BaseHNASTest):
    @test.idempotent_id('2294cde0-a8c2-48d0-bd67-d660f04f7134')
    @test.services('compute', 'network', 'volume')
    def test_hnas_sb01(self):
        """Write to an attached volume

        1. Create an instance i1:
        Select Boot Source: Image
        Create New Volume: No
        2. Create a volume v1 with 10 GB
        3. Attach v1 to i1
        4. Confirm that a disk is added on nova node and the disk in the VM is
           writable
        5. Detach v1 from i1
        6. Delete v1
        7. Delete i1
        """
        LOG.debug("1 -> Creating an instance i1. Boot from image not "
                  "creating a new volume.")
        instance, ssh_client = self.create_instance_and_client()

        for backend in self.hnas_backends:
            volume_size = 10
            LOG.debug("2 -> Creating a volume V1 with %s GB.", volume_size)
            vol, vol_ref = self.create_volume(backend, size=volume_size)

            LOG.debug("Making sure there's a corresponding file in HNAS.")
            self.assertTrue(self.retry(vol_ref.exists),
                            "Volume does not exist as a file within HNAS.")

            LOG.debug("3 -> Attaching V1 to i1.")
            self.nova_volume_attach(instance, vol)

            LOG.debug("4 -> Confirming that V1 is writable in i1.")
            self.verify_volume_writable(ssh_client, vol_ref)

            LOG.debug("5 -> Detaching V1 from i1.")
            self.nova_volume_detach(instance, vol)

            LOG.info("Volume %s detached from vm", vol['id'])
            LOG.debug("6 -> Deleting V1.")
            self.delete_volume(vol_ref)

            LOG.debug("7 -> Deleting i1.")
            # instance cleanup

    @test.idempotent_id('0dbb46eb-6ad8-433c-8587-2108e9c565e9')
    @test.services('compute', 'network', 'volume')
    def test_hnas_sb02(self):
        """Detach a volume and attach to a new VM

        1. Create an instance i1
        Select Boot Source: Image
        Create New Volume: Yes (V1)
        Delete Volume on Instance Delete: No
        2. Check if it creates a new volume V1 in the backend and in cinder
        3. Delete the instance i1
        4. Check that v1 was not deleted, both in cinder and in backend
        5. Create a new instance i2:
        Select Boot Source: Image
        Create New Volume: No
        6. Attach V1 to i2
        7. Confirm that a disk is added on nova node and the disk in the VM is
           writable
        8. Detach v1 from i2
        9. Delete v1
        10. Delete i2
        """
        LOG.debug("1 -> Creating an instance i1. Boot from an image creating "
                  "a new volume V1, that is not deleted on terminate.")
        inst, ssh_client = self.create_instance_and_client(
            create_backing_vol=True,
            delete_vol_on_termination=False)
        vol = inst['os-extended-volumes:volumes_attached'][0]
        vol_id = vol['id']

        LOG.debug("2.1 -> Checking if volume V1 exists in Cinder...")
        self.assertTrue(self.vol_exists_in_cinder(vol_id))

        LOG.debug("2.2 -> Checking if volume exists in the backends...")
        vol_ref = self.vol_exists_in_some_backend(vol_id)
        self.assertTrue(vol_ref)

        LOG.debug("3 -> Deleting instance %s.", inst['id'])
        self.delete_instance(inst['id'])

        LOG.debug("4.1 -> Checking if volume V1 still exists in Cinder...")
        self.assertTrue(self.vol_exists_in_cinder(vol_id))

        LOG.debug("4.2 -> Checking if volume still exists in the backends...")
        self.assertTrue(self.retry(vol_ref.exists))

        LOG.debug("5 -> Creating an instance i2. Boot from an image NOT "
                  "creating a new volume.")
        inst, ssh_client = self.create_instance_and_client()

        LOG.debug("6 -> Attaching V1 to i2.")
        self.nova_volume_attach(inst, vol)

        LOG.debug("7 -> Confirming that V1 is writable in i2.")
        self.verify_volume_writable(ssh_client, vol_ref)

        LOG.debug("8 -> Detaching V1 from i2.")
        self.nova_volume_detach(inst, vol)

        LOG.debug("9 -> Deleting V1.")
        self.delete_volume(vol_ref)
        LOG.debug("Checking that volume V1 does not exist in Cinder...")
        self.assertFalse(self.vol_exists_in_cinder(vol_id))

        LOG.debug("10 -> Deleting i2.")
        # instance cleanup

    @test.idempotent_id('c9baa47c-9d19-4246-a82c-6492e3ae5328')
    @test.services('compute', 'network', 'volume')
    def test_hnas_sb03(self):
        """Create a volume from image, create a volume from snapshot

        1. Create a volume V1 from an image
        2. Create a snapshot S1 from V1
        3. Create a new instance i1:
        Select Boot Source: Volume Snapshot
        Select S1
        4. Check that it creates a new volume V2
        5. Delete i1
        6. Delete S1
        7. Delete V1 and V2
        """

        for backend in self.hnas_backends:
            test_inst, test_ssh = self.create_instance_and_client()

            LOG.debug("1 -> Creating a volume V1 from an image.")
            v1, v1_ref = self.create_volume(
                backend, imageRef=CONF.compute.image_ref)

            LOG.debug("1.1 -> Checking if volume V1 still exists in Cinder")
            self.assertTrue(self.vol_exists_in_cinder(v1['id']))
            LOG.debug("1.2 -> Checking if volume still exists in the backends")
            self.assertTrue(self.retry(v1_ref.exists))

            LOG.debug("1.3 -> Write something into V1...")
            self.nova_volume_attach(test_inst, v1)
            test_data = self.verify_volume_writable(test_ssh, v1_ref)

            LOG.debug("2 -> Creating a snapshot S1 from V1.")
            s1, s1_ref = self.create_snapshot_from_volume(v1_ref)
            LOG.debug("2.1 -> Check that the snapshot has the same data...")
            self.assertEqual(s1_ref.get_first_bytes(len(test_data)), test_data,
                             ("Data read from snapshot is not the same as data"
                              "written to volume"))

            LOG.debug("3 -> Creating a new instance i1. "
                      "Boot Source: Volume Snapshot S1.")
            i1, i1_ssh = self.create_instance_and_client(
                create_backing_vol=True,
                source_type='snapshot', source_uuid=s1_ref.uuid,
                delete_vol_on_termination=False)
            LOG.debug("4 -> Checking that it creates a new volume V2.")
            v2_id = i1['os-extended-volumes:volumes_attached'][0]['id']
            v2_ref = backend.get_volume_reference(v2_id)

            LOG.debug("4.1 -> Checking if V2 exists in Cinder...")
            self.assertTrue(self.vol_exists_in_cinder(v2_id))
            LOG.debug("4.2 -> Checking if V2 exists in the backend...")
            self.assertTrue(self.retry(v2_ref.exists))

            LOG.debug("4.3 -> Check if the data in the VM disk is the same "
                      "as the data from the snapshot it came from")
            self.assertEqual(v2_ref.get_first_bytes(len(test_data)), test_data,
                             ("VM created from snapshot does not have the same"
                              "data from the snapshot in its volume"))

            LOG.debug("5 -> Deleting i1.")
            self.delete_instance(i1['id'])

            LOG.debug("6 -> Deleting S1...")
            self.delete_snapshot(s1_ref)

            LOG.debug("7 -> Deleting V1 and V2...")
            self.nova_volume_detach(test_inst, v1)
            self.delete_volume(v1_ref)
            self.delete_volume(v2_ref)

    @test.idempotent_id('2759647a-fd7b-4ac4-98bd-cb2f0a1f10a0')
    @test.services('compute', 'network', 'volume')
    def test_hnas_sb04(self):
        """Create a snapshot before and after volume extend

        1. Create a volume V1 with 20 GB
        2. Create a snapshot from V1
        3. Extend the volume to 25 GB
        4. Create another snapshot from V1
        5. Delete all snapshots
        6. Delete V1
        """

        for backend in self.hnas_backends:
            volume_size = 20
            LOG.debug("1 -> Creating a volume V1 with %s GB.", volume_size)
            v1, v1_ref = self.create_volume(backend, size=volume_size)

            LOG.debug("Checking if volume V1 still exists in Cinder...")
            self.assertTrue(self.vol_exists_in_cinder(v1['id']))
            LOG.debug("Checking if volume still exists in the backends...")
            self.assertTrue(self.retry(v1_ref.exists))

            LOG.debug("2 -> Creating a snapshot from V1.")
            s1, s1_ref = self.create_snapshot_from_volume(v1_ref)

            ext_size = volume_size + 5
            LOG.debug("3 -> Extending the volume to %s GB.", ext_size)
            v1, v1_ref = self.extend_volume(v1_ref, ext_size)
            self.assertEqual(ext_size, v1['size'],
                             "Volume size does not match extended size")
            self.assertEqual(ext_size, v1_ref.get_size(),
                             "Volume size on backend does not match extended "
                             "size")

            LOG.debug("4 -> Creating another snapshot from V1.")
            s2, s2_ref = self.create_snapshot_from_volume(v1_ref)

            LOG.debug("5 -> Deleting all snapshots.")
            self.delete_snapshot(s2_ref)
            self.delete_snapshot(s1_ref)

            LOG.debug("6 -> Deleting V1.")
            self.delete_volume(v1_ref)

    @test.idempotent_id('27f71c2a-c216-451d-acbe-95d516f7342f')
    @test.services('compute', 'network', 'volume')
    def test_hnas_sb05(self):
        """Create a volume from image, upload a volume to an image

        1. Create a volume V1 from an image
        2. Upload to an image im1
        3. Create an instance i1 from image im1 with the correct flavor to
           accommodate the volume
        4. Delete i1
        5. Delete im1
        6. Delete V1
        """

        for backend in self.hnas_backends:
            LOG.debug("1 -> Creating a volume V1 from an image.")
            v1, v1_ref = self.create_volume(
                backend, imageRef=CONF.compute.image_ref)

            LOG.debug("Checking if volume V1 still exists in Cinder...")
            self.assertTrue(self.vol_exists_in_cinder(v1['id']))
            LOG.debug("Checking if volume still exists in the backends...")
            self.assertTrue(self.retry(v1_ref.exists))

            LOG.debug("2 -> Uploading to an image im1.")
            # NOTE(tpsilva): Image will be deleted automatically, so step #5
            # is not necessary.
            img_ref = self.upload_volume_to_image(v1)

            LOG.debug("3 -> Creating instance i1 from image im1.")
            i1, ssh_client = self.create_instance_and_client(
                create_backing_vol=True,
                source_uuid=img_ref,
                delete_vol_on_termination=True)

            LOG.debug("4 -> Deleting instance i1.")
            self.delete_instance(i1['id'])

            LOG.debug("5 -> Deleting image im1.")
            self.delete_image(img_ref)

            LOG.debug("6 -> Deleting volume V1.")
            self.delete_volume(v1_ref)

    @test.idempotent_id('af47aa31-d10e-4215-a386-04f1e5657544')
    @testtools.skipUnless(CONF.hnas.enabled_backends,
                          ("Missing HNAS backend configuration in "
                           "tempest config file."))
    @test.services('compute', 'network', 'volume')
    def test_hnas_sb06(self):
        """Creating a VM from an instance snapshot

        1. Create an instance i1:
        Select Boot Source: Image
        Create New Volume: Yes
        Delete Volume on Instance Delete: Yes
        2. Check if it creates a new volume V1 in the backend and in cinder
        3. Create an instance snapshot is1 from i1
        4. Create a new instance i2:
        Select Boot Source: Instance Snapshot
        Select is1
        Delete Volume on Instance Delete: Yes
        5. Check if a new volume V2 was created and attached to i2
        6. Check if a volume snapshot for is1 was created
        7. Delete the instance snapshot is1
        8. Delete the volume snapshot for is1
        9. Delete the instances i1 and i2
        10. Check if the volumes were deleted

        * Bug: as of 09.21.2016, horizon lists instance snapshot under
        Select Boot Source -> Image instead of
        Select Boot Source -> Instance Snapshot. It's reported at
        https://bugs.launchpad.net/horizon/+bug/1626202.
        """

        for backend in self.hnas_backends:
            LOG.debug("1 -> Creating an instance i1. Boot from image "
                      "creating a new volume that will be delete on "
                      "terminate.")
            i1, ssh_client = self.create_instance_and_client(
                create_backing_vol=True,
                delete_vol_on_termination=True)

            LOG.debug("2 -> Checking if it creates a new volume V1 in the "
                      "backend and in cinder.")
            v1_id = i1['os-extended-volumes:volumes_attached'][0]['id']
            v1_ref = backend.get_volume_reference(v1_id)

            self.assertTrue(self.vol_exists_in_cinder(v1_id))
            self.assertTrue(self.retry(v1_ref.exists))

            LOG.debug("3 -> Creating an instance snapshot is1 from i1. "
                      "(I'll actually take a volume snapshot through the "
                      "cinder API, because the nova one is deprecated).")
            is1, is1_ref = self.create_snapshot_from_volume(v1_ref)

            LOG.debug("4 -> Creating a new instance i2. Boot from instance "
                      "snapshot is1 deleting on terminate.")
            i2, ssh_client_2 = self.create_instance_and_client(
                source_uuid=is1['id'],
                source_type='snapshot',
                create_backing_vol=True,
                delete_vol_on_termination=True)

            LOG.debug("5 -> Checking if a new volume V2 was created and "
                      "attached to i2.")
            v2_id = i2['os-extended-volumes:volumes_attached'][0]['id']
            v2_ref = backend.get_volume_reference(v2_id)

            self.assertTrue(self.vol_exists_in_cinder(v2_id))
            self.assertTrue(self.retry(v2_ref.exists))

            LOG.debug("6 -> Checking if a volume snapshot for is1 was "
                      "created.")
            snaps_resp = self.snapshots_client.list_snapshots(detail=True)
            snaps = snaps_resp['snapshots']
            self.assertTrue(is1['id'] in [s['id'] for s in snaps])
            self.assertTrue(self.retry(is1_ref.exists))

            LOG.debug("7 -> Deleting the instance snapshot is1 (this is "
                      "seemingly deprecated, since the nova snapshots and "
                      "the cinder snapshots should be one and the same).")

            LOG.debug("8 -> Deleting the volume snapshot snapshot for is1.")
            self.delete_snapshot(is1_ref)

            LOG.debug("9 -> Deleting the instances i1 and i2.")
            self.delete_instance(i2['id'])
            self.delete_instance(i1['id'])

            LOG.debug("10 -> Checking if the volumes were deleted.")
            self.assertFalse(self.retry(v1_ref.exists, expect_success=False),
                             ("Deleted volume still resides in HNAS "
                              "at %s" % v1_ref.unix_path))
            self.assertFalse(self.retry(v2_ref.exists, expect_success=False),
                             ("Deleted volume still resides in HNAS "
                              "at %s" % v2_ref.unix_path))

    @test.idempotent_id('46beca74-8c35-4cb2-b2df-c85d9011deff')
    @test.services('compute', 'network', 'volume')
    def test_hnas_sb07(self):
        """Extend a cloned volume

        1. Create a volume V1 with 15 GB
        2. Create a cloned volume C1 with 20 GB
        3. Check if C1 was successfully created
        4. Extend C1 to 25 GB
        5. Delete the cloned volume
        6. Delete the original volume
        """

        for backend in self.hnas_backends:
            volume_size = 15
            LOG.debug("1.1 -> Creating a volume V1 with %s GB.", volume_size)
            v1, v1_ref = self.create_volume(backend,
                                            size=volume_size)

            LOG.debug("Checking if volume V1 still exists in Cinder...")
            self.assertTrue(self.vol_exists_in_cinder(v1['id']))
            LOG.debug("Checking if volume still exists in the backends...")
            self.assertTrue(self.retry(v1_ref.exists))

            LOG.debug("1.2 -> Writing something to V1.")
            instance, ssh_client = self.create_instance_and_client()
            self.nova_volume_attach(instance, v1)
            v1_test_str = self.verify_volume_writable(ssh_client, v1_ref)
            self.nova_volume_detach(instance, v1)

            cloned_size = volume_size + 5
            LOG.debug("2 -> Creating a cloned volume C1 with %s GB.",
                      cloned_size)
            c1, c1_ref = self.create_volume(backend,
                                            source_volid=v1['id'],
                                            size=cloned_size)

            LOG.debug("3.1 -> Checking if C1 was successfully created.")
            self.assertTrue(self.vol_exists_in_cinder(c1['id']))
            self.assertTrue(self.retry(c1_ref.exists))

            LOG.debug("3.2 -> Verifying that C1 contains the same data that "
                      "was written to V1.")
            c1_test_str = c1_ref.get_first_bytes(len(v1_test_str))
            self.assertEqual(v1_test_str, c1_test_str,
                             ("Cloned volume does not have the same contents "
                              "as the original volume."))

            extended_size = cloned_size + 5
            LOG.debug("4 -> Extending C1 to %s GB.", extended_size)
            c1, c1_ref = self.extend_volume(c1_ref, extended_size)
            self.assertEqual(extended_size, c1['size'],
                             "Volume size does not match extended size")
            self.assertEqual(extended_size, c1_ref.get_size(),
                             "Volume size on backend does not match extended "
                             "size")

            LOG.debug("5 -> Deleting the cloned volume.")
            self.delete_volume(c1_ref)

            LOG.debug("6 -> Deleting the original volume.")
            self.delete_volume(v1_ref)

    @test.idempotent_id('bdb91834-c374-4946-a430-55506ede0c21')
    @test.services('compute', 'network', 'volume')
    def test_hnas_sb08(self):
        """Extend a volume created from a snapshot

        1. Create a volume V1 with 10 GB
        2. Create a snapshot S1 from volume V1
        3. Check if the S1 was successfully created
        4. Create a volume V2 from snapshot S1
        5. Extend V2 to 15 GB
        6. Check if the V2 was successfully created and extended
        7. Delete V2
        8. Delete S1
        9. Delete V1
        """

        for backend in self.hnas_backends:
            volume_size = 10
            LOG.debug("1 -> Creating a volume V1 with %s GB", volume_size)
            v1, v1_ref = self.create_volume(backend, size=volume_size)

            LOG.debug("2 -> Creating a snapshot S1 from V1.")
            s1, s1_ref = self.create_snapshot_from_volume(v1_ref)

            LOG.debug("3 -> Checking if S1 was created in the backend and in "
                      "cinder.")
            self.assertTrue(self.snap_exists_in_cinder(s1['id']))
            self.assertTrue(self.retry(s1_ref.exists))

            LOG.debug("4 -> Creating a volume V2 from snapshot S1.")
            # NOTE: the volume size must be declared and cannot be smaller
            # than the snapshot size
            v2, v2_ref = self.create_volume(backend,
                                            size=volume_size,
                                            snapshot_id=s1['id'])

            ext_size = volume_size + 5
            LOG.debug("5 -> Extending V2 to %s GB.", ext_size)
            v2, v2_ref = self.extend_volume(v2_ref, ext_size)

            LOG.debug("6 -> Checking if V2 was created and extended.")
            self.assertTrue(self.vol_exists_in_cinder(v2['id']))
            self.assertTrue(self.retry(v2_ref.exists))
            self.assertEqual(ext_size, v2['size'],
                             "Volume size does not match extended size")
            self.assertEqual(ext_size, v2_ref.get_size(),
                             "Volume size on backend does not match extended "
                             "size")

            LOG.debug("7 -> Deleting V2.")
            self.delete_volume(v2_ref)

            LOG.debug("8 -> Deleting S1.")
            self.delete_snapshot(s1_ref)

            LOG.debug("9 -> Deleting V1.")
            self.delete_volume(v1_ref)

    @test.idempotent_id('045e9cb1-f091-421d-a69d-02783dff03c9')
    @test.services('compute', 'network', 'volume')
    def test_hnas_sb09(self):
        """Detach and attach a volume after creating an online snapshot

        1. Create volume V1
        2. Create an instance i1
        3. Attach V1 to i1
        4. Create online snapshot S1 from V1
        5. Delete S1
        6. Detach V1 from i1
        7. Attach volume V1 to an i1 again
        8. Check if V1 was successfully attached to i1
        9. Detach V1 from i1
        10. Delete V1
        11. Delete i1
        """

        for backend in self.hnas_backends:
            LOG.debug("1 -> Creating a volume V1.")
            v1, v1_ref = self.create_volume(backend)

            LOG.debug("2 -> Creating an instance i1.")
            i1, ssh_client = self.create_instance_and_client()

            LOG.debug("3 -> Attaching volume V1 to instance i1.")
            self.nova_volume_attach(i1, v1)

            LOG.debug("4 -> Creating online snapshot S1 from V1.")
            s1, s1_ref = self.create_snapshot_from_volume(v1_ref)

            LOG.debug("5 -> Deleting S1.")
            self.delete_snapshot(s1_ref)

            LOG.debug("6.1 -> Detaching V1 from i1.")
            self.nova_volume_detach(i1, v1)

            LOG.debug("7 -> Attaching volume V1 to instance i1 again.")
            self.nova_volume_attach(i1, v1)

            LOG.debug("8 -> Checking if V1 was attached to i1.")
            # NOTE(yumiriam): attach and detach functions already check if
            #   their own procedure succeed.
            #   to check if the volume is attached to the correct instance
            #   (i2) we could implement a function in base_hnas_tests using
            #   get_attachment_from_volume

            LOG.debug("9 -> Detaching V1 from i1 again.")
            self.nova_volume_detach(i1, v1)

            LOG.debug("10 -> Deleting V1.")
            self.delete_volume(v1_ref)

            LOG.debug("11 -> Deleting i1.")
            self.delete_instance(i1['id'])

    @decorators.skip_because(bug="1652811")
    @test.idempotent_id('9f8e1cf9-b7ba-41e2-bff9-55f1d1c0cfc2')
    @test.services('compute', 'network', 'volume')
    def test_hnas_sb10(self):
        """Remanage a volume and extend

        1. Create a volume v1 with 1GB
        2. Unmanage v1
        3. Check that v1 is not listed by 'cinder list', but still exist in
        HNAS renamed to 'unmanage-<vol_name>'*
        4. Manage v1
        5. Check that v1 is listed by 'cinder list'
        6. Extend v1 to 5GB
        7. Verify that the size of v1 is correctly updated
        8. Delete v1
        9. Check that no files related to v1 exist on HNAS anymore

        * in an iSCSI backend, the volume alias will be renamed to
        unmanage-<vol_name>. However, its path remains the same.
        """

        for backend in self.hnas_backends:
            LOG.debug("1 -> Creating a volume V1.")
            v1, v1_ref = self.create_volume(backend)
            v1_id = v1['id']

            LOG.debug("2 -> Unmanaging V1.")
            self.unmanage_volume(v1_ref, v1)

            LOG.debug("3 -> Checking if V1 does not exists in cinder.")
            self.assertFalse(self.vol_exists_in_cinder(v1_id))

            LOG.debug("4 -> Managing V1.")
            v1, v1_ref = self.manage_volume(backend, v1_ref)
            new_v1_id = v1['id']

            LOG.debug("5 -> Checking that V1 exists in cinder.")
            self.assertTrue(self.vol_exists_in_cinder(new_v1_id))

            ext_size = 5
            LOG.debug("6 -> Extending V1 to %sGB.", ext_size)
            v1, v1_ref = self.extend_volume(v1_ref, ext_size)

            LOG.debug("7 -> Checking if V1 was extended...")
            self.assertEqual(ext_size, v1['size'],
                             "Volume size does not match extended size")
            self.assertEqual(ext_size, v1_ref.get_size(),
                             "Volume size on backend does not match extended "
                             "size")

            LOG.debug("8 -> Deleting V1.")
            self.delete_volume(v1_ref)

            LOG.debug("9 -> Checking if V1 does not exist in the backend.")
            self.assertFalse(self.retry(v1_ref.exists, expect_success=False),
                             ("Deleted volume still resides in HNAS at %s"
                              % v1_ref.unix_path))

    @test.idempotent_id('3746d40b-36ec-4d0f-b798-9e53c2d7af70')
    @test.services('compute', 'network', 'volume')
    def test_hnas_sb11(self):
        """HNAS creates a volume after restablishing connection

        1. Create a volume v1
        2. Open 5 SSH connections on HNAS
        3. Clone a volume v2 from v1 and wait to get an error on cinder
        3.1. Start to wait for an error on cinder-volume.log (HNASConnError:
        Failed to establish SSC connection.)
        3.2. Clone a volume v2 from v1
        3.3. After the first time that the error appears, close SSH
        connections on HNAS
        4. The volume should be created successfully
        5. Delete v1 and v2
        """

        for backend in self.hnas_backends:
            LOG.debug("1 -> Creating a volume V1.")
            v1, v1_ref = self.create_volume(backend)
            v1_id = v1['id']

            LOG.debug("2 -> Opening SSH connections on backend...")
            ssc_tester = self.create_ssc_limit_tester(backend)
            ssc_tester.open_connections()

            LOG.debug("3 -> Cloning a volume V2 from V1 and waiting for an "
                      "error...")
            LOG.debug("3.1 -> Start to wait for an error. (Connections will "
                      "be closed when the error appears on the log.)")
            # NOTE(yumiriam): steps 3.1 and 3.3 were joined to run in a
            # separate thread in order to close the connections as soon as the
            # error occurs and during the attempts of cloning the volume.
            ssc_tester.close_connections_on_error()

            LOG.debug("3.2 -> Cloning a volume V2 from V1.")
            v2, v2_ref = self.create_volume(backend, source_volid=v1_id)
            self.assertTrue(ssc_tester.error_has_occurred,
                            "SSC error was expected.")

            LOG.debug("4 -> Checking if volume V2 was successfully "
                      "created.")
            self.assertTrue(self.vol_exists_in_cinder(v2['id']))
            self.assertTrue(self.retry(v2_ref.exists))

            LOG.debug("5 -> Deleting volumes V1 and V2.")
            self.delete_volume(v1_ref)
            self.assertFalse(self.retry(v1_ref.exists, expect_success=False),
                             "Deleted volume still resides in HNAS")

            self.delete_volume(v2_ref)
            self.assertFalse(self.retry(v2_ref.exists, expect_success=False),
                             "Deleted volume still resides in HNAS")

    @test.idempotent_id('ca75fc35-002f-47db-876a-1e0b04472f15')
    @test.services('compute', 'network', 'volume')
    def test_hnas_sb12(self):
        """Unmanage and manage snapshot

        1. Create a volume V1
        2. Create a snapshot S1 from V1
        3. Unmanage S1
        4. Check that S1 is not listed by 'cinder snapshot-list', but still
        exist in HNAS renamed to 'unmanage-<snap_name>'
        5. Manage S1
        6. Check that S1 is managed by cinder again
        7. Delete S1
        8. Check that S1 does not exist on cinder and HNAS
        9. Delete V1
        """

        for backend in self.hnas_backends:
            LOG.debug("1 -> Creating a volume V1.")
            v1, v1_ref = self.create_volume(backend)

            LOG.debug("2 -> Creating a snapshot S1 from V1.")
            s1, s1_ref = self.create_snapshot_from_volume(v1_ref)

            LOG.debug("3 -> Unmanaging snapshot S1.")
            self.unmanage_snapshot(s1_ref)

            LOG.debug("4 -> Checking that S1 does not exist in cinder but "
                      "remains in HNAS.")
            self.assertFalse(self.snap_exists_in_cinder(s1['id']))
            self.assertTrue(self.retry(s1_ref.exists))

            LOG.debug("5 -> Managing snapshot S1.")
            # svc_idx will be 0 (default)
            s1, s1_ref = self.manage_snapshot(v1['id'], s1_ref)

            LOG.debug("6 -> Checking that snapshot S1 exists in cinder.")
            self.assertTrue(self.snap_exists_in_cinder(s1['id']))
            self.assertTrue(self.retry(s1_ref.exists))

            LOG.debug("7 -> Deleting snapshot S1.")
            self.delete_snapshot(s1_ref)

            LOG.debug("8 -> Checking that snapshot S1 does not exist in "
                      "cinder and in HNAS.")
            self.assertFalse(self.snap_exists_in_cinder(s1['id']))
            self.assertFalse(self.retry(s1_ref.exists, expect_success=False),
                             "Deleted snapshot still resides in HNAS.")

            LOG.debug("9 -> Deleting volume V1.")
            self.delete_volume(v1_ref)

    @test.idempotent_id('8d2f57d7-b9e4-47aa-9dc6-b390658b5bf8')
    @test.services('compute', 'network', 'volume')
    def test_hnas_sb13(self):
        """"Create a vol from a snap; Take another snap, unmanage and manage

        1. Create a volume V1
        2. Create a volume V2 from V1
        3. Create a snapshot S1 from V2
        4. Create a volume V3 from S1
        5. Create a volume snapshot S2 from V3
        6. Unmanage S1 and S2
        7. Check that S1 and S2 are not managed by Cinder, but still exist
           on HNAS
        8. Manage S1 and S2
        9. Check that S1 and S2 are managed by Cinder again
        10. Delete all the snapshots
        11. Delete all the volumes
        """

        for backend in self.hnas_backends:
            LOG.debug("1 -> Creating a volume V1.")
            v1, v1_ref = self.create_volume(backend)

            LOG.debug("2 -> Creating a volume V2 from V1.")
            v2, v2_ref = self.create_volume(backend,
                                            source_volid=v1['id'])

            LOG.debug("3 -> Creating a snapshot S1 from V2.")
            s1, s1_ref = self.create_snapshot_from_volume(v2_ref)

            LOG.debug("4 -> Creating a volume V3 from S1.")
            v3, v3_ref = self.create_volume(backend,
                                            snapshot_id=s1['id'])

            LOG.debug("5 -> Creating a volume snapshot S2 from V3.")
            s2, s2_ref = self.create_snapshot_from_volume(v3_ref)

            LOG.debug("6 -> Unmanaging S1 and S2.")
            self.unmanage_snapshot(s1_ref)
            self.unmanage_snapshot(s2_ref)

            LOG.debug("7 -> Checking that S1 and S2 are not managed by "
                      "Cinder, but still exist on HNAS.")
            self.assertFalse(self.snap_exists_in_cinder(s1['id']))
            self.assertFalse(self.snap_exists_in_cinder(s2['id']))
            self.assertTrue(self.retry(s1_ref.exists))
            self.assertTrue(self.retry(s2_ref.exists))

            LOG.debug("8 -> Managing S1 and S2.")
            s1, s1_ref = self.manage_snapshot(v2['id'], s1_ref)
            s2, s2_ref = self.manage_snapshot(v3['id'], s2_ref)

            LOG.debug("9 -> Checking that S1 and S2 are managed by Cinder "
                      "again.")
            self.assertTrue(self.snap_exists_in_cinder(s1['id']))
            self.assertTrue(self.snap_exists_in_cinder(s2['id']))

            LOG.debug("10 -> Deleting all the snapshots.")
            self.delete_snapshot(s1_ref)
            self.delete_snapshot(s2_ref)

            self.assertFalse(self.snap_exists_in_cinder(s1['id']))
            self.assertFalse(self.retry(s1_ref.exists, expect_success=False),
                             "Deleted snapshot s1 still resides in HNAS.")

            self.assertFalse(self.snap_exists_in_cinder(s2['id']))
            self.assertFalse(self.retry(s2_ref.exists, expect_success=False),
                             "Deleted snapshot s2 still resides in HNAS.")

            LOG.debug("11 -> Deleting all the volumes.")
            self.delete_volume(v1_ref)
            self.delete_volume(v2_ref)
            self.delete_volume(v3_ref)

    @test.idempotent_id('c722c3c1-0801-4d9c-9e8a-4c3b78aa24b2')
    @test.services('compute', 'network', 'volume')
    def test_hnas_sb14(self):
        """Almost fill up a volume; Take a snapshot, unmanage and manage

        1. Create a volume V1 with 1GB
        2. Create an instance VM1 and attach V1
        3. SSH to VM1, write 900MB random bytes[1] and "1st Snapshot"[2] to v1
        4. Create an online snapshot SS1 from V1
        5. Unmanage SS1
        6. SSH to VM1 again, rewrite 900MB and "2nd Snapshot" in V1
        7. Create an online snapshot SS2 from V1
        8. Unmanage SS2
        9. Detach V1
        10. Manage SS1 as SS1_managed
        11. Manage SS2 as SS2_managed
        12. Create a volume S3 from SS1_managed
        13. Create a volume S4 from SS2_managed
        14. Attach S3 to VM1
        15. Attach S4 to VM1
        16. SSH into VM1
        17. Read the first bytes of S3 and check if it
            contains "1st Snapshot"[3]
        18. Read the first bytes of S4 and check if it's written "2nd Snapshot"
        19. Delete SS1_managed and SS2_managed
        20. Delete VM1
        21. Delete V1, S3 and S4
        22. Check that there are no files remaining in the backend

        [1] dd if=/dev/urandom of=/dev/vdb bs=1M count=900
        [2] echo "1st Snapshot" > file.txt;
            sudo dd if=file.txt of=/dev/<disk> bs=1 count=12
        [3] sudo head -c 12 /dev/<disk>
        """
        snap_str_marker_1 = "1st Snapshot"
        snap_str_marker_2 = "2nd Snapshot"

        for backend in self.hnas_backends:
            LOG.debug("1 -> Creating a volume V1 with 1GB.")
            v1, v1_ref = self.create_volume(backend, size=1)

            LOG.debug("2 -> Creating an instance VM1 and attach V1.")
            vm1, vm1_ssh = self.create_instance_and_client()
            self.nova_volume_attach(vm1, v1)

            LOG.debug("3 -> SSH to VM1, writing 900MB random bytes using 'dd' "
                      "and '1st Snapshot' to v1.")
            blk_dev_tester = clients.InstanceBlockDevTester(
                vm1_ssh,
                CONF.compute.volume_device_name)
            blk_dev_tester.fill_with_random_data(900, snap_str_marker_1)

            LOG.debug("4 -> Creating an online snapshot SS1 from V1.")
            ss1, ss1_ref = self.create_snapshot_from_volume(v1_ref)

            LOG.debug("5 -> Unmanaging SS1.")
            self.unmanage_snapshot(ss1_ref)

            LOG.debug("6 -> SSH to VM1 again, rewriting 900MB and "
                      "'2nd Snapshot' in V1.")
            blk_dev_tester.fill_with_random_data(900, snap_str_marker_2)

            LOG.debug("7 -> Creating an online snapshot SS2 from V1.")
            ss2, ss2_ref = self.create_snapshot_from_volume(v1_ref)

            LOG.debug("8 -> Unmanaging SS2.")
            self.unmanage_snapshot(ss2_ref)

            LOG.debug("9 -> Detaching V1.")
            self.nova_volume_detach(vm1, v1)

            LOG.debug("10 -> Managing SS1 as SS1_managed.")
            ss1_mng, ss1_mng_ref = self.manage_snapshot(v1['id'],
                                                        ss1_ref)

            LOG.debug("11 -> Managing SS2 as SS2_managed.")
            ss2_mng, ss2_mng_ref = self.manage_snapshot(v1['id'],
                                                        ss2_ref)

            LOG.debug("12 -> Creating a volume S3 from SS1_managed.")
            s3, s3_ref = self.create_volume(backend,
                                            snapshot_id=ss1_mng['id'])

            LOG.debug("13 -> Creating a volume S4 from SS2_managed.")
            s4, s4_ref = self.create_volume(backend,
                                            snapshot_id=ss2_mng['id'])

            LOG.debug("14 -> Attaching S3 to VM1.")
            self.nova_volume_attach(vm1, s3)
            str_marker_from_snap_1 = blk_dev_tester.get_bytes_at_offset(
                900, len(snap_str_marker_1))
            self.nova_volume_detach(vm1, s3)

            LOG.debug("15 -> Attaching S4 to VM1.")
            self.nova_volume_attach(vm1, s4)
            str_marker_from_snap_2 = blk_dev_tester.get_bytes_at_offset(
                900, len(snap_str_marker_2))
            self.nova_volume_detach(vm1, s4)

            LOG.debug("16 -> SSH into VM1.")
            LOG.debug("17 -> Reading the first bytes of S3 and checking if it "
                      "contains '1st Snapshot'.")
            self.assertEqual(str_marker_from_snap_1, snap_str_marker_1)

            LOG.debug("18 -> Reading the first bytes of S4 and checking if it "
                      "contains '2nd Snapshot'.")
            self.assertEqual(str_marker_from_snap_2, snap_str_marker_2)

            LOG.debug("19 -> Deleting SS1_managed and SS2_managed.")
            self.delete_snapshot(ss1_mng_ref)
            self.delete_snapshot(ss2_mng_ref)

            LOG.debug("20 -> Deleting VM1.")
            self.delete_instance(vm1['id'])

            LOG.debug("21 -> Deleting V1, S3 and S4.")
            self.delete_volume(v1_ref)
            self.delete_volume(s3_ref)
            self.delete_volume(s4_ref)

            LOG.debug("22 -> Checking that there are no files remaining in "
                      "HNAS.")
            for v_ref in (v1_ref, s3_ref, s4_ref, ss1_ref, ss2_ref,
                          ss1_mng_ref, ss2_mng_ref):
                self.assertFalse(
                    self.retry(v_ref.exists, expect_success=False),
                    ("Deleted volume still resides in HNAS "
                     "at %s" % v_ref.unix_path))

    @test.idempotent_id('feb9a257-6134-45ce-a191-0d41b2c57d98')
    @test.services('compute', 'network', 'volume')
    def test_hnas_sb15(self):
        """Fill up a volume and take snapshots several times, unmanage and manage

        1. Create a volume V1 with 1GB
        2. Create an instance VM1 and attach V1
        3. SSH to VM1 and write a 1GB file in V1
        4. Create a snapshot SS1 from V1
        5. Unmanage SS1
        6. Recreate the 1GB file created in step 3
        7. Create a snapshot SS2 from V1
        8. Unmanage SS2
        9. Recreate the 1GB file create in step 6
        10. Create a snapshot SS3 from V1
        11. Unmanage SS3
        12. Detach V1
        13. Manage all unmanaged snapshots
        14. Verify that no errors occurs on backend
        15. Delete all snapshots, instance and volumes
        """

        for backend in self.hnas_backends:
            LOG.debug("1 -> Creating a volume V1 with 1GB")
            v1, v1_ref = self.create_volume(backend, size=1)

            LOG.debug("2 -> Creating an instance VM1 and attach V1")
            vm1, vm1_ssh = self.create_instance_and_client()
            self.nova_volume_attach(vm1, v1)

            LOG.debug("3 -> Writing a 1GB file in V1")
            blk_dev_tester = clients.InstanceBlockDevTester(
                vm1_ssh,
                CONF.compute.volume_device_name)
            blk_dev_tester.fill_with_random_data(1000)

            LOG.debug("4 -> Creating a snapshot SS1 from V1")
            ss1, ss1_ref = self.create_snapshot_from_volume(v1_ref)

            LOG.debug("5 -> Unmanaging SS1")
            self.unmanage_snapshot(ss1_ref)

            LOG.debug("6 -> Recreating the 1GB file created in step 3")
            blk_dev_tester.fill_with_random_data(1000)

            LOG.debug("7 -> Creating a snapshot SS2 from V1")
            ss2, ss2_ref = self.create_snapshot_from_volume(v1_ref)

            LOG.debug("8 -> Unmanaging SS2")
            self.unmanage_snapshot(ss2_ref)

            LOG.debug("9 -> Recreating the 1GB file create in step 6")
            blk_dev_tester.fill_with_random_data(1000)

            LOG.debug("10 -> Creating a snapshot SS3 from V1")
            ss3, ss3_ref = self.create_snapshot_from_volume(v1_ref)

            LOG.debug("11 -> Unmanaging SS3")
            self.unmanage_snapshot(ss3_ref)

            LOG.debug("12 -> Detaching V1")
            self.nova_volume_detach(vm1, v1)

            LOG.debug("13 -> Managing all unmanaged snapshots")
            ss1_mng, ss1_mng_ref = self.manage_snapshot(v1['id'],
                                                        ss1_ref)
            ss2_mng, ss2_mng_ref = self.manage_snapshot(v1['id'],
                                                        ss2_ref)
            ss3_mng, ss3_mng_ref = self.manage_snapshot(v1['id'],
                                                        ss3_ref)

            LOG.debug("14 -> Verifying that no errors occurs on backend")
            # Errors would show as exceptions, so if we're here, then there
            # were no errors
            LOG.debug("15 -> Deleting all snapshots, instance and volumes")
            for snap_ref in (ss1_mng_ref, ss2_mng_ref, ss3_mng_ref):
                self.delete_snapshot(snap_ref)
                self.assertFalse(
                    self.retry(snap_ref.exists, expect_success=False),
                    ("Deleted snapshot still resides in HNAS "
                     "at %s" % snap_ref.unix_path))
            self.delete_volume(v1_ref)
            self.assertFalse(
                self.retry(v1_ref.exists, expect_success=False),
                ("Deleted volume still resides in HNAS "
                 "at %s" % v1_ref.unix_path))

    @test.idempotent_id('9bcf8bc7-8cc4-48d2-a5b9-f14e9bc05993')
    @test.services('compute', 'network', 'volume')
    def test_hnas_sb16(self):
        """Unmanage a snapshot. Extend the original volume. Manage the snapshot

        1. Create a volume V1 with 10GB
        2. Create a snapshot SS1 from V1
        3. Unmanage SS1
        4. Extend V1 to 15GB
        5. Manage SS1 as SS1_managed
        6. Delete SS1_managed
        7. Delete V1
        """

        for backend in self.hnas_backends:
            LOG.debug("1 -> Creating a volume V1 with 10GB")
            v1, v1_ref = self.create_volume(backend, size=10)

            LOG.debug("2 -> Creating a snapshot SS1 from V1")
            ss1, ss1_ref = self.create_snapshot_from_volume(v1_ref)

            LOG.debug("3 -> Unmanaging SS1")
            self.unmanage_snapshot(ss1_ref)

            LOG.debug("4 -> Extending V1 to 15GB")
            ext_size = 15
            v1, v1_ref = self.extend_volume(v1_ref, ext_size)
            self.assertEqual(ext_size, v1['size'],
                             "Volume size does not match extended size")
            self.assertEqual(ext_size, v1_ref.get_size(),
                             "Volume size on backend does not match extended "
                             "size")

            LOG.debug("5 -> Managing SS1 as SS1_managed")
            ss1_mng, ss1_mng_ref = self.manage_snapshot(v1['id'],
                                                        ss1_ref)

            LOG.debug("6 -> Deleting SS1_managed")
            self.delete_snapshot(ss1_mng_ref)
            self.assertFalse(
                self.retry(ss1_mng_ref.exists, expect_success=False),
                ("Deleted snapshot still resides in HNAS "
                 "at %s" % ss1_mng_ref.unix_path))

            LOG.debug("7 -> Deleting V1")
            self.delete_volume(v1_ref)
            self.assertFalse(
                self.retry(v1_ref.exists, expect_success=False),
                ("Deleted volume still resides in HNAS "
                 "at %s" % v1_ref.unix_path))

    @test.idempotent_id('cb013efb-6962-4149-a576-5dad7dce95b8')
    @test.services('volume')
    def test_hnas_sb17(self):
        """Unmanage a snapshot. Delete the original volume.

        1. Create a volume V1 with 5GB
        2. Create a snapshot SS1 from V1
        3. Unmanage SS1
        4. Delete V1 - it should succeed
        5. Check that the snapshot file still exists in the backend
        6. Delete the snapshot file manually
        """

        for backend in self.hnas_backends:
            LOG.debug("1 -> Creating a volume V1 with 5GB")
            v1, v1_ref = self.create_volume(backend, size=5)

            LOG.debug("2 -> Creating a snapshot SS1 from V1")
            ss1, ss1_ref = self.create_snapshot_from_volume(v1_ref)

            LOG.debug("3 -> Unmanaging SS1")
            self.unmanage_snapshot(ss1_ref)

            LOG.debug("4 -> Deleting V1")
            self.delete_volume(v1_ref)
            self.assertFalse(
                self.retry(v1_ref.exists, expect_success=False),
                ("Deleted volume still resides in HNAS "
                 "at %s" % v1_ref.unix_path))

            LOG.debug("5 -> Checking that the snapshot still resides in HNAS")
            self.assertTrue(
                self.retry(ss1_ref.exists),
                ("Deleted volume still resides in HNAS "
                 "at %s" % ss1_ref.unix_path))

            LOG.debug("6 -> Deleting SS1")
            ss1_ref.rm_via_ssc()
            self.assertFalse(
                self.retry(ss1_ref.exists, expect_success=False),
                ("Deleted volume still resides in HNAS "
                 "at %s" % ss1_ref.unix_path))

    @test.idempotent_id('09a3e392-8efe-42d2-a277-a48826f5a262')
    @test.services('compute', 'volume')
    def test_hnas_sb18(self):
        """Extending the volume to a larger size than the quota should fail.

        1. Create a volume v1 with 1GB
        2. Check tenant gigabytes quota
        3. Try to extend v1 to <tenant gigabyte quota> + 1GB
        4. Command should fail
        5. Delete v1
        """

        for backend in self.hnas_backends:
            LOG.debug("1 -> Creating a volume V1 with 1GB.")
            v1, v1_ref = self.create_volume(backend)

            LOG.debug("2 -> Checking tenant gigabytes quota.")
            quota = self.get_gigabytes_quota()

            ext_size = quota + 1
            LOG.debug("3 -> Trying to extend V1 to %sGB...", ext_size)
            try:
                v1, v1_ref = self.extend_volume(v1_ref, ext_size)
            except exceptions.OverLimit:
                LOG.debug("Failed to extend V1.")

            LOG.debug("4 -> Verifying volume size.")
            self.assertIsNot(ext_size, v1['size'],
                             "Volume size matches quota size")
            self.assertIsNot(ext_size, v1_ref.get_size(),
                             "Volume size on backend matches quota size")

            LOG.debug("5 -> Deleting V1.")
            self.delete_volume(v1_ref)
            self.assertFalse(
                self.retry(v1_ref.exists, expect_success=False),
                ("Deleted volume still resides in HNAS "
                 "at %s" % v1_ref.unix_path))
