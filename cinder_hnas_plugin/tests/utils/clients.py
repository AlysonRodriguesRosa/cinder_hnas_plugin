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

import os
from oslo_config import cfg
import re
from tempest import config
from tempest.lib import exceptions as lib_exc
import threading
import time

import logging

from cinder_hnas_plugin.tests.utils.hnas import remote_client

LOG = logging.getLogger(__name__)

# copied from cinder/volume/drivers/hitachi/hnas_utils.py
# Keep this to date! Also,
# TODO(Tomaz): Make some test that checks if these options match
# the ones from the cinder driver
# TODO(alyson): recheck this notes
# Note #1: This opts do not handle with iscsi_ip param?
# Note #2: This opts should be in tempest/config.py with all other config opts
hnas_drivers_common_opts = [
    # options from the generic NFS driver:
    cfg.StrOpt('volume_backend_name',
               help='The backend name for a given driver implementation'),
    # options from hnas common
    cfg.IPOpt('hnas_mgmt_ip0',
              help='Management IP address of HNAS. This can '
                   'be any IP in the admin address on HNAS or '
                   'the SMU IP.'),
    cfg.StrOpt('hnas_username',
               help='HNAS username.'),
    cfg.StrOpt('hnas_password',
               secret=True,
               help='HNAS password.'),
    cfg.StrOpt('hnas_svc0_hdp',
               help='This is the same option from the HNAS cinder NFS driver '
                    'that describes itself as "Service 0 HDP". It is the url '
                    'of the NFS share being used to store the volumes. You '
                    'should just copy whatever is set up on your cinder.conf '
                    'into this value.'),
    cfg.StrOpt('hnas_svc0_pool_name',
               help='This is the same option from the HNAS cinder NFS driver '
                    'which describes itself as "Service 0 volume type". It '
                    'is an arbitrary string that is used to identify an HNAS '
                    'service. You should copy this value from the equivalent '
                    'value on your cinder.conf file.'),
    cfg.StrOpt('hnas_svc1_hdp',
               help='Service 1 HDP'),
    cfg.StrOpt('hnas_svc1_pool_name',
               help='Service 1 pool name'),
    cfg.StrOpt('hnas_svc2_hdp',
               help='Service 2 HDP'),
    cfg.StrOpt('hnas_svc2_pool_name',
               help='Service 2 pool name'),
    cfg.StrOpt('hnas_svc3_hdp',
               help='Service 3 HDP'),
    cfg.StrOpt('hnas_svc3_pool_name',
               help='Service 3 pool name'),
]
CONF = config.CONF


class HNASVolumeReference(object):
    """A reference to a volume file residing in HNAS

    An object that represents a volume file residing within HNAS,
    somewhere like
    /mnt/lb/evs12/fs-by-name/FS-TestCG/nfs_cinder/volume-8aa(...) .
    These objects are created by HNASCinderBackend objects, using the
    factory method 'get_volume_reference'
    """

    def __init__(self, hnas_backend, uuid, evs_idx):
        self.hnas_backend = hnas_backend
        self.uuid = uuid
        self.evs_idx = evs_idx
        self.update_volume_path()

    def rm_via_ssc(self):
        LOG.info("Deleting reference in HNAS via ssc...")
        self.hnas_backend.ssc_rm(self.evs_idx, self.fs_name, self.ssc_path,
                                 force=True)

    def get_nfs_url(self, svc_idx):
        """Returns a url that can be used to manage this volume

        :returns: A string similar to "10.0.0.1:/path/to/vol"

        The unix_path of a volume is something like
        /mnt/lb/evs12/fs-by-name/FS-TestCG/nfs_cinder/volume-8aa(...)
        And the svc_hdp of the hnas_backend is something like
        172.24.49.34:/nfs_cinder
        In order to get the volume path as it is visible from the outside
        we'll have to get this volume's unix_path and grab just the part
        after the share path, which can be retrieved via
        hnas_backend.export_path
        """
        self.update_volume_path()
        pth = self.unix_path.split(self.hnas_backend.export_path[svc_idx])[-1]
        if pth.startswith('/'):
            pth = pth[1:]
        return os.path.join(self.hnas_backend.svc_hdp[svc_idx], pth)

    def mark_as_unmanaged(self):
        """Prepends the filename in unix_path with 'unmanage-'

        This is what the driver does to unmanaged volumes. If a volume is
        remanaged, then you should get a new volume reference to it
        """
        fname = self.unix_path.split('/')[-1]
        self.unix_path = self.unix_path[: -len(fname)] + 'unmanage-' + fname

    def get_first_bytes(self, num_bytes):
        return self.hnas_backend.exec_command(
            ("dd if=%(vol_path)s bs=1 count=%(num_bytes)d" %
             {"vol_path": self.unix_path, "num_bytes": num_bytes}),
            sudo=True)

    def exists(self):
        return self.hnas_backend.exec_command("ls %s" % self.unix_path)

    def get_size(self):
        size = int(self.hnas_backend.exec_command(
            'ls -l %s | cut -d " " -f 5' % self.unix_path))
        return size / (1024 ** 3)

    def update_volume_path(self):
        paths = self.hnas_backend.permissive_find("/mnt/lb",
                                                  "*%s" % self.uuid)
        self.unix_path = None
        self.ssc_path = None
        self.fs_name = None
        for path in paths.split('\n'):
            if 'fs-by-name' in path:
                if self.unix_path is not None:
                    raise Exception(("Found two volumes with the same id: "
                                     "%s and %s" % (path, self.unix_path)))
                self.unix_path = path
                self.fs_name = path.split('fs-by-name/')[1].split('/')[0]
                self.ssc_path = self.unix_path.split(self.fs_name)[1]
                break
        else:
            raise Exception("Something went wrong while searching for volume "
                            "%s" % self.uuid)


class HNASClient(remote_client.RemoteClient):
    """An ssh client with some specific HNAS methods"""

    def __init__(self,
                 ip_address,
                 username,
                 password,
                 pkey=None,
                 server=None,
                 servers_client=None):
        self.ip_address = ip_address
        self.password = password
        super(HNASClient, self).__init__(ip_address, username, password, pkey,
                                         server, servers_client)

    def exec_command(self, command, sudo=False):
        if sudo:
            command = ("sudo -k && echo '%s' | sudo -S %s" %
                       (self.password, command))
        return super(HNASClient, self).exec_command(command)

    def permissive_find(self, path, pattern):
        """find command that does not fail if stdout is not empty

        Runs a find command that ignores non zero return codes as long as
        there is something in the stdout
        """
        try:
            sout = self.exec_command("find %s -name '%s'" % (path, pattern))
        except lib_exc.SSHExecCommandFailed as ex:
            sout = re.split('\nstdout:\n', ex._error_string)[1]
            if len(sout) == 0 or sout.isspace():
                raise ex
        return sout

    def ssc(self, command):
        fullcmd = "su supervisor -c 'ssc -u supervisor localhost \"%s\"'"
        fullcmd = fullcmd % command
        output = self.exec_command(fullcmd, sudo=True)
        return output

    def get_evs_by_ip(self, ips):
        all_evs = self.evs_list()
        evs_list = []
        for ip in ips:
            for evs in all_evs:
                if evs['IP Address'] == ip:
                    break
                if evs['IP Address'].__class__ == list:
                    for evs_ip in evs['IP Address']:
                        if ip == evs_ip:
                            break
                    else:
                        continue
                    break
            else:
                raise Exception("Could not find evs with ip %s" % ip)
            evs['host_path'] = ""
            evs_list.append(evs)
        return evs_list

    def evs_list(self):
        return self._table_to_list_of_dicts(self.ssc("evs list"))

    def _table_to_list_of_dicts(self, text):
        lines = text.split('\n')
        for line_idx in range(len(lines)):
            line = lines[line_idx]
            # if line entirely composed of dashes and space
            if ((re.search("^[- ]+$", line) is not None) and
                    (not line.isspace())):
                separator_line = line
                column_names_line = lines[line_idx - 1]
                data_line_begin_idx = line_idx + 1
                break

        # Create a regex based on the line composed of dashes. The number of
        # dashes is the maximum number of characters that can appear in each
        # field. The objective of this code is to go from a separator line
        # like this:
        # --- ------- ----- ------
        # to a regex like this:
        # (...) (.......) (.....) (......)
        # which will capture, from the lines of data, columns that fit into
        # the parenthesis
        line_parser_regex = ""
        for c in separator_line:
            if c == '-':
                # if beginning a new group of dashes
                if (len(line_parser_regex) == 0 or
                        line_parser_regex[-1] not in ['.', '(']):
                    line_parser_regex += '('

                line_parser_regex += '.'
            else:
                # ending group of dashes
                if len(line_parser_regex) and line_parser_regex[-1] == '.':
                    line_parser_regex += ')'
                line_parser_regex += c
        if line_parser_regex[-1] == '-':
            line_parser_regex += ')'

        def separate_columns(parser_regex, line_text):
            """Parses a line of text from a table and wraps it into an array

            Parses a line of text from a table emitted by hnas commands
            such as evs-list, retrieving the contents of each column and
            appending each one of them to an array.

            :param parser_regex: A string representing a regex like so:
                                 (...) (.....) (...) (....)
            :param line_text: The line of text that is to be parsed using the
                               regex passed in parser_regex
            :returns: An array representing the elements of the line being
                      parsed.
            """
            raw_columns = re.search(parser_regex, line_text).groups()
            return [c.strip() for c in raw_columns]

        col_names = separate_columns(line_parser_regex, column_names_line)

        table_data = []  # a list of dictionaries
        for data_line in lines[data_line_begin_idx:]:
            if len(data_line) == 0 or data_line.isspace():
                continue

            ordered_data = separate_columns(line_parser_regex, data_line)
            # if this is a continuation line, ...
            if len(ordered_data[0]) == 0:
                # ... then we'll append data to the last line
                d = table_data[-1]
                for d_idx in range(len(ordered_data)):
                    key = col_names[d_idx]
                    data = ordered_data[d_idx]
                    if len(data) == 0:
                        continue

                    if not d[key]:
                        d[key] = data
                        continue

                    if d[key].__class__ != list:
                        d[key] = [d[key]]
                    d[key].append(ordered_data[d_idx])
            else:
                d = {}
                for d_idx in range(len(ordered_data)):
                    key = col_names[d_idx]
                    d[key] = ordered_data[d_idx]
                table_data.append(d)
        return table_data

    def get_pids(self, pr_name):
        # Get pid(s) of a process/program
        cmd = "ps -ef | grep %s | grep -v 'grep' | awk {'print $2'}" % pr_name
        return self.exec_command(cmd).split('\n')


class InstanceBlockDevTester(object):
    def __init__(self, ssh_client, dev_name):
        self.ssh_client = ssh_client
        self.dev_name = dev_name
        self.dev_path = '/dev/%s' % self.dev_name

    def get_block_dev_number_of_sectors(self):
        return int(self.ssh_client.exec_command('cat /sys/block/%s/size' %
                                                self.dev_name))

    def get_block_dev_size_mb(self):
        return self.get_block_dev_number_of_sectors() / 2

    def write_to_top_of_block_dev(self, ascii_data):
        commands = []
        commands.append(
            "echo '%(data)s' | sudo dd of=%(dev_path)s" %
            {'data': ascii_data, 'dev_path': self.dev_path})
        commands.append("sync")
        out = self.ssh_client.exec_command(" && ".join(commands))
        return out

    def fill_with_random_data(self, size_mb, str_to_append=""):
        commands = []
        commands.append(
            "sudo dd if=/dev/urandom of=%(dev_path)s bs=1M count=%(size_mb)d" %
            {'dev_path': self.dev_path, 'size_mb': size_mb})
        commands.append(
            "echo '%(str_to_append)s' |"
            "sudo dd of=%(dev_path)s bs=1M seek=%(seek_mb)d" %
            {'str_to_append': str_to_append, 'dev_path': self.dev_path,
             'seek_mb': size_mb})
        commands.append("sync")
        out = self.ssh_client.exec_command(" && ".join(commands))
        return out

    def get_bytes_at_offset(self, offset_mb, num_bytes):
        out_str = self.ssh_client.exec_command(
            "sudo dd if=%(dev_path)s bs=1 skip=%(skip_bytes)d "
            "count=%(num_bytes)d" %
            {"dev_path": self.dev_path, 'skip_bytes': 1024 * 1024 * offset_mb,
             "num_bytes": num_bytes})
        return out_str


class HNASCinderBackend(HNASClient):
    """An ssh client with methods to verify HNAS inner workings.

    This class represents a cinder backend configuration like the ones
    configured in cinder.conf->DEFAULT->enabled_backends. It contains an
    ssh client to the underlying HNAS and several methods used to operate on
    such a backend.
    """

    def __init__(self,
                 name,
                 cinder_manage_host,
                 svc_pool_name,
                 hnas_ip,
                 svc_hdp,
                 volume_backend_name,
                 hnas_tester_user,
                 hnas_tester_password):
        self.name = name
        self.cinder_manage_host = cinder_manage_host
        self.svc_pool_names = svc_pool_name
        self.svc_hdp = svc_hdp
        self.evs_ips = []
        self.export_path = []
        for hdp in svc_hdp:
            self.evs_ips.append(hdp.split(':/')[0])
            self.export_path.append(hdp.split(':/')[1])

        self.volume_backend_name = volume_backend_name
        super(HNASCinderBackend, self).__init__(hnas_ip, hnas_tester_user,
                                                hnas_tester_password)
        self.evs_dict = self.get_evs_by_ip(self.evs_ips)
        self.evs_idx = []
        for evs in self.evs_dict:
            self.evs_idx.append(evs['EVS ID'])

    def get_volume_reference(self, uuid, svc_idx=0):
        return HNASVolumeReference(self, uuid, self.evs_idx[svc_idx])

    # def get_fs_name_from_dir(self, dirname):
    #    self.permissive_find()

    def get_ssc_file_cmd_prefix(self, evs_num, fs_name):
        prefix = r"vn %s && " % evs_num
        prefix += r"selectfs  %s" % fs_name
        return prefix

    def ssc_rm(self, evs_num, fs_name, path, force=True):
        force_flag = '-f' if force else ''
        commands = []
        commands.append(self.get_ssc_file_cmd_prefix(evs_num, fs_name))
        commands.append("rm %s %s" % (force_flag, path))
        return self.ssc(" && ".join(commands))

    def ls_iscsi_volume(self, evs_num, fs_name, volume_id):
        commands = []
        commands.append(self.get_ssc_file_cmd_prefix(evs_num, fs_name))
        commands.append(r"cd /.cinder")
        commands.append(r"ls -liah volume-%s.iscsi" % volume_id)
        return self.ssc(" && ".join(commands))

    def ls_volume(self, evs_num, fs_name, volume_id):
        commands = []
        commands.append(r"vn %s" % evs_num)
        commands.append(r"selectfs  %s" % fs_name)
        commands.append(r"cd /.cinder")
        commands.append(r"ls -liah *%s*" % volume_id)
        return self.ssc(" && ".join(commands))

    @classmethod
    def create_backends_from_conf(cls):
        """Parses tempest config file producing an array of HNASCinderBackends.

        """
        backends = []
        for bname in CONF.hnas.enabled_backends:
            CONF.register_opts(hnas_drivers_common_opts, bname)
            backend = getattr(CONF, bname)

            svcs_pool = [backend.hnas_svc0_pool_name,
                         backend.hnas_svc1_pool_name,
                         backend.hnas_svc2_pool_name,
                         backend.hnas_svc3_pool_name]
            svcs_hdp = [backend.hnas_svc0_hdp, backend.hnas_svc1_hdp,
                        backend.hnas_svc2_hdp, backend.hnas_svc3_hdp]
            svcs_pool_list = []
            svcs_hdp_list = []

            for pool, hdp in zip(svcs_pool, svcs_hdp):
                if pool and hdp:
                    svcs_pool_list.append(pool)
                    svcs_hdp_list.append(hdp)

            backends.append(
                HNASCinderBackend(
                    name=bname,
                    cinder_manage_host=CONF.volume.cinder_manage_host,
                    volume_backend_name=backend.volume_backend_name,
                    hnas_ip=backend.hnas_mgmt_ip0,
                    hnas_tester_user=backend.hnas_username,
                    hnas_tester_password=backend.hnas_password,
                    svc_hdp=svcs_hdp_list,
                    svc_pool_name=svcs_pool_list))
        return backends


class SSCLimitTester(object):
    """A tester for the HNAS maximum connection limit.

    The object must be created using the method create_ssc_limit_tester on
    BaseHNASTest to set up the cleaning process.
    """

    def __init__(self, hnas_backend, connections=5):
        self.connections = connections
        self.backend = hnas_backend
        self.pid_list = []
        self.error_has_occurred = False

    def open_connections(self):
        num_retries = 10
        wait_secs = 1
        ssc_command = "sleep 60"
        conn_list = []

        LOG.debug("Opening %s connections...", self.connections)
        while len(conn_list) < self.connections:
            name = 'ssc-thread-%s' % len(conn_list)

            conn = SSCThread(name, self.backend, ssc_command)
            conn.start()

            time.sleep(1)
            if conn.is_alive:
                conn_list.append(conn)
            else:
                num_retries -= 1
                LOG.debug(("Failed to open connection. Retrying in "
                           "%(wait_secs)s sec "
                           "(%(num_retries)s tries left)..."),
                          {'wait_secs': wait_secs,
                           'num_retries': num_retries})
                time.sleep(wait_secs)

            if num_retries == 0:
                break

        full_command = ('\'ssc -u supervisor localhost "%s"\'' % ssc_command)
        self.pid_list = self.backend.get_pids(pr_name=full_command)

    def close_connections_on_error(self):
        """Starts a thread to wait for errors and to close connections.

        Starts a thread to wait for error on cinder log. If the error shows up
        on the log file, the connections are closed.
        """
        thread = threading.Thread(target=self._run_close_connections_on_error)
        thread.start()

    def _run_close_connections_on_error(self):
        self._wait_for_cinder_log('Failed to establish SSC connection')

        if self.error_has_occurred:
            self.close_connections()

    def _wait_for_cinder_log(self, expected_str, start_pos=None, timeout=5):
        """Waits for errors on cinder log.

        :param expected_str: A message to be found on log file.
        :param start_pos: A file position to start reading the log file.
        :param timeout: A time in seconds to leave the function if the
            expected_str is not found.
        :return: None
        """
        LOG.debug("Reading cinder log file...")

        with open('/home/ubuntu/devstack_logs/c-vol.log', 'r') as f:
            if not start_pos:
                f.seek(0, 2)
            else:
                f.seek(start_pos)
            start_time = time.time()
            while True:
                where = f.tell()
                line = f.readline()
                if not line:
                    time.sleep(1)
                    f.seek(where)
                elif expected_str in line:
                    LOG.debug(line)
                    self.error_has_occurred = True
                    return

                if time.time() - timeout > start_time:
                    LOG.debug("Reading cinder log timed out.")
                    return

    def close_connections(self):
        LOG.debug("Closing connections...")
        for pid in self.pid_list:
            if pid:
                try:
                    self.backend.send_signal(pid=pid, signum=15)
                except lib_exc.SSHExecCommandFailed as e:
                    if 'No such process' in str(e):
                        LOG.debug("Connection already closed.")
                    else:
                        raise


class SSCThread(threading.Thread):

    def __init__(self, name, hnas_backend, command):
        threading.Thread.__init__(self)
        self.backend = hnas_backend
        self.name = name
        self.command = command

    def run(self):
        try:
            result = self.backend.ssc(self.command)
            LOG.debug("Thread %s result: %s", (self.name, result))
        except lib_exc.SSHExecCommandFailed as e:
            LOG.debug("%s - %s", (self.name, str(e)))
