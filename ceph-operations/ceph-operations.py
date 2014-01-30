import ConfigParser
import os
import subprocess
import sys
import time

import paramiko

from argparse import ArgumentParser, ArgumentError
from os.path import expanduser, isfile


config = ConfigParser.ConfigParser()
config_file = 'deploy_config.ini'


def read_config(config_file):
    config.read(config_file)


def get_config_section_map(section, sub_section):
    conf_dict = {}
    options = config.options(section)
    for option in options:
        if option == sub_section:
            conf_dict[option] = config.get(section, option)
        else:
            pass
    return conf_dict


def execute_shell_command(command):
    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    while True:
        out = p.stdout.read(1)
        if out == '' and p.poll() is not None:
            break
        if out != '':
            sys.stdout.write(out)
            return out


def remote_ssh(server, username, command):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(server, username=username)
    stdin, stdout, stderr = ssh.exec_command(command)
    for line in stdout.readlines():
        print line


def add_new_monitor(ceph_home):
    pass


def remove_existing_monitor(ceph_home):
    pass


def add_new_osd(ceph_home):
    read_config(config_file)
    os.chdir(ceph_home)

    '''
        Create the OSD. If no UUID is given, it will be set automatically
        when the OSD starts up
    '''

    osd_create_command = 'ceph osd create'
    osd_no = execute_shell_command(osd_create_command)

    '''
       Create the default directory on your new OSD.
    '''

    osd_dir = 'sudo mkdir -p /var/lib/ceph/osd/ceph-%s' % (osd_no)
    remote_ssh('ceph-node4', 'ceph', osd_dir)

    '''
        If the OSD is for a drive other than the OS drive, prepare it for use
        with Ceph, and mount it to the directory you just created

        Create and mount the device
    '''
    osd_nodes = {}
    conf_dict = get_config_section_map('NEW_OSD', 'osd_nodes')
    for node in conf_dict.values():
        for osd_node in node.split(','):
            node = osd_node.strip().split(':')
            osd_nodes[node[0]] = node[1]

    '''
       Format and mount device on all nodes
    '''

    for node in osd_nodes.keys():
        format_system = 'sudo mkfs -t xfs -f /dev/%s' % (osd_nodes[node])
        remote_ssh(node, 'ceph', format_system)
        mount_command = 'sudo mount  /dev/%s /var/lib/ceph/osd/ceph-%s' % (
            osd_nodes[node], osd_no)
        remote_ssh(node, 'ceph', mount_command)

        '''
            Install ceph on the new node and copy configurations file
            from admin node in /etc/ceph directory
        '''

        command = 'ceph-deploy install %s' % (node)
        execute_shell_command(command)

        '''
            Copy configurations file to a new node using ceph-deploy
        '''

        command = 'ceph-deploy admin %s' % (node)
        execute_shell_command(command)

    for node in osd_nodes.keys():
        '''
            Initialize the OSD data directory.
        '''

        init_command = 'sudo ceph-osd -i %s --mkfs --mkkey' % (osd_no)
        remote_ssh(node, 'ceph', init_command)

        '''
            Register ceph osd key
        '''

        register_command = 'sudo ceph auth add osd.%s osd "allow *" mon \
<<<<<<< HEAD
"allow rwx" -i /var/lib/ceph/osd/ceph-%s/keyring' % (osd_no, osd_no)

=======
                           "allow rwx" -i /var/lib/ceph/osd/ceph-%s/\
                            keyring' % (osd_no, osd_no)
>>>>>>> 3d67b9670c7bf38631ea2454092adbb2f2d3b483
        remote_ssh(node, 'ceph', register_command)

        '''
            Add node to the bucket
        '''

        command = 'ceph osd crush add-bucket %s host' % (node)
        remote_ssh(node, 'ceph', command)

        command = 'ceph osd crush move %s root=default' % (node)
        remote_ssh(node, 'ceph', command)

        '''
           Add OSD to crush map
        '''

        # may need to change
        command = 'ceph osd crush add osd.%s 1.0 host=%s' % (osd_no, node)
        remote_ssh(node, 'ceph', command)

        '''
           Start the OSD
        '''

        start_command = 'sudo start ceph-osd id=%s' % (osd_no)
        remote_ssh(node, 'ceph', start_command)


def remove_existing_osd(ceph_home):
    pass


def ceph_install(ceph_home):
    read_config(config_file)

    '''
        Create a directory on your admin node node for maintaining the
        configuration that ceph-deploy generates for our cluster.
        If directory exists empty the directory contents
    '''

    if os.path.isdir(ceph_home):
        for file in os.listdir(ceph_home):
            file_path = os.path.join(ceph_home, file)
            os.unlink(file_path)
    else:
        execute_shell_command('mkdir %s' % ceph_home)

    os.chdir(ceph_home)

    '''
        Create a cluster
    '''

    conf_dict = get_config_section_map('INSTALL', 'mon_nodes')
    ceph_nodes = conf_dict.values()[0].replace(',', '')
    command = 'ceph-deploy new %s' % (ceph_nodes)
    execute_shell_command(command)

    '''
       Install ceph
    '''

    conf_dict = get_config_section_map('INSTALL', 'ceph_nodes')
    ceph_nodes = conf_dict.values()[0].replace(',', '')
    command = 'ceph-deploy install %s' % (ceph_nodes)
    execute_shell_command(command)

    '''
       Add a Ceph Monitor
    '''

    conf_dict = get_config_section_map('INSTALL', 'mon_nodes')
    ceph_nodes = conf_dict.values()[0].replace(',', '')
    command = 'ceph-deploy mon create %s' % (ceph_nodes)
    execute_shell_command(command)
    '''
       Gather Keys
    '''

    conf_dict = get_config_section_map('INSTALL', 'mon_nodes')
    monitor_nodes = conf_dict.values()[0].replace(',', '')

    command = 'ceph-deploy gatherkeys %s' % (monitor_nodes)
    execute_shell_command(command)
    while isfile('ceph.client.admin.keyring') is False:
        print "Quorum status not reached, trying after 5 seconds"
        time.sleep(5)
        execute_shell_command(command)

    '''
       Add OSD
    '''

    osd_nodes = {}
    conf_dict = get_config_section_map('INSTALL', 'osd_nodes')
    for node in conf_dict.values():
        for osd_node in node.split(','):
            node = osd_node.strip().split(':')
            osd_nodes[node[0]] = node[1]

    osd_devices = []
    for node in osd_nodes.keys():
        osd_devices.append('%s:%s:/dev/%s' %
                           (node, osd_nodes[node], osd_nodes[node]))

    osd_device_install = ' '.join(osd_devices)

    '''
       Prepare OSDs
    '''

    prepare_command = 'ceph-deploy osd prepare %s' % (osd_device_install)
    execute_shell_command(prepare_command)

    '''
       Activate OSDs
    '''

    activate_command = 'ceph-deploy osd activate %s' % (osd_device_install)
    execute_shell_command(activate_command)

    '''
       Copy Configurations
    '''

    conf_dict = get_config_section_map('INSTALL', 'ceph_nodes')
    ceph_nodes = conf_dict.values()[0].replace(',', '')
    command = 'ceph-deploy admin ceph-client %s' % (ceph_nodes)
    execute_shell_command(command)


def main():
    parser = ArgumentParser(description="Ceph Operations")

    parser.add_argument('operation', type=str,
                        help='install/add_monitor/remove_monitor/add_osd/\
                        remove_osd')
    parser.add_argument('-c', '--ceph_dir',
                        help='Ceph home directory', required=True)
    parser.add_argument('-u', '--username', type=str,
                        help='The username for the server', default='ceph')
    parser.add_argument('-v', '--version', action='version',
                        version='%(prog)s 1.0')

    try:
        args = parser.parse_args()
    except ArgumentError, exc:
        print exc.message, '\n', exc.argument

    if args.operation:
        operation = args.operation

    if args.ceph_dir:
        ceph_dir = args.ceph_dir

    if operation == 'install':
        ceph_install(ceph_dir)

    if operation == 'add_monitor':
        add_new_monitor(ceph_dir)

    if operation == 'remove_monitor':
        remove_existing_monitor(ceph_dir)

    if operation == 'add_osd':
        add_new_osd(ceph_dir)

    if operation == 'remove_osd':
        remove_existing_osd(ceph_dir)

    else:
        print "Coming soon............"
        sys.exit()

if __name__ == '__main__':
    main()
