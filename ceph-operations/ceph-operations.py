import ConfigParser
import os
import subprocess
import sys
from argparse import ArgumentParser, ArgumentError
from os.path import expanduser

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
    p = subprocess.Popen(command, shell=True, stderr=subprocess.PIPE)
    while True:
        out = p.stderr.read(1)
        if out == '' and p.poll() != None:
            break
        if out != '':
            sys.stdout.write(out)    

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
        execute_shell_command('mkdir %s' %ceph_home)
    
    os.chdir(ceph_home)
    
    '''
        Create a cluster
    '''
    
    conf_dict = get_config_section_map('INSTALL', 'mon_nodes')
    ceph_nodes = conf_dict.values()[0].replace(',','')
    command = 'ceph-deploy new %s' %(ceph_nodes)
    execute_shell_command(command)
    
    '''    
       Install ceph
    '''
    
    conf_dict = get_config_section_map('INSTALL', 'ceph_nodes')
    ceph_nodes = conf_dict.values()[0].replace(',','')
    command = 'ceph-deploy install %s' %(ceph_nodes)
    execute_shell_command(command)
    
    '''
       Add a Ceph Monitor
    '''
    
    conf_dict = get_config_section_map('INSTALL', 'mon_nodes')
    ceph_nodes = conf_dict.values()[0].replace(',','')
    command = 'ceph-deploy mon create %s' %(ceph_nodes)
    execute_shell_command(command)
    
    '''
       Gather Keys
    '''
    
    conf_dict = get_config_section_map('INSTALL', 'mon_nodes')
    monitor_nodes = conf_dict.values()[0].replace(',','')
    command = 'ceph-deploy gatherkeys %s' %(monitor_nodes)
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
        osd_devices.append('%s:%s:/dev/%s' %(node, osd_nodes[node], osd_nodes[node]))
    
    osd_device_install = ' '.join(osd_devices)

    '''
       Prepare OSDs
    '''
    
    prepare_command = 'ceph-deploy osd prepare %s' %(osd_device_install)
    execute_shell_command(prepare_command)

    ''' 
       Activate OSDs
    '''

    activate_command = 'ceph-deploy osd activate %s' %(osd_device_install)
    execute_shell_command(activate_command)
    
    '''
       Copy Configurations
    '''
    
    conf_dict = get_config_section_map('INSTALL', 'ceph_nodes')
    ceph_nodes = conf_dict.values()[0].replace(',','')
    command = 'ceph-deploy admin ceph-client %s' %(ceph_nodes)
    execute_shell_command(command)
    
    
def main():
    parser = ArgumentParser(description="Ceph Operations")
         
    parser.add_argument('operation',type=str, help='install/add_monitor/remove_monitor/add_osd/remove_osd')
    parser.add_argument('-c', '--ceph_dir', help='Ceph home directory', required=True)  
    parser.add_argument('-u', '--username', type=str, help='The username for the server', default='ceph')
    parser.add_argument('-v', '--version', action='version', version='%(prog)s 1.0')

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
        
    else:
        print "Coming soon............"
        sys.exit()
            
if __name__ == '__main__':
    main()
