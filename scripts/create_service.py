#!/usr/bin/env python3
import os
import subprocess
from os.path import join, abspath, dirname, exists
import stat
import getpass

if os.getuid() != 0:
    print("Need to run as root")
    exit(1)

near_release_path = abspath(join(dirname(__file__), '../target/release/near'))
near_debug_path = abspath(join(dirname(__file__), '../target/debug/near'))

default_near_path = None
if exists(near_release_path):
    default_near_path = near_release_path
elif exists(near_debug_path):
    default_near_path = near_debug_path
default_near_args = 'run'
default_near_user = getpass.getuser()

near_user = raw_input("Enter user to run service, default: '{}': ".format(
    default_near_user)) or default_near_user
near_path = raw_input("Enter the nearcore binary path, default '{}': ".format(
    default_near_path if default_near_path else "")) or default_near_path
near_args = raw_input("Enter args, default: '{}': ".format(
    default_near_args)) or default_near_args

template = open(join(dirname(__file__), './near.service.template')).read()
service_file = template.format(exec_start=near_path + " " + near_args,
                               user=near_user)

service_file_path = '/etc/systemd/system/near.service'
with open(service_file_path, 'w') as f:
    f.write(service_file)

st = os.stat(service_file_path)
os.chmod(service_file_path,
         st.st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
print("""
Service created at {}.
To start: sudo systemctl start near
To start on boot: sudo systemctl enable near
To view log: sudo systemctl status near
To stop: sudo sytemctl stop near
To disable start on boot: sudo systemctl disable near
""".format(service_file_path))
