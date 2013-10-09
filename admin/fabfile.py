from fabric.api import local, settings, abort, run, env, hide, roles, cd, sudo
from fabric.contrib.console import confirm
from fabric.operations import *
from fabric.colors import *
import re
import inspect
from contextlib import closing
from zipfile import ZipFile, ZIP_DEFLATED
import os
from time import strftime
import datetime

env.disable_known_hosts = True

def deploy():
    #Build as linux
    local('GOOS=linux go build goshire_admin.go')
    upload()
    cmd = 'cd /mnt/goshire_admin && sudo -u trendrr ./goshire_admin --config=goshire_admin.yaml'

    service_start('goshire_admin', cmd)
    
def upload():
    sudo('mkdir -p /mnt/goshire_admin')
    sudo('mkdir -p /mnt/goshire_admin/data')
    sudo('mkdir -p /mnt/goshire_admin/views')
    sudo('mkdir -p /mnt/goshire_admin/static')

    put('goshire_admin', '/mnt/goshire_admin', use_sudo=True)
    put('goshire_admin.yaml', '/mnt/goshire_admin', use_sudo=True)
    put('views/**', '/mnt/goshire_admin/views', use_sudo=True)
    put('static/**', '/mnt/goshire_admin/static', use_sudo=True)


    sudo('chown -R trendrr:trendrr /mnt/goshire_admin')
    sudo('chmod +x /mnt/goshire_admin/goshire_admin')
    
def log():
    sudo('tail -f /var/log/upstart/goshire_admin.log')

def service_start(name, command):
     # creates a new service that can start/stop and automatically starts on server reboot.
    service_stop(name)
    
    sudo('touch /etc/init/%s.conf' % name)
    conf = '''
    #
    # Trendrr service deployed from fab
    #
    start on (net-device-up
              and local-filesystems
              and runlevel [2345])
    script
        %s
    end script

    kill timeout 180
    ''' % (command)
    sudo('echo "%s" | sudo tee -a /etc/init/%s.conf' % (conf,name))

    sudo('start %s' % name)


def service_stop(name):  
    with settings(warn_only=True): 
        sudo('rm /etc/init/%s.conf' % name)
    with settings(warn_only=True): 
        sudo('stop %s' % name)    

  
    
def node(): 
    env.user = raw_input(green('Please enter your username for the server:'))
    sudo("echo 'Please enter your password NOW'") # force the password.