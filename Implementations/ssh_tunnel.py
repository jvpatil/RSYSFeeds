import socket
from contextlib import closing
from datetime import time

# import paramiko
# import os
# import sys
#
# from paramiko import SSHConfig, AutoAddPolicy, SSHClient, ProxyCommand
#
#
#
# def paramiko_connect(host):
#     client = paramiko.SSHClient()
#     client._policy = paramiko.WarningPolicy()
#     client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#
#     ssh_config = paramiko.SSHConfig()
#     user_config_file = os.path.expanduser("C:\\cygwin64\\home\\jaganpat\\.ssh\\config.dat")
#     try:
#         with open(user_config_file) as f:
#             ssh_config.parse(f)
#     except FileNotFoundError:
#         print("{} file could not be found. Aborting.".format(user_config_file))
#         sys.exit(1)
#     options = ssh_config.lookup(host)
#     cfg = {'hostname': options['hostname'], 'username': options["user"]}
#
#     # user_config = ssh_config.lookup(cfg['hostname'])
#     user_config = ssh_config.lookup(host)
#     for k in ('hostname', 'user', 'port'):
#         if k in user_config:
#             cfg[k] = user_config[k]
#
#     if 'proxycommand' in user_config:
#         cfg['sock'] = paramiko.ProxyCommand(user_config['proxycommand'])
#
#     return client.connect(**cfg)
#
# paramiko_connect("dc1-c-rsp-bastion-01.responsys.net")

# def ssh(host, forward_agent=False, sudoable=False, max_attempts=1, max_timeout=5):
#     """Manages a SSH connection to the desired host.
#        Will leverage your ssh config at ~/.ssh/config if available
#
#     :param host: the server to connect to
#     :type host: str
#     :param forward_agent: forward the local agents
#     :type forward_agent: bool
#     :param sudoable: allow sudo commands
#     :type sudoable: bool
#     :param max_attempts: the maximum attempts to connect to the desired host
#     :type max_attempts: int
#     :param max_timeout: the maximum timeout in seconds to sleep between attempts
#     :type max_timeout: int
#     :returns a SSH connection to the desired host
#     :rtype: Connection
#
#     :raises MaxConnectionAttemptsError: Exceeded the maximum attempts
#     to establish the SSH connection.
#     """
#     with closing(SSHClient()) as client:
#         client.set_missing_host_key_policy(AutoAddPolicy())
#
#         cfg = {
#             "hostname": host,
#             "timeout": max_timeout,
#         }
#
#         ssh_config = SSHConfig()
#         user_config_file = os.path.expanduser("C:\\cygwin64\\home\\jaganpat\\.ssh\\config.dat")
#         if os.path.exists(user_config_file):
#             with open(user_config_file) as f:
#                 ssh_config.parse(f)
#                 host_config = ssh_config.lookup(host)
#                 if "user" in host_config:
#                     cfg["username"] = host_config["user"]
#
#                 if "proxycommand" in host_config:
#                     cfg["sock"] = ProxyCommand(host_config["proxycommand"])
#
#                 if "identityfile" in host_config:
#                     cfg['key_filename'] = host_config['identityfile']
#
#                 if "port" in host_config:
#                     cfg["port"] = int(host_config["port"])
#
#         attempts = 0
#         while attempts < max_attempts:
#             try:
#                 attempts += 1
#                 client.connect(**cfg)
#                 break
#             except socket.error:
#                 if attempts < max_attempts:
#                     time.sleep(max_timeout)
#         # else:
#         #     raise MaxConnectionAttemptsError(
#         #         "Exceeded max attempts to connect to host: {0}".format(max_attempts)
#         #     )
#         #
#         # yield Connection(client, forward_agent, sudoable)
#
# ssh("dc1-c-rsp-bastion-01.responsys.net")

from paramiko import SSHClient, SSHConfig, SSHException
import getpass
import paramiko


def getSSHConnection(hostName="dc1-c-rsp-bastion-01.responsys.net"):
    config = SSHConfig()

    user = getpass.getuser()
    # config.parse(open('C:/Users/' + user +'/.ssh/config'))
    config.parse(open('C:\\cygwin64\\home\\jaganpat\\.ssh\\config.dat'))
    host=config.lookup(hostName)


     # setup SSH client
    client = SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

     #Check for proxy settings
    try:
        print(host ['proxycommand'])
        # proxy = paramiko.ProxyCommand(host['proxycommand'])
        proxy = paramiko.ProxyCommand("ssh -o StrictHostKeyChecking=no jumphostIP nc targethostIP 22")
    except:
            proxy = None
    #Setup the SSH connection
    try:
        passwd = "ocna@123" + str(input("Please enter password : "))
        if (proxy is None):
            # client.connect(host['hostname'],22, username=host['user'],key_filename=host['identityfile'])
            client.connect(host['hostname'],22, username=host['user'],password=passwd)
        else:
            # client.connect(host['hostname'],22, username=host['user'],  key_filename=host['identityfile'], sock=proxy)
            client.connect(host['hostname'],22, username=host['user'], password=passwd, sock=proxy)
    except SSHException as ex:
        print(ex)

    return client


# ssh_client = getSSHConnection('dc1-c-rsp-bastion-01.responsys.net')
#
# # run a command
# print("\nRun a command")
# cmd = 'ls'
# stdin, stdout, stderr = ssh_client.exec_command(cmd)
#
# print(stdout.read())

# ssh_client = getSSHConnection('dc1-c-rsp-bastion-01.responsys.net')
#
# # run a command
# print("\nRun a command")
# cmd = 'ps aux'
# stdin, stdout, stderr = ssh_client.exec_command(cmd)
#
# print(stdout.read())