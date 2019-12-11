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
from stat import S_ISDIR
global sftp

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
    if client:
        print("Connection to ", hostName, " is successful")

    return client

def get_ssh():
    ssh_client = getSSHConnection('dc1-c-rsp-bastion-01.responsys.net')

    ls = 'ls'
    pwd = 'pwd'
    print("\nRunning command :", ls)
    stdin, stdout, stderr = ssh_client.exec_command(ls)
    for i in stdout.readlines():
        print(i.strip())


def get_sftp():
    global sftp
    ssh_client = getSSHConnection()
    # ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    sftp = create_sftp_client('files-nonprod.dc2.responsys.net', 22, 'qa15_scp', 'D:\\Responsys\\qatestkey.pem', 'RSA')
    # print(sftp.listdir('.'))
    download_dir('/home/cli/qa15_scp/Jagan/Feeds/qa2connect', 'C:\\Users\\jaganpat.ORADEV\\PycharmProjects\\RSYSFeeds\\InputFIles')
    # sftp.get('/home/cli/qa15_scp/Jagan/Filter&DM_TestList_CommaNDoubleQoute.csv', 'C:\\Users\\jaganpat.ORADEV\\PycharmProjects\\RSYSFeeds\\InputFIles\\')
    sftp.close()
    # admin_server = ssh_client.connect('admin01-qa1.qa1.responsys.com', username='jpatil', password='patil@910')
    # if admin_server:
    #     print("Connected to admin server")
    # else:
    #     print("Connection to admin server failed")
    #
    # admin_server.connect('files-nonprod.dc2.responsys.net', username='qa15_scp', password='D:\\Responsys\\qatestkey.pem')
    # sftp = ssh_client.open_sftp()
    # print(sftp.getcwd())

def connect_to_sftp_ssh():
    return

def download_dir(remote_dir, local_dir):
    import os
    os.path.exists(local_dir) or os.makedirs(local_dir)
    dir_items = sftp.listdir_attr(remote_dir)
    # dir_items = sftp.listdir(remote_dir)
    print("Copying ", len(dir_items), "files from ",remote_dir, "to local")
    for item in dir_items:
        # assuming the local system is Windows and the remote system is Linux
        # os.path.join won't help here, so construct remote_path manually
        remote_path = remote_dir + '/' + item.filename
        local_path = os.path.join(local_dir, item.filename)
        if S_ISDIR(item.st_mode):
            download_dir(remote_path, local_path)
        else:
            print("Copying file : ", item.filename)
            sftp.get(remote_path, local_path)
def create_sftp_client(host, port, username,  keyfilepath, keyfiletype,password=None):
    """
    create_sftp_client(host, port, username, password, keyfilepath, keyfiletype) -> SFTPClient

    Creates a SFTP client connected to the supplied host on the supplied port authenticating as the user with
    supplied username and supplied password or with the private key in a file with the supplied path.
    If a private key is used for authentication, the type of the keyfile needs to be specified as DSA or RSA.
    :rtype: SFTPClient object.
    """
    sftp = None
    key = None
    transport = None
    try:
        if keyfilepath is not None:
            # Get private key used to authenticate user.
            if keyfiletype == 'DSA':
                # The private key is a DSA type key.
                key = paramiko.DSSKey.from_private_key_file(keyfilepath)
            else:
                # The private key is a RSA type key.
                key = paramiko.RSAKey.from_private_key(open(keyfilepath))

        # Create Transport object using supplied method of authentication.
        transport = paramiko.Transport((host, port))
        transport.connect(None, username, password, key)

        sftp = paramiko.SFTPClient.from_transport(transport)
        if sftp:
            print("Connection to ", host, " is successful")

        return sftp
    except Exception as e:
        print('An error occurred creating SFTP client: %s: %s' % (e.__class__, e))
        if sftp is not None:
            sftp.close()
        if transport is not None:
            transport.close()
        pass
get_sftp()
# getSSHConnection()