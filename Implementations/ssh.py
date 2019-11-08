# from paramiko import SSHClient
#
# ssh = SSHClient()
# ssh.load_system_host_keys()
# ssh.connect('jaganpat@dc1-c-rsp-bastion-01.responsys.net')
# # ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command('ls')
# # print(ssh_stdout) #print the output of ls command

import paramiko
import time
import sys

def connect_to_ssh():
    ip = "dc1-c-rsp-bastion-01.responsys.net"
    name = "jaganpat"
    ssh_client = paramiko.SSHClient()
    # ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        ssh_client.exec_command("cd C:\\cygwin64\\home\\jaganpat\\.ssh" )
        ssh_client.exec_command("ssh tunnels -F config.dat")
        password = "ocna@123" + str(input("Please enter Password : "))
        print("Starting Connection ... ")
        ssh_client.connect(hostname=ip,username=name,password=password)
        if not ssh_client.get_transport().authenticated:
            print("SSH session failed on login.")
            print(str(ssh_client))
        else:
            print("SSH session login successful")
            stdin,stdout,stderr=ssh_client.exec_command('pwd')
            outlines=stdout.readlines()
            resp=''.join(outlines)
            print("Present working directory is : ", resp)
    except Exception as e:
        print(e)
    # ssh_client.close()
    # if ssh_client.get_transport() is not None:
    #      if not (ssh_client.get_transport().is_active()):
    #          print("SSH Conection is Still Alive")
    # else:
    #     print("SSH Conection is Closed")

connect_to_ssh()