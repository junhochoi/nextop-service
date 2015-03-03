import os
import subprocess
import paramiko

class Struct:
	def __init__(self, **kwds):
		self.__dict__.update(kwds)

hosts = {'dev': '54.149.233.13'}
users = {}

def remote(host, cmd):
	user = users.get(host, 'ubuntu')
	print('ssh {user}@{host} {cmd}'.format(user=user, host=host, cmd=cmd))
	ssh = paramiko.SSHClient()
	ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	ssh.connect(host, username=user)
	stdin, stdout, stderr = ssh.exec_command(cmd)
	# FIXME subprocess.communicate() is better than this ... figure out how to get that level here
	for line in filter(lambda line: line, map(lambda line: line.strip(), stdout.readlines() + stderr.readlines())):
		print(line)

def local(cmd):
	print(cmd)
	p = subprocess.Popen(cmd, shell=True)
	p.communicate()


def admin_cmds(admin_module, instance, port):
	image='nxdeploy/%s' % (admin_module)
	tag='%s-%s' % (os.environ['NEXTOP_TOOLS_HASH'], os.environ['NEXTOP_SERVICE_HASH'])

	is_local = 'local' == instance
	cmd_docker = 'docker' if is_local else 'sudo docker'

	cmd_push=''.join([cmd_docker, """ push """, image, ':', tag])
	cmd_pull=''.join([cmd_docker, """ pull """, image, ':', tag])
	# CONTAINER ID        IMAGE               COMMAND             CREATED             STATUS              PORTS               NAMES
	cmd_stop=''.join(["""for cid in `""", cmd_docker, """ ps -a | tail -n +2 | awk 'BEGIN {FS=" {2,}|:"} $2==""", '"', image, '"', """ {print $1}'`; do """, cmd_docker, """ stop $cid; """, cmd_docker, """ rm $cid; done"""])
	cmd_start=''.join([cmd_docker, """ run -d -p """, str(port), ':', str(port), """ """, image, """:""", tag, """ start -c /etc/nextop/service/""", admin_module, '/', instance, """.conf.json"""])

	instance_runner = local if is_local else lambda cmd: remote(hosts[instance], cmd)

	return Struct(cmd_push=cmd_push, cmd_pull=cmd_pull, cmd_stop=cmd_stop, cmd_start=cmd_start, instance_runner=instance_runner)

def overlord_cmds(instance, host, port, local_key, access_key):
	if not host:
		host = hosts.get(instance, '')
	
	image='nxdeploy/overlord'
	tag='%s-%s' % (os.environ['NEXTOP_TOOLS_HASH'], os.environ['NEXTOP_SERVICE_HASH'])

	is_local = 'local' == instance
	cmd_docker = 'docker' if is_local else 'sudo docker'

	cmd_push=''.join([cmd_docker, """ push """, image, """:""", tag])
	cmd_pull=''.join([cmd_docker, """ pull """, image, """:""", tag])
	# CONTAINER ID        IMAGE               COMMAND             CREATED             STATUS              PORTS               NAMES
	cmd_stop=''.join(["""for cid in `""", cmd_docker, """ ps -a | tail -n +2 | awk 'BEGIN {FS=" {2,}"} $7==""", '"', local_key, '"', """ {print $1}'`; do """, cmd_docker, """ stop $cid; """, cmd_docker, """ rm $cid; done"""])
	cmd_start=''.join([cmd_docker, """ run -d -p """, str(port), ':', str(port), """ -p """, str(port - 5000), ':', str(port - 5000), """ -p """, str(port - 10000), ':', str(port - 10000), """ --name=""", '"', local_key, '" ', image, """:""", tag, """ start -c /etc/nextop/service/overlord/""", instance, """.conf.json -a """, access_key, """ -P """, str(port)])

	instance_runner = local if is_local else lambda cmd: remote(host, cmd)

	return Struct(cmd_push=cmd_push, cmd_pull=cmd_pull, cmd_stop=cmd_stop, cmd_start=cmd_start, instance_runner=instance_runner)

def nx_cmds(instance, subcommand_args):
	image='nxdeploy/cli'
	tag='%s-%s' % (os.environ['NEXTOP_TOOLS_HASH'], os.environ['NEXTOP_SERVICE_HASH'])

	is_local = 'local' == instance
	cmd_docker = 'docker' if is_local else 'sudo docker'

	cmd_push=''.join([cmd_docker, """ push """, image, ':', tag])
	cmd_pull=''.join([cmd_docker, """ pull """, image, ':', tag])
	cmd_run=''.join([cmd_docker, """ run """, image, ':', tag, ' ', ' '.join(subcommand_args)])

	instance_runner = local if is_local else lambda cmd: remote(hosts[instance], cmd)

	return Struct(cmd_push=cmd_push, cmd_pull=cmd_pull, cmd_run=cmd_run, instance_runner=instance_runner)

