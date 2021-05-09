from commands import getoutput


class LinuxUtility:

    def __init__(self, output_type, host=None, node=None):
        self.conn = 'command'
        self.update_main_command(host, node)
        self.output_function = 'OutputUtility(self.output).return_as_%s()' % output_type

    def update_main_command(self, host, node):
        if host and node:
            self.conn = 'ssh %s "ssh %s command"' % (host, node)
        elif host:
            self.conn = 'ssh %s "command"' % host

    def output_decorator(func):

        def wrapper(self, *args):
            self.output = func(self, *args)
            final_output = eval(self.output_function)
            return final_output

        return wrapper

    @output_decorator
    def latest_file_from_path(self, path):
        command = " ls -lrt %s | tail -1 | gawk -F' ' '{{print \\$9}}' " % path
        return getoutput(self.conn.replace('command', command))

    @output_decorator
    def list_of_dirs(self, path):
        command = "ls -lrt %s | grep ^d | awk -F' ' '{{print \\$9}}' " % path
        return getoutput(self.conn.replace('command', command))

    @output_decorator
    def get_count_of_files(self, path):
        command = ' ls %s | wc -l' % path
        return getoutput(self.conn.replace('command', command))

    @output_decorator
    def get_list_of_files(self, path, pattern=' '):
        command = " ls -lrt %s | grep '%s' | gawk -F' ' '{print $9}' " % (path, pattern)
        return getoutput(self.conn.replace('command', command))

    @output_decorator
    def get_size_of_dir(self, path):
        command = ''' du -ks %s | gawk -F' ' '{print $1}' ''' % path
        return getoutput(self.conn.replace('command', command))

    @output_decorator
    def get_size_of_files(self, path, pattern=' '):
        command = "ls -lrt %s*%s*| gawk -F' ' '{print $5}'| gawk '{print sum1+=$1}'| tail -1" % (path, pattern)
        return getoutput(self.conn.replace('command', command))

    @output_decorator
    def get_nodes(self, node):
        command = "source /opt/nsn/ngdb/ifw/lib/platform/utils/platform_definition.sh; echo \\${%s[@]} " % node
        return getoutput(self.conn.replace('command', command))

    @output_decorator
    def grep_output(self, log_file, *patterns):
        grep_command = "grep '{0}' {1}".format(patterns[0], log_file)
        for pattern in patterns[1:]:
            grep_command += "| grep '%s'" % pattern
        return getoutput(self.conn.replace('command', grep_command))

    @output_decorator
    def count_from_grep_output(self, log_file, *patterns):
        count_command = "grep -c '{0}' {1}".format(patterns[0], log_file)
        for pattern in patterns[1:]:
            count_command += "| grep '%s'" % pattern
        return getoutput(self.conn.replace('command', count_command))

    @output_decorator
    def get_pid_using_jps(self, pattern):
        command = " jps | grep %s | gawk -F' ' '{{print \\$1}}' ".format(node, pattern)
        return getoutput(self.conn.replace('command', command)).split(" ")[0]

    @output_decorator
    def get_pid_using_jpsv(self, pattern):
        command = " jps -v | grep -iw %s | gawk -F' ' '{{print \\$1}}' ".format(node, pattern)
        return getoutput(self.conn.replace('command', command)).split(" ")[0]

    @output_decorator
    def get_pid_using_systemctl(self, *patterns):
        command = "systemctl status {0} | grep '{1}' | gawk -F' ' '{{print \\$3}}'".format(*patterns)
        return getoutput(self.conn.replace('command', command)).split(" ")[3]

    @output_decorator
    def get_pid_using_pseaf(self, *patterns):
        command = "ps -eaf | grep {0} | grep ^{1} | gawk -F' ' '{{print \\$2}}'".format(*patterns)
        return list(filter(None, getoutput(self.conn.replace('command', command)).split(' ')))[1]

    @output_decorator
    def get_pid_using_ssnlp(self, pattern):
        command = "ss -nlp | grep %s | tr -s ' ' | gawk -F',' '{{print \\$2}}' | gawk -F'=' '{{print $2}}'" % pattern
        return getoutput(self.conn.replace('command', command))

    @output_decorator
    def get_pid_using_cat(self, *patterns):
        command = 'cat {0}/*{1}.pid | head -1'.format(*patterns)
        return getoutput(self.conn.replace('command', command))

    @output_decorator
    def get_jstat_memory(self, pid, memory={}):
        command = "jstat -gc %s | gawk -F' ' '{{print}}'" % pid
        output = getoutput(self.conn.replace('command', command)).split('\n')
        for key, value in zip(list(filter(None, output[0].split(' '))), list(filter(None, output[1].split(' ')))):
            memory[key] = float(value)
        total_memory = (memory['OU'] + memory['EU'] + memory['S0U'] + memory['S1U']) / 1000
        return str(round(total_memory, 2))


class OutputUtility:

    def __init__(self, output):
        self.output = output

    def return_as_str(self):
        return self.output

    def return_as_int(self):
        return int(self.output.strip())

    def return_as_float(self):
        return float(self.output.strip())

    def return_as_str_list(self):
        return [i.strip() for i in list(filter(None, self.output.split('\n')))]

    def return_as_int_list(self):
        return [int(i.strip()) for i in list(filter(None, self.output.split('\n')))]

    def return_as_str_dict(self):
        output_dict = {}
        for line in self.output.split('\n')[2:-2]:
            key, value = list(filter(None, line.split('|')))
            output_dict[key.strip()] = value.strip()
        return output_dict
