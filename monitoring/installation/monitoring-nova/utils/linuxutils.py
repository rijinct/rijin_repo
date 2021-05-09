from subprocess import getoutput


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
    def get_count_of_files(self, path):
        command = ' ls %s | wc -l' % path
        return getoutput(self.conn.replace('command', command))


    @output_decorator
    def get_size_of_dir(self, path):
        command = ''' du -ks %s | awk -F' ' '{print $1}' ''' % path
        return getoutput(self.conn.replace('command', command))


class OutputUtility:

    def __init__(self, output):
        self.output = output

    def return_as_str(self):
        return self.output
