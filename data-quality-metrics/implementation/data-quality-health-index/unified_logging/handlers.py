'''
Created on 16-Sep-2021

@author: rithomas
'''
import os
import socket
import sys
import time
import logging.handlers
import unified_logging

class CustomSysLogHandler(logging.handlers.SysLogHandler):
    """
    Custom logging.handlers.SysLogHandler
    """
    append_char = '\n'
    def emit(self, record):
        r"""
        The official SysLogHandler will append a '\000' to the each msg which caused Rsyslog cannot distinguish
        single message from the TCP channel. This function will replace '\000' with '\n'.
        In addtion,this function will populate a RFC 3164 format log message which can be handled by Rsyslog
        :param record: LogRecord object
        """
        try:
            msg = self.format(record)
            msg += self.append_char

            # We need to convert record level to lowercase, maybe this will
            # change in the future.
            prio = '<%d>' % self.encodePriority(self.facility, unified_logging.nameToSyslogLevel[record.levelname])
            # Message is a string. Convert to bytes as required by RFC 5424
            if sys.version_info[0] >= 3:
                msg = msg.encode('utf-8')
            else:
                if type(msg) is unicode:
                    msg = msg.encode('utf-8')
            if self.unixsocket:
                try:
                    header = prio.encode('utf-8')
                    msg = header + msg
                    self.socket.send(msg)
                except OSError:
                    self.socket.close()
                    self._connect_unixsocket(self.address)
                    self.socket.send(msg)
            else:
                # get the host name
                hostname = socket.gethostname()
                # datetime
                date = time.strftime('%b %d %H:%M:%S', time.localtime())
                # process name and id
                program = os.path.basename(sys.argv[0])
                pid = os.getpid()
                # msg header
                header = '%s%s %s %s[%d]: ' % (prio, date, hostname, program, pid)
                header = header.encode('utf-8')
                # msg
                msg = header + msg
                if self.socktype == socket.SOCK_DGRAM:
                    self.socket.sendto(msg, self.address)
                else:
                    self.socket.sendall(msg)
        except Exception:
            self.handleError(record)
