from datetime import datetime


class listener_parser():
    def __init__(self, file_name=None,line_offset=0):
        self.file_list = []
        self.line_class = {}
        self.badlines = {}
        self.line_class['badlines'] = 0
        if file_name is not None: self.file_list.append(file_name)

        self.dictswitch = {
            '(CONNECT_DATA': self.connect_data,
            '<unknown conn': self.unknown_connect,
            'log_directory': self.log_directory,
            'ping': self.ping,
            'service_updat': self.service_update,
            'trc_directory': self.trc_directory,
            'version': self.version,
        }

    def addfile(self,file_name):
        self.file_list.append(file_name)

    def extract_values(self,component, keys):
        returndict = {}
        for value in keys:
            start = component.find(value)  # start substring after the '=' sign EG: PROGRAM=thing
            if start > 0:
                start = start + len(value) + 1
                end = component.find(')', start)
                returndict[value] = component[start: end]
        return returndict

    def extract_date(self,date_string):  # 08-DEC-2016 22:21:01
        return datetime.strptime(date_string, '%d-%b-%Y %H:%M:%S')

    def connect_data(self,line):
        l = {}
        if not line[-2] == 'version':
            connect_keys = (
            'PROGRAM', 'SERVER', 'SERVICE_NAME', 'HOST', 'USER', 'INSTANCE_NAME', 'FAILOVER_MODE', 'COMMAND',
            'ARGUMENTS', 'SERVICE', 'VERSION')
            l['call_type'] = 'listener_version'
            l['service'] = line[-2]
        else:
            connect_keys = (
            'PROGRAM', 'SERVER', 'SERVICE_NAME', 'HOST', 'USER', 'INSTANCE_NAME', 'FAILOVER_MODE', 'COMMAND',
            'ARGUMENTS', 'SERVICE', 'VERSION')

        protocol_keys = ('PROTOCOL', 'HOST', 'PORT')
        l['line_type'] = 'connect'
        l['timestamp'] = self.extract_date(line[0])
        l['connect_info'] = self.extract_values(line[1], connect_keys)
        l['protoinfo'] = self.extract_values(line[2], protocol_keys)
        l['return_code'] = line[-1]
        return l

    def unknown_connect(self,line):  # 08-DEC-2016 22:21:01 * <unknown connect data> * 12537
        l = {}
        l['line_type'] = 'unknown connect data'
        l['timestamp'] = self.extract_date(line[0])
        l['return_code'] = line[-1]
        return l

    def log_directory(self,line):  # 09-DEC-2016 07:48:04 * log_directory * 0
        l = {}
        l['line_type'] = 'log_directory'
        l['timestamp'] = self.extract_date(line[0])
        l['return_code'] = line[-1]
        return l

    def ping(self,line):  # 08-DEC-2016 22:25:07 * ping * 0
        l = {}
        l['line_type'] = 'ping'
        l['timestamp'] = self.extract_date(line[0])
        l['return_code'] = line[-1]
        return l

    def service_update(self,line):  # 09-DEC-2016 07:48:04 * service_update * porvok1 * 0
        l = {}
        l['line_type'] = 'service_update'
        l['timestamp'] = self.extract_date(line[0])
        l['service_name'] = line[-2]
        l['return_code'] = line[-1]
        return l

    def trc_directory(self,line):  # 09-DEC-2016 07:48:04 * trc_directory * 0
        l = {}
        l['line_type'] = 'trc_directory'
        l['timestamp'] = self.extract_date(line[0])
        l['return_code'] = line[-1]
        return l

    def version(self,line):  # 08-DEC-2016 22:25:07 * ping * 0
        l = {}
        l['line_type'] = 'ping'
        l['timestamp'] = self.extract_date(line[0])
        l['return_code'] = line[-1]
        return l

    def getrecords(self):
        for file_name in self.file_list:
            with open(file_name) as f:
                for line in f:
                    line = line.rstrip('\n')
                    try:
                        line_list = line.split(' * ')
                        linestart = line_list[1][:13]
                        # lines.append(dictswitch[linestart](line_list))
                        # print line
                        if linestart in self.line_class:
                            self.line_class[linestart] += 1
                        else:
                            self.line_class[linestart] = 1
                        yield self.dictswitch[linestart](line_list)
                    except:
                        self.line_class['badlines'] += 1
                        if line in self.badlines:
                            self.badlines[line] += 1
                        else:
                            self.badlines[line] = 1

