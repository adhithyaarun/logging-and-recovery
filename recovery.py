import os
import sys
import time

class Recovery:
    def __init__(self, filename=None):
        self.disk = {}           # Disk
        self.complete = {}       # Transaction status
        
        self.filename = filename # Filename
    
    def data_status(self):
        lexi_disk = list(self.disk.keys())
        log_entry = ''

        # Print the status of disk
        if len(lexi_disk) > 0:
            lexi_disk.sort()
            for attr in lexi_disk:
                log_entry += '{0} {1} '.format(attr, self.disk[attr])
                if attr == lexi_disk[-1]:
                    log_entry = log_entry[:-1]
        else:
            log_entry = ''

        return log_entry

    def write_log(self, log):
        with open('20171066_2.txt', 'w+') as file:
            file.write(log + '\n')

    def initialize_database(self, disk_init=None):
        '''Initialize database variables'''
        if disk_init is None:
            return
        else:
            init_info = disk_init.split()
            for i in range(0, len(init_info), 2):
                key = init_info[i]
                value = int(init_info[i+1])
                self.disk[key] = value

    def initialize(self):
        with open(self.filename, 'r') as file:
            disk_init = file.readline()
            self.initialize_database(disk_init=disk_init)
            
            lines = file.readlines()
            logs = []
            
            for line in lines:
                if len(line.strip()) > 0:
                    logs.append(line.strip())
            return logs

    def get_log_type(self, log):
        log_type = None
        if 'END CKPT' in log:
            log_type = 'END CHECKPOINT'
        elif 'START CKPT' in log:
            log_type = 'START CHECKPOINT'
        elif 'START' in log:
            log_type = 'START LOG'
        elif 'COMMIT' in log:
            log_type = 'COMMIT LOG'
        elif len(log.split(',')) > 2:
            log_type = 'CHANGE LOG'
        return log_type

    def check_transaction_status(self):
        for key, value in self.complete.items():
            if value is False:
                return False
        return True

    def process(self, logs):
        logs.reverse()
        start_found = False
        end_found = False

        for log in logs:
            log = log[1:-1] # Remove angular brackets
            log_type = self.get_log_type(log=log)

            if log_type == 'START CHECKPOINT':
                if end_found is True:
                    break
                start_found = True
                transactions = log.split()[-1].strip()
                transactions = transactions[1:-1].split()
                for transaction in transactions:
                    if transaction in self.complete.keys() and self.complete[transaction] is True:
                        continue
                    self.complete[transaction] = True
            elif log_type == 'START LOG':
                if start_found is True:
                    start_log = log.split()
                    self.complete[start_log[1]] = True
            elif log_type == 'CHANGE LOG':
                change_log = log.split(',')
                change_log = [change_log[i].strip() for i in range(len(change_log))]
                if change_log[0] in self.complete.keys() and self.complete[change_log[0]] is True:
                    continue
                self.disk[change_log[1]] = int(change_log[2])
            elif log_type == 'COMMIT LOG':
                commit_log = log.split()
                self.complete[commit_log[1].strip()] = True
            elif log_type == 'END CHECKPOINT':
                end_found = True
      
    def recover(self):
        logs = self.initialize()
        self.process(logs=logs)
        log = self.data_status()
        self.write_log(log)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('USAGE: python3.7 20171066_2.py <input_filename>')
        exit(1)
    filename = sys.argv[1]

    if not os.path.isfile(filename):
        print('Input file not found')
        exit(1)

    recovery = Recovery(filename=filename)
    recovery.recover()