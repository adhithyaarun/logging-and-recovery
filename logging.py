import os
import sys
import time

from collections import OrderedDict 

class Logger:
    def __init__(self, filename=None, round_size=1):
        self.transactions = OrderedDict()  # Transaction Attributes: Size, Commands
        self.disk = {}                     # Database variables in disk
        self.memory = {}                   # Main memory
        self.registers = {}                # Registers

        self.filename = filename           # Filename
        self.round_size = int(round_size)  # Round Robin size

        self.logs = []                     # Logs to log file

        # Constants
        self.DISK_OPERATIONS = ['READ', 'WRITE', 'OUTPUT']
        self.MATH_OPERATIONS = ['+', '-', '*', '/']

    def data_status(self):
        lexi_memory = list(self.memory.keys())
        lexi_disk = list(self.disk.keys())
        log_entry = ''
        
        # Print the status of memory
        if len(lexi_memory) > 0:
            lexi_memory.sort()
            for attr in lexi_memory:
                log_entry += '{0} {1} '.format(attr, self.memory[attr])
                if attr == lexi_memory[-1]:
                    log_entry = log_entry[:-1]
        else:
            log_entry = ''

        self.logs.append(log_entry)
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

        self.logs.append(log_entry)

    def initilize_database(self, disk_init=None):
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
        '''Get transactions and initialize database'''
        # Flags
        is_disk_initialized = False
        is_new_transaction = True

        # Counters
        transaction_count = 0
        current_transaction = None

        with open(self.filename, 'r') as file:
            for line in file:
                line = line.strip()
                
                if is_disk_initialized is False:
                    self.initilize_database(disk_init=line)
                    is_disk_initialized = True
                else:
                    if line == '':
                        transaction_count += 1
                        is_new_transaction = True
                    else:
                        if is_new_transaction is True:
                            # Extract transaction information
                            transaction_info = line.split()
                            key = transaction_info[0].strip()
                            value = int(transaction_info[1].strip())
                            
                            if key not in self.transactions.keys():
                                self.transactions[key] = {
                                    'size': value,
                                    'commands': []
                                }

                                current_transaction = key
                            else:
                                print('[Error] Transaction name is not unique.')
                                exit(1)
                    
                            # Reset variables
                            is_new_transaction = False  
                            transaction_count = 0

                        else:
                            self.transactions[current_transaction]['commands'].append(line)
                            transaction_count += 1
                            if transaction_count == self.transactions[current_transaction]['size']:
                                is_new_transaction = True

    def get_operation_type(self, command):
        operation_type = None
        operation_found = None
        for operation in self.DISK_OPERATIONS:
            if operation in command:
                operation_type = 'DISK'
                operation_found = operation
                break
        
        if operation_type is None:
            for operation in self.MATH_OPERATIONS:
                if operation in command:
                    operation_type = 'MATH'
                    operation_found = operation
                    break
        
        return operation_type, operation_found

    def execute_disk(self, command, operation, transaction):
        extraction = command.split('(')[1]
        extraction = extraction.split(')')[0]
        operands = extraction.strip()

        if ',' in operands:
            operands = operands.split(',')
        else:
            operands = list(operands)

        # Check if attribute in memory, if not, load
        if operands[0] not in self.memory.keys():
            self.memory[operands[0]] = self.disk[operands[0]]
        
        if operation == 'READ':
            # Read into register
            self.registers[operands[1]] = self.memory[operands[0]]
        elif operation == 'WRITE':
            self.logs.append('<{0}, {1}, {2}>'.format(transaction, operands[0], self.memory[operands[0]]))
            # Write into memory (from register)
            self.memory[operands[0]] = self.registers[operands[1]]
            self.data_status()
        elif operation == 'OUTPUT':
            self.disk[operands[0]] = self.memory[operands[0]]

    def execute_math(self, command, operation, transaction):
        equation = command.split(':=')
        lhs = equation[0].strip()
        rhs = equation[1].strip()
        operands = rhs.split(operation)
        operands = [operands[i].strip() for i in range(len(operands))]

        if operation == '+':
            self.registers[lhs] = self.registers[operands[0]] + int(operands[1])
        elif operation == '-':
            self.registers[lhs] = self.registers[operands[0]] - int(operands[1])
        elif operation == '*':
            self.registers[lhs] = self.registers[operands[0]] * int(operands[1])
        elif operation == '/':
            if int(operands[1]) == 0:
                print('Division by zero.')
                exit(1)
            else:
                self.registers[lhs] = self.registers[operands[0]] / int(operands[1])

    def run_command(self, transaction_name, start, end):
        commands = self.transactions[transaction_name]['commands'][start:end]
        
        for command in commands:
            operation_type, operation_found = self.get_operation_type(command)
            if operation_type == 'DISK':
                self.execute_disk(command, operation_found, transaction_name)
            elif operation_type == 'MATH':
                self.execute_math(command, operation_found, transaction_name)

    def process(self):
        '''Process the transaction commands'''
        transactions_complete = 0
        operations_complete = 0
        while transactions_complete < len(self.transactions.keys()):
            for transaction in self.transactions.keys():
                size = self.transactions[transaction]['size']
                
                # Number of Commands < Operations Completed => Transaction Complete
                if size <= operations_complete:
                    transactions_complete += 1
                    continue
                
                # New transaction, then log <START>
                if operations_complete == 0:
                    self.logs.append('<START {0}>'.format(transaction))
                    self.data_status()  # Log memory and disk status
                
                # Run commands for transaction
                start = operations_complete
                end = min(operations_complete + self.round_size, size)
                self.run_command(transaction_name=transaction, start=start, end=end)

                # Transaction complete, then log <COMMIT>
                if end == size:
                    self.logs.append('<COMMIT {0}>'.format(transaction))
                    self.data_status()  # Log memory and disk status
            # Update operations complete
            operations_complete += self.round_size

    def write_log(self):
        with open('20171066_1.txt', 'w+') as file:
            log = '\n'.join(self.logs)
            file.write(log)

    def log(self):
        self.initialize()
        self.process()
        self.logs.append('')
        self.write_log()
        


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('USAGE: python3.7 20171066_1.py <input_filename> <x_value>')
        exit(1)
    filename = sys.argv[1]
    x = sys.argv[2]

    if not os.path.isfile(filename):
        print('Input file not found')
        exit(1)

    logger = Logger(filename=filename, round_size=x)
    logger.log()