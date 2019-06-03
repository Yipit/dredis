class Parser(object):

    MAX_BUFSIZE = 1024 * 1024
    CRLF = '\r\n'

    def __init__(self, read_fn):
        self._buffer = bytearray()
        self._buffer_pos = 0
        self._read_fn = read_fn

    def _readline(self):
        if self.CRLF not in self._buffer[self._buffer_pos:]:
            raise StopIteration()
        crlf_position = self._buffer[self._buffer_pos:].find(self.CRLF)
        result = self._buffer[self._buffer_pos:][:crlf_position]
        self._buffer_pos += crlf_position + len(self.CRLF)
        return result

    def _read_into_buffer(self):
        # FIXME: implement a maximum size for the buffer to prevent a crash due to bad clients
        data = self._read_fn(self.MAX_BUFSIZE)
        self._buffer.extend(data)

    def _read(self, n_bytes):
        if len(self._buffer[self._buffer_pos:]) < n_bytes:
            raise StopIteration()
        result = self._buffer[self._buffer_pos:][:n_bytes]
        # FIXME: ensure self.CRLF is next
        self._buffer_pos += n_bytes + len(self.CRLF)
        return result

    def get_instructions(self):
        self._read_into_buffer()
        while self._buffer:
            self._buffer_pos = 0
            instructions = self._readline()
            # the Redis protocol says that all commands are arrays, however,
            # Redis's own tests have commands like PING being sent as a Simple String
            if instructions.startswith('+'):
                self._buffer = self._buffer[self._buffer_pos:]
                yield str(instructions[1:].strip()).split()
            elif instructions.startswith('*'):
                # array of instructions
                array_length = int(instructions[1:])  # skip '*' char
                instruction_set = []
                for _ in range(array_length):
                    line = self._readline()
                    str_len = int(line[1:])  # skip '$' char
                    instruction = str(self._read(str_len))
                    instruction_set.append(instruction)
                self._buffer = self._buffer[self._buffer_pos:]
                yield instruction_set
            else:
                # inline instructions, saw them in the Redis tests
                for line in instructions.split(self.CRLF):
                    self._buffer = self._buffer[self._buffer_pos:]
                    yield str(line.strip()).split()
