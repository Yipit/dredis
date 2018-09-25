class Parser(object):

    MAX_BUFSIZE = 1024 * 1024
    CRLF = '\r\n'

    def __init__(self, read_fn):
        self._buffer = ""
        self._read_fn = read_fn

    def _readline(self):
        if '\n' not in self._buffer:
            self._read_into_buffer()
        crlf_position = self._buffer.find(self.CRLF)
        result = self._buffer[:crlf_position]
        self._buffer = self._buffer[crlf_position + len(self.CRLF):]
        return result

    def _read_into_buffer(self, min_bytes=0):
        data = self._read_fn(self.MAX_BUFSIZE)
        self._buffer += data
        while data and len(self._buffer) < min_bytes:
            data = self._read_fn(self.MAX_BUFSIZE)
            self._buffer += data

    def _read(self, n_bytes):
        if len(self._buffer) < n_bytes:
            self._read_into_buffer(min_bytes=n_bytes)
        result = self._buffer[:n_bytes]
        self._buffer = self._buffer[n_bytes + 2:]
        return result

    def get_instructions(self):
        if not self._buffer:
            self._read_into_buffer()
        while self._buffer:
            instructions = self._readline()
            if not instructions:
                raise StopIteration()

            # the Redis protocol says that all commands are arrays, however,
            # Redis's own tests have commands like PING being sent as a Simple String
            if instructions.startswith('+'):
                yield instructions[1:].strip().split()
            # if instructions.startswith('*'):
            elif instructions.startswith('*'):
                # array of instructions
                array_length = int(instructions[1:])  # skip '*' char
                instruction_set = []
                for _ in range(array_length):
                    str_len = int(self._readline()[1:])  # skip '$' char
                    instruction = self._read(str_len)
                    instruction_set.append(instruction)
                yield instruction_set
            else:
                # inline instructions, saw them in the Redis tests
                for line in instructions.split('\r\n'):
                    yield line.strip().split()
