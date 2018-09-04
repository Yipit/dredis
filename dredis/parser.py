class Parser(object):

    MAX_BUFSIZE = 1024 * 1024
    CRLF = '\r\n'

    def __init__(self, read_fn):
        self._buffer = ""
        self._read_fn = read_fn

    def _readline(self):
        if '\r\n' not in self._buffer:
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
        result = []
        self._read_into_buffer()

        while self._buffer:
            instructions = self._readline()
            if not instructions:
                return result

            # the Redis protocol says that all commands are arrays, however,
            # Redis's own tests have commands like PING being sent as a Simple String
            if instructions.startswith('+'):
                result = [instructions[1:].strip()]
            else:
                # assume it's an array of instructions
                array_length = int(instructions[1:])  # skip '*' char
                for _ in range(array_length):
                    str_len = int(self._readline()[1:])  # skip '$' char
                    instruction = self._read(str_len)
                    result.append(instruction)
        return result

