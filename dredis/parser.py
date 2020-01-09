class Parser(object):

    MAX_BUFSIZE = 1024 * 1024
    CRLF = '\r\n'

    def __init__(self, read_fn):
        self._buffer = bytearray()
        self._buffer_pos = 0
        self._array_length = -1
        self._instruction_set = []
        self._request_type = ''
        self._str_len = -1
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
        if len(self._buffer[self._buffer_pos:]) < n_bytes + len(self.CRLF):
            raise StopIteration()
        result = self._buffer[self._buffer_pos:][:n_bytes]
        # FIXME: ensure self.CRLF is next
        self._buffer_pos += n_bytes + len(self.CRLF)
        return result

    def get_instructions(self):
        self._read_into_buffer()
        while self._buffer:
            if not self._request_type:
                self._request_type = chr(self._buffer[0])

            if self._request_type == '*':
                # inspired by `processMultibulkBuffer()` from Redis: https://git.io/Jvv3N
                if self._array_length == -1:
                    instructions = self._readline()
                    self._array_length = int(instructions[1:])  # skip '*' char
                    self._trim_buffer()

                while self._array_length > 0:
                    if self._str_len == -1:
                        line = self._readline()
                        self._str_len = int(line[1:])  # skip '$' char
                    instruction = str(self._read(self._str_len))
                    self._instruction_set.append(instruction)
                    self._trim_buffer()
                    self._array_length -= 1
                    self._str_len = -1
                yield self._instruction_set
                self.reset()
            else:
                instructions = self._readline()
                self._trim_buffer()
                yield str(instructions[1:].strip()).split()
                self.reset()

    def _trim_buffer(self):
        self._buffer = self._buffer[self._buffer_pos:]
        self._buffer_pos = 0

    def reset(self):
        self._instruction_set = []
        self._request_type = ''
        self._array_length = -1
