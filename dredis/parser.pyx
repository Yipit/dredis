cdef class Parser(object):

    cdef bytearray _buffer
    cdef int _buffer_pos
    cdef object _read_fn
    cdef int MAX_BUFSIZE
    cdef str CRLF
    cdef int CRLF_LEN

    def __cinit__(self, read_fn):
        self._buffer = bytearray()
        self._buffer_pos = 0
        self._read_fn = read_fn
        self.MAX_BUFSIZE = 1024 * 1024
        self.CRLF = '\r\n'
        self.CRLF_LEN = 2

    cdef bytearray _readline(self):
        cdef bytearray result
        cdef bytearray buffer
        cdef int crlf_position

        buffer = self._buffer[self._buffer_pos:]
        crlf_position = buffer.find(self.CRLF)
        if crlf_position == -1:
            raise StopIteration()
        result = buffer[:crlf_position]
        self._buffer_pos += crlf_position + self.CRLF_LEN
        return result

    cdef void _read_into_buffer(self):
        cdef bytes data

        # FIXME: implement a maximum size for the buffer to prevent a crash due to bad clients
        data = self._read_fn(self.MAX_BUFSIZE)
        self._buffer.extend(data)

    cdef bytearray _read(self, int n_bytes):
        cdef bytearray result
        cdef bytearray buffer

        buffer = self._buffer[self._buffer_pos:]
        if len(buffer) < n_bytes:
            raise StopIteration()
        result = buffer[:n_bytes]
        # FIXME: ensure self.CRLF is next
        self._buffer_pos += n_bytes + self.CRLF_LEN
        return result

    def get_instructions(self):
        cdef bytearray instructions
        cdef int array_length
        cdef list instruction_set
        cdef bytearray line
        cdef str instruction

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
