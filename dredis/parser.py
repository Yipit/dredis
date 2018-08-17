def parse_instructions(instructions):
    result = []
    if not instructions:
        return result

    # the Redis protocol says that all commands are arrays, however,
    # the code tests have commands like PING being sent as a Simple String
    if instructions.startswith('+'):
        result = [instructions[1:].strip()]
    else:
        # assume it's an array of instructions
        i = 0
        j = instructions[i:].index('\r\n')
        i += 1  # skip '*' char
        array_length = int(instructions[i:j])
        i = j + 2  # skip '\r\n'
        for _ in range(array_length):
            j = i + instructions[i:].index('\r\n')
            i += 1  # skip '$' char
            str_len = int(instructions[i:j])
            i = j + 2
            j = i + str_len
            s = instructions[i:j]
            result.append(s)
            i = j + 2  # skip '\r\n'
        result.extend(parse_instructions(instructions[i:]))
    return result
