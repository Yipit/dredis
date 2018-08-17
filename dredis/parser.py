def parse_instructions(instructions):
    result = []
    if not instructions:
        return result

    # the Redis protocol says that all commands are arrays, however,
    # Redis's own tests have commands like PING being sent as a Simple String
    if instructions.startswith('+'):
        result = [instructions[1:].strip()]
    else:
        # assume it's an array of instructions
        array_length = int(consume(instructions, skip=1))  # skip '*' char
        instructions = advance(instructions)
        for _ in range(array_length):
            str_len = int(consume(instructions, skip=1))  # skip '$' char
            instructions = advance(instructions)

            instruction = read(instructions, str_len)
            result.append(instruction)

            instructions = advance(instructions, skip=str_len)
        result.extend(parse_instructions(instructions))
    return result


def consume(instructions, skip=0):
    return instructions[skip:instructions.index('\r\n')]


def advance(instructions, skip=0):
    return instructions[instructions[skip:].index('\r\n') + skip + 2:]


def read(instructions, n_chars):
    return instructions[:n_chars]
