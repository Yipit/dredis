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
        array_length = int(consume(instructions)[1:])  # skip '*' char
        instructions = advance(instructions)
        for _ in range(array_length):
            str_len = int(consume(instructions)[1:])  # skip '$' char
            instructions = advance(instructions)

            instruction = read(instructions, str_len)
            result.append(instruction)

            instructions = instructions[str_len:]
            instructions = advance(instructions)
        result.extend(parse_instructions(instructions))
    return result


def consume(instructions):
    return instructions[:instructions.index('\r\n')]


def advance(instructions):
    return instructions[instructions.index('\r\n') + 2:]


def read(instructions, n_chars):
    return instructions[:n_chars]
