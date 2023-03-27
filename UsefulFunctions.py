import sys

STRICT_ERROR_PRINT = True


def error_print(msg:str, strict:bool = STRICT_ERROR_PRINT):
    print(f"(!) ERROR: {msg}")
    if strict:
        sys.exit(1)

def is_pow_of_2(n):
    return type(n) == int and n > 0 and n&(n-1) == 0    # Clever Bitwise approach: O(1)
