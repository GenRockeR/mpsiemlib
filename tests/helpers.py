from random import choice
from string import ascii_uppercase, ascii_lowercase

def gen_uppercase_string(strlen: int) -> str:
    return ('unittest_' + ''.join(choice(ascii_uppercase) for i in range(strlen)))

def gen_lowercase_string(strlen: int) -> str:
    return ('unittest_' + ''.join(choice(ascii_lowercase) for i in range(strlen)))
