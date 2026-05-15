"""Secure temporary password generation."""

import string
import secrets
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def generate_temp_password(length: int = 12, include_symbols: bool = False) -> str:
    """
    Generate a random password with upper, lower, and digit characters.

    Symbols are optional because temporary credentials may be distributed through
    email or copied manually by administrators.
    """
    if length < 8:
        logger.warning(
            "Security Notice: Generating a password with length %d. "
            "A minimum length of 12 is recommended for temporary credentials.", 
            length
        )

    letters = string.ascii_letters
    digits = string.digits
    symbols = "!@#$%&*_-"
    
    alphabet = letters + digits
    if include_symbols:
        alphabet += symbols

    while True:
        password = ''.join(secrets.choice(alphabet) for _ in range(length))

        has_lower = any(char.islower() for char in password)
        has_upper = any(char.isupper() for char in password)
        has_digit = any(char.isdigit() for char in password)
        
        basic_criteria_met = has_lower and has_upper and has_digit

        if include_symbols:
            has_symbol = any(char in symbols for char in password)
            if basic_criteria_met and has_symbol:
                return password
        elif basic_criteria_met:
            return password
