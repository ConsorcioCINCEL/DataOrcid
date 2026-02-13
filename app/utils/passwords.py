"""
Module: passwords.py
Author: Gastón Olivares
Project: DataOrcid-Chile (Open Source)
License: MIT
Description: 
    Secure Password Generation Utility.
    
    This module provides a robust mechanism for creating temporary credentials.
    It utilizes Python's `secrets` module (PEP 506) to ensure cryptographically 
    strong randomness, making it suitable for managing sensitive user access.
    
    Key Features:
    - Enforces complexity rules (Upper, Lower, Digit, Symbol).
    - Customizable length and character sets.
    - Secure random generation to prevent prediction attacks.
"""

import string
import secrets
import logging
from typing import Optional

# --- Logging Configuration ---
logger = logging.getLogger(__name__)


def generate_temp_password(length: int = 12, include_symbols: bool = False) -> str:
    """
    Generates a secure, random temporary password.

    This function ensures that the generated password meets standard complexity 
    requirements by verifying the presence of specific character classes:
    1. At least one Lowercase letter.
    2. At least one Uppercase letter.
    3. At least one Digit.
    4. (Optional) At least one Special Symbol.

    If the randomly generated string does not meet these criteria, it is discarded 
    and re-generated until a compliant one is found.

    Args:
        length (int): The total length of the password. Defaults to 12.
                      A minimum of 12 is recommended for temporary credentials.
        include_symbols (bool): If True, includes special characters (!@#$%&*_-) 
                                in the allowable pool. Defaults to False.

    Returns:
        str: A cryptographically secure password string.
    """
    # Security Warning for short passwords
    if length < 8:
        logger.warning(
            "Security Notice: Generating a password with length %d. "
            "A minimum length of 12 is recommended for temporary credentials.", 
            length
        )

    # Define the character pools
    letters = string.ascii_letters
    digits = string.digits
    symbols = "!@#$%&*_-"
    
    # Construct the base alphabet
    alphabet = letters + digits
    if include_symbols:
        alphabet += symbols

    while True:
        # Generate a candidate password using cryptographically secure random choice
        password = ''.join(secrets.choice(alphabet) for _ in range(length))

        # Validate Complexity Requirements
        has_lower = any(char.islower() for char in password)
        has_upper = any(char.isupper() for char in password)
        has_digit = any(char.isdigit() for char in password)
        
        basic_criteria_met = has_lower and has_upper and has_digit

        # Check against requested configuration
        if include_symbols:
            has_symbol = any(char in symbols for char in password)
            if basic_criteria_met and has_symbol:
                return password
        elif basic_criteria_met:
            return password
        
        # If criteria are not met, the loop continues to generate a new candidate.