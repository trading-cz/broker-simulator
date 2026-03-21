#!/usr/bin/env python3
"""Utility to reverse a reversed secret string back to original."""

def reverse_secret(reversed_string: str) -> str:
    """Reverse the string back to original."""
    return reversed_string[::-1]

if __name__ == "__main__":
    print("Paste the reversed secret string below and press Enter twice:")
    print("(or run with: python decrypt_reversed.py 'your_reversed_string')")
    print()
    
    import sys
    if len(sys.argv) > 1:
        # If passed as argument
        reversed_val = sys.argv[1]
    else:
        # Interactive input
        reversed_val = input("Reversed value: ").strip()
    
    original = reverse_secret(reversed_val)
    print()
    print(f"Original secret: {original}")
    print()
    print("✓ Copy this value to verify it matches your actual key")
