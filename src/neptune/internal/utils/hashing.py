__all__ = ["generate_hash"]

import hashlib


def generate_hash(*descriptors, max_length: int = 8):
    combined = "_".join(map(str, descriptors))
    return hashlib.sha256(combined.encode(), usedforsecurity=False).hexdigest()[-max_length:]
