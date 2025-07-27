import hashlib

def generate_hash_key(*args):
    """
    Returns a SHA-256 hash of concatenated stringified input values.
    """
    input_string = "||".join([str(arg).strip().lower() for arg in args])
    return hashlib.sha256(input_string.encode("utf-8")).hexdigest()
