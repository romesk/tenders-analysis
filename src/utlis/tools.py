import uuid


def generate_run_id() -> str:
    """
    Generate a unique ID for the run
    """
    return str(uuid.uuid4())
