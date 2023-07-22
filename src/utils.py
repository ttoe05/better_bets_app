import logging
import sys
from pathlib import Path


def init_logger(name: str) -> None:
    """
    Initialize logging, creating necessary folder and file. If it doesn't already exist
    create it

    Parameters
    _____________________

        name: str
    The name of the log file i.e get_data
    """
    # Assume script is called from top-level directory
    log_dir = Path("logs")
    if not log_dir.exists():
        log_dir.mkdir(parents=True)

    # Configue handlers to print logs to file and std out
    file_handler = logging.FileHandler(filename=f"logs/{name}.log")
    stdout_handler = logging.StreamHandler(sys.stdout)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[file_handler, stdout_handler],
    )