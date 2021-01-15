import logging

log = logging.getLogger(__name__)


def read(*requirements_file_paths: str):
    """Extract list of individual requirements from a 'requirements.txt' file

    Args:
        requirements_file_paths (Iterable[str]): Requirements file path

    Returns:
        [set[str]]: Individual requirements.
    """
    return set.union(
        *[
            set(
                filter(
                    lambda line: len(line) > 0 and not line.startswith("#"),
                    map(str.strip, open(file_path).readlines()),
                )
            )
            for file_path in requirements_file_paths
        ]
    )
