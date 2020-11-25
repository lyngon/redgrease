
def read(*requirements_file_paths: str):
    """Extract list of individual requirements from a 'requirements.txt' file

    Args:
        requirements_file_paths (Iterable[str]): Requirements file path

    Returns:
        [set[str]]: Individual requirements.
    """
    # TODO: Error "descriptor 'union' for 'set' objects
    # doesn't apply to a 'filter' object"
    return set.union(
        *[
            filter(
                lambda line: not line.startswith('#'),
                map(
                    str.strip,
                    open(file_path).readlines()
                )
            )
            for file_path in requirements_file_paths
        ]
    )
