import itertools
from typing import Dict, Iterable, List, Mapping, Optional, Set, Union

import packaging.requirements
import packaging.version

default_redgrease = packaging.requirements.Requirement("redgrease[runtime]")

# Re-exposing for convenience
Version = packaging.version.Version
Requirement = packaging.requirements.Requirement
InvalidRequirement = packaging.requirements.InvalidRequirement


PackageOption = Optional[Union[bool, str, Version, Requirement]]


def read_requirements(
    *requirements_file_paths: str,
) -> Set[Requirement]:
    """Extract list of individual requirements from a 'requirements.txt' file

    Args:
        requirements_file_paths (Iterable[str]): Requirements file path

    Returns:
        [set[str]]: Individual requirements.
    """
    requirements = set.union(
        *[
            set(
                map(
                    lambda req_line: Requirement(req_line.split("#")[0]),
                    filter(
                        lambda line: len(line) > 0 and not line.startswith("#"),
                        map(str.strip, open(file_path).readlines()),
                    ),
                )
            )
            for file_path in requirements_file_paths
        ]
    )

    return resolve_requirements(requirements)


def to_packages_dict(
    requirements: Iterable[Requirement],
) -> Dict[str, List[Requirement]]:
    return {
        str(name): list(pkgs)
        for name, pkgs in itertools.groupby(requirements, lambda r: r.name)
    }


def from_packages_dict(
    packages: Mapping[str, Iterable[Requirement]]
) -> Set[Requirement]:
    return set([req for _, req_list in packages.items() for req in req_list])


def resolve_requirements(
    requirements: Iterable[Union[str, Requirement]],
    enforce_redgrease: PackageOption = False,
) -> Set[Requirement]:
    """
    resolve_requirements attempts to resolve the given requirements,
    and that the desired version of redgrease is, or is NOT, included.

    :param requirements: Any iterable of requirements,
        encoded as either strings ('numpy', 'passlib[bcrypt]==1.7.4')
        or a packaging.requirements.Requiremnt object.
    :type requirements: Iterable[Union[str, Requirement]]

    :param enforce_redgrease: Indicates if, and potentially which version of,
        redgrease should be enforced as a requirement.
        It can take several optional types:
        - None : enforces that 'redgrease' is NOT in thequirements,
            any redgrease requirements will be removed.
        - bool :
            True, enforces latest 'redgrease' package on PyPi,
            False, no enforcing. requirements are passed as-is,
                with or without 'redgrease'
        - str :
            a. Specific version. E.g. "1.2.3".
            b. Version qualifier. E.g. ">=1.0.0"
            c. Extras. E.g. "all" or "runtime".
                Will enforce the latest version, with this/these extras
            d. Custom source. E.g:
                "redgrease[all]>=1.2.3"
                "redgrease[runtime]@git+https://github.com/lyngon/redgrease.git@branch_xyz"
        - Version : behaves just as string version (a.)
        - Requirment : behaves just as string version (d.)
        defaults to False
    :type enforce_redgrease: Union[bool, str, Version, Requirement], optional

    :raises packaging.requirements.InvalidRequirement:
        If any of the requirements are not well-formed

    :return: Set of requirements
    :rtype: Set[Requirement]
    """

    requirements_set = set(
        [packaging.requirements.Requirement(str(req)) for req in requirements]
        if requirements
        else []
    )

    packages = to_packages_dict(requirements_set)

    if enforce_redgrease is None:
        # enforce_redgrease is explicitly None, meaning we enforce that
        # redgrease should NOT be in ther requirements set
        # so we remove it
        if "redgrease" in packages:
            del packages["redgrease"]
        return from_packages_dict(packages)

    if isinstance(enforce_redgrease, bool):
        if enforce_redgrease and "redgrease" not in packages:
            # enforce_redgrease is True  (i.e: any version of redgrease)
            # and it is not currently in the requirement set
            # so we add the latest version from PyPi,
            packages["redgrease"] = [default_redgrease]
        return from_packages_dict(packages)

    if isinstance(enforce_redgrease, Requirement):
        # enforce_redgrease is a complete requirement
        # just enforce it and exit
        packages["redgrease"] = [enforce_redgrease]
        return from_packages_dict(packages)

    enforce_redgrease = str(enforce_redgrease)

    if enforce_redgrease[0] in ["=", ">", "<", "!"]:
        # enforce_redgrease is a qualifier / constraint string, e.g:
        # ">=1.0.0"
        redgrease_package = f"redgrease[runtime]{enforce_redgrease}"
    elif enforce_redgrease[0].isnumeric():
        # enforce_redgrease is a specific version string, e.g:
        # "1.2.3"
        redgrease_package = f"redgrease[runtime]=={enforce_redgrease}"
    elif "redgrease" not in enforce_redgrease:
        # enforce_redgrease does not contain 'redgrease'
        # so it is assumed to specify the 'extras'
        redgrease_package = "redgrease[{enforce_redgrease}]"
    else:
        # enforce_redgrease is some completely custom string, e.g:
        # "redgrease[runtime]@git+https://github.com/lyngon/redgrease.git@feature_xyz"
        redgrease_package = enforce_redgrease

    # Mostly to confirm that it is well formed
    redgrease_requirement = packaging.requirements.Requirement(redgrease_package)

    packages["redgrease"] = [redgrease_requirement]

    return from_packages_dict(packages)
