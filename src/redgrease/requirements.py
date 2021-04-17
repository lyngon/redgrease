# -*- coding: utf-8 -*-
"""
Module for handling of requirements.
"""
__author__ = "Anders Åström"
__contact__ = "anders@lyngon.com"
__copyright__ = "2021, Lyngon Pte. Ltd."
__licence__ = """The MIT License
Copyright © 2021 Lyngon Pte. Ltd.

Permission is hereby granted, free of charge, to any person obtaining a copy of this
 software and associated documentation files (the “Software”), to deal in the Software
 without restriction, including without limitation the rights to use, copy, modify,
 merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 permit persons to whom the Software is furnished to do so, subject to the following
 conditions:

The above copyright notice and this permission notice shall be included in all copies
 or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
 CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE
 OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""


import itertools
from typing import Any, Dict, Iterable, List, Mapping, Optional, Set, Union

import packaging.requirements
import packaging.version

from redgrease.utils import safe_str

default_redgrease = packaging.requirements.Requirement("redgrease[runtime]")
"""The default redgrease runtime package. Latest from PyPi."""

# Re-exposing for convenience
Version = packaging.version.Version
"""Same as 'packaging.version.Version', for convenience."""

Requirement = packaging.requirements.Requirement
"""Same as 'packaging.requirements.Requirement', for convenience."""

InvalidRequirement = packaging.requirements.InvalidRequirement
"""Same as 'packaging.requirements.InvalidRequirement', for convenience."""

PackageOption = Optional[Union[bool, str, Version, Requirement]]
"""Type alias for valid types of RedGrease package version specification."""


def safe_requirement(req: Any) -> Requirement:
    """Parse a Requirement safely

    Args:
        req (Any):
            Value to parse

    Returns:
        Requirement:
            Parsed Requirement object

    Raises:
        packaging.requirements.InvalidRequirement
            If input could not be parsed.
    """
    if not isinstance(req, Requirement):
        req = Requirement(safe_str(req))
    return req


def same_name(req1: Any, req2: Any) -> bool:
    """Check if two requirements have the same name.

    Version numbers, extras etc is ignored.

    This means that `foo>1.2.3` will match `foo[bar]==0.1.2`,
    because both are from the same `foo` package.


    Args:
        req1 (Any):
            First requirement to parse.

        req2 (Any):
            Second requirement to parse.

    Returns:
        bool:
            `True` if the two requirements share the same name.
            `False` if they don't share name, or if there was a parse error.
    """
    try:
        return safe_requirement(req1).name == safe_requirement(req2).name
    except packaging.requirements.InvalidRequirement:
        return False


def read_requirements(
    *requirements_file_paths: str,
) -> Set[Requirement]:
    """Extract list of individual requirements from a 'requirements.txt' file

    Args:
        requirements_file_paths (Iterable[str]):
            Requirements file path

    Returns:
        Set[str]:
            The individual requirements in the file.
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
    """Transforms a collection of requirements to a dict mapping packge names to
    a list of requirements.

    I.e. grouping the different versions of packages in the requiremnts by name.

    E.g.
        ```
        reqs = map(Requirement, ["pack_x[xtra]", "pack_y>=1.0", "pack_x!=2.1"])
        to_packages_dict(reqs)
        ```
        results in:
        ```
        {
            "pack_x": [<Requirement("pack_x[xtra]")>, <Requirement("pack_x!=2.1")>],
            "pack_y": [<Requirement("pack_y>=1.0")>]
        }
        ```

    Args:
        requirements (Iterable[Requirement]):
            A collection of requirements.

    Returns:
        Dict[str, List[Requirement]]:
            A dict that maps packge names to a list of specific requirements.
    """
    return {
        str(name): list(pkgs)
        for name, pkgs in itertools.groupby(requirements, lambda r: r.name)
    }


def from_packages_dict(
    packages: Mapping[Any, Iterable[Requirement]]
) -> Set[Requirement]:
    """Flattens any dict, where the values are collections of Requirements into a
    set of thoes requirements.

    Args:
        packages (Mapping[str, Iterable[Requirement]]):
            Any dict where the values are collections of requirements.
            For example the results from `to_package_dict`.

    Returns:
        Set[Requirement]:
            The set of unique requirments.
            Note that if the dictionary contains multiple versions
            (i.e different version constraints and/or extras) of the same requirement,
            the resulting set will contain them all too, as they are thechically not
            identical.
    """
    return set([req for _, req_list in packages.items() for req in req_list])


def resolve_requirements(
    requirements: Iterable[Union[str, Requirement]],
    enforce_redgrease: PackageOption = None,
) -> Set[Requirement]:
    """Attempts to resolve the given requirements, and that the desired version of
    redgrease is, or is NOT, included.

    Args:
        requirements (Iterable[Union[str, Requirement]]):
            Any iterable of requirements, encoded as either strings ('numpy',
            'passlib[bcrypt]==1.7.4') or a packaging.requirements.Requiremnt object.

        enforce_redgrease (PackageOption, optional):
            Indicates if, and potentially which version of, redgrease should be
            enforced as a requirement.

            It can take several optional types::

                - None : no enforcing. requirements are only deduplicated, with or
                    without 'redgrease'

                - bool :
                    True, enforces latest 'redgrease' package on PyPi,
                    False, enforces that 'redgrease' is NOT in the quirements set,
                        any redgrease requirements will be removed.

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

            Defaults to None.

    Returns:
        Set[Requirement]:
            Set of resulting Requirements.

    Raises:
        InvalidRequirement:
            If any of the requirements are not well-formed.
    """
    # Parse requirments and, remove duplicates
    requirements_set = (
        {packaging.requirements.Requirement(str(req)) for req in set(requirements)}
        if requirements
        else set()
    )

    if enforce_redgrease is None:
        # If there is no requirement to keep nor remove redgrease,
        # then the requirments are returned, as-is
        return requirements_set

    packages = to_packages_dict(requirements_set)

    if isinstance(enforce_redgrease, bool):
        if not enforce_redgrease:
            # enforce_redgrease is explicitly False, meaning we enforce that
            # redgrease should NOT be in ther requirements set
            # so we remove it
            packages.pop("redgrease", None)
            return from_packages_dict(packages)

        if "redgrease" not in packages:
            # enforce_redgrease is True  (i.e: any version of redgrease)
            # but it is not currently specified in the requirement set
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
