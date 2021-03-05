#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Builds the package and publises it to PyPi
Increasess the pacth number by one, in the 'setup.cfg' file, unless otherwise stated,
 before building.

Args:
    --current-version: Only reads and returns the current version from 'setup.cfg'
    --read-version: Same as '--current-version'
    rc: Release as a release candidate version (eg 1.2.3.rc1)
    dev: Release as a development version (eg 1.2.3.dev1)
    test: Same as 'dev'
    major: Increase major version by one and set minor and patch to zero.
        e.g: 1.2.3 becomes 2.0.0
    minor: Increase minor version by one and set patch to zero.
        e.g: 1.2.3 becomes 1.3.0
    -y: Auto-confirm, i.e. Do not require manual confirmation of version

Todo:
    * Simplify, not modify the version in 'setup.cfg' but just use as input

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

import configparser
import subprocess
import sys

from packaging.version import Version
from setuptools import sandbox

config_file = "setup.cfg"
config = configparser.ConfigParser()
config.read(config_file)
current_version = Version(config.get("metadata", "version", fallback="0.0.0"))

if "--read-version" in sys.argv or "--current-version" in sys.argv:
    print(current_version)
    exit(0)


major = current_version.major
minor = current_version.minor
patch = current_version.micro

if "rc" in sys.argv:
    pre_release = "rc1" if not current_version.pre else f".rc{current_version.pre[1]+1}"
else:
    pre_release = ""

if "dev" in sys.argv or "test" in sys.argv:
    dev_release = (
        ".dev1"
        if not current_version.is_devrelease
        else f".dev{current_version.dev+1}"  # type: ignore
    )
    if current_version.pre and not pre_release:
        print(f"current_version: {current_version}")
        print(f"current_version.pre: {current_version.pre}")
        pre_release = f".{current_version.pre[0]}{current_version.pre[1]}"
else:
    dev_release = ""

if not current_version.is_prerelease:
    if "major" in sys.argv:
        major = major + 1
        minor = 0
        patch = 0
    elif "minor" in sys.argv:
        minor = minor + 1
        patch = 0
    else:
        patch = patch + 1


new_version = Version(f"{major}.{minor}.{patch}{pre_release}{dev_release}")

confirmed = "-y" in sys.argv
while not confirmed:
    print("")
    print(f"You are about to publish version : {new_version}")
    print(f"    (Current version : {current_version})")
    print("")
    proceed = input(f"Is version '{new_version}' correct? [yN] : ")
    confirmed = proceed.lower() in ["y", "yes"]

    if not confirmed:
        new_version = Version(input("Input desired version: "))

if not config.has_section("metadata"):
    config.add_section("metadata")

config.set("metadata", "version", str(new_version))

with open(config_file, "w") as cfg_file:
    config.write(cfg_file)

try:
    sandbox.run_setup("setup.py", ["sdist", "bdist_wheel"])

    res = subprocess.call(
        ["python3", "-m", "twine", "upload", f"dist/redgrease-{new_version}*"]
    )
except Exception:
    config.set("metadata", "version", str(current_version))

    with open(config_file, "w") as cfg_file:
        config.write(cfg_file)
    res = 1

sys.exit(res)
