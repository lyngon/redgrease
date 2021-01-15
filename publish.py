#!/usr/bin/env python3

import sys
from packaging.version import Version
from setuptools import sandbox
import subprocess
import configparser


config_file = "setup.cfg"
config = configparser.ConfigParser()
config.read(config_file)
current_version = Version(config.get("metadata", "version", fallback="0.0.0"))

major = current_version.major
minor = current_version.minor
patch = current_version.micro

if "rc" in sys.argv:
    pre_release = (
        "rc" if not current_version.is_prerelease else f"rc{current_version.pre[1]+1}"
    )
else:
    pre_release = ""

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


new_version = Version(f"{major}.{minor}.{patch}{pre_release}")

print("")
print(f"Current version : {current_version}")
while True:
    print("")
    proceed = input(f"Is version {new_version} correct? [yN] : ")
    if proceed.lower() in ["y", "yes"]:
        break
    new_version = Version(input("Input desired version: "))

if not config.has_section("metadata"):
    config.add_section("metadata")

config.set("metadata", "version", str(new_version))

with open(config_file, "w") as cfg_file:
    config.write(cfg_file)


sandbox.run_setup("setup.py", ["sdist", "bdist_wheel"])

res = subprocess.call(
    ["python3", "-m", "twine", "upload", f"dist/redgrease-{new_version}*"]
)

sys.exit(res)
