#!/usr/bin/env python3

import sys
from os import path
from packaging import version
from setuptools import sandbox
import subprocess


ver_file_path = ".version"
current_version = None

if path.isfile(ver_file_path):
    with open(ver_file_path) as ver_file:
        for line in ver_file:
            current_version = version.parse(line)
            if isinstance(current_version, version.Version):
                break

if current_version is None:
    current_version = version.parse('0.0.0')

major = current_version.major
minor = current_version.minor
patch = current_version.micro

if 'rc' in sys.argv:
    pre_release = "rc" if not current_version.is_prerelease \
        else f"rc{current_version.pre[1]+1}"
else:
    pre_release = ''

if not current_version.is_prerelease:
    if 'major' in sys.argv:
        major = major+1
        minor = 0
        patch = 0
    elif 'minor' in sys.argv:
        minor = minor+1
        patch = 0
    else:
        patch = patch+1


new_version = version.parse(f"{major}.{minor}.{patch}{pre_release}")

print("")
print(f"Current version : {current_version}")
while True:
    print("")    
    proceed = input(f"Is version {new_version} correct? [yN] : ")
    if proceed.lower() in ['y', 'yes']:
        break
    new_version = input("Input desired version: ",)

open(ver_file_path, 'w').write(str(new_version))

sandbox.run_setup('setup.py', ['sdist', 'bdist_wheel'])

res = subprocess.call(
    [
        'python3',
        '-m',
        'twine',
        'upload',
        f'dist/redgrease-{new_version}*'
    ]
)

sys.exit(res)
