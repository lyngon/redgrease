#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Setup of RedGrease package

Todo:
    * Use config as much as possible

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


import pathlib

import setuptools

here = pathlib.Path(__file__).parent.resolve()


def text(*names, encoding="utf8"):
    return here.joinpath(*names).read_text(encoding=encoding)


common_extras = [
    "attrs",
    "redis",
    "cloudpickle",
    "packaging",
    "wrapt",
]

runtime_extras = common_extras + []
client_extras = common_extras + [
    "typing-extensions",
    "redis-py-cluster",
]
cli_extras = client_extras + [
    "watchdog",
    "ConfigArgParse",
    "pyyaml",
]

all_extras = cli_extras

setuptools.setup(
    name="redgrease",
    license="MIT License",
    description="RedisGears helper package",
    long_description=text("README.md"),
    long_description_content_type="text/markdown",
    author="Anders Åström",
    author_email="anders@lyngon.com",
    url="https://github.com/lyngon/redgrease",
    packages=setuptools.find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[  # https://pypi.org/classifiers/
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Database",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries :: Application Frameworks",
    ],
    install_requires=[],  # Should remain empty to allow for minimal runtime install.
    extras_require={
        "all": all_extras,
        "client": client_extras,
        "cli": cli_extras,
        "runtime": runtime_extras,
    },
    entry_points={
        "console_scripts": ["redgrease=redgrease.cli:main [cli]"],
    },
    setup_requires=[],
    python_requires=">=3.6",
    keywords="Redis, Gears, development",
)
