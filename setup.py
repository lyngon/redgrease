#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import pathlib
import sys

import setuptools

here = pathlib.Path(__file__).parent.resolve()


def text(*names, encoding="utf8"):
    return here.joinpath(*names).read_text(encoding=encoding)


runtime_extras = ["attrs", "redis", "cloudpickle"]
client_extras = ["attrs", "redis", "cloudpickle"]
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
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "Topic :: Database",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries :: Application Frameworks",
    ],
    install_requires=[],  # Should remain empty for minimal runtime install.
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

sys.exit(0)
