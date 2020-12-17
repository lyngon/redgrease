#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import setuptools
import pathlib
import sys

here = pathlib.Path(__file__).parent.resolve()


def text(*names, encoding='utf8'):
    return here.joinpath(*names).read_text(encoding=encoding)


try:
    version = open('.version').readline()
except FileNotFoundError:
    print("Unable to determine version.")
    print("Set the version on the first line of a '.version' file")
    sys.exit(404)

client_extras = ['redis', 'attrs', ]
cli_extras = client_extras + ['watchdog', 'ConfigArgParse', 'pyyaml', ]

all_extras = cli_extras

setuptools.setup(
    name="redgrease",
    version=version,
    license="MIT License",
    description="RedisGears helper package",
    long_description=text("README.md"),
    long_description_content_type="text/markdown",

    author="Anders Åström",
    author_email="anders@lyngon.com",
    url="https://github.com/lyngon/redgrease",

    packages=setuptools.find_packages(where='src'),
    package_dir={'': 'src'},
    classifiers=[  # https://pypi.org/classifiers/
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Database",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries :: Application Frameworks",  # noqa: E501
    ],
    install_requires=[],  # Should remain empty for minimal runtime install.
    extras_reqires={
        'all': all_extras,
        'client': client_extras,
        'cli':  cli_extras,
    },
    entry_points={
        'console_scripts': ['redgrease=redgrease.cli:main [cli]'],
    },
    setup_requires=[],
    python_requires='>=3.6',
    keywords="Redis, Gears, development"
)

sys.exit(0)
