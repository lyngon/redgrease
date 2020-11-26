#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import setuptools
import pathlib

here = pathlib.Path(__file__).parent.resolve()


def text(*names, encoding='utf8'):
    return here.joinpath(*names).read_text(encoding=encoding)


setuptools.setup(
    name="redgrease",
    version="0.0.12",
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
        "Topic :: Software Development :: Libraries :: Application Frameworks",
    ],
    install_requires=[
        'redis'
    ],
    extras_requires=[],
    setup_requires=[],
    python_requires='>=3.6',
    keywords="Redis, Gears, development"
)
