#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import io
import setuptools
import os.path
# from redgrease import requirements


def read(*names, encoding='utf8'):
    with io.open(
        os.path.join(os.path.dirname(__file__), *names),
        encoding=encoding
    ) as fh:
        return fh.read()


setuptools.setup(
    name="redgrease",
    version="0.0.2",
    license="MIT License",
    description="RedisGears helper package",
    long_description=read("README.md"),
    long_description_content_type="text/markdown",

    author="Anders Åström",
    author_email="anders@lyngon.com",
    url="https://github.com/lyngon/redgrease",

    packages=setuptools.find_packages('src'),
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
    keywords="Redis Gears Helper"
)
