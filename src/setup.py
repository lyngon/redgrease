import setuptools
from redgrease import requirements

with open("README.md", "r") as fh:
    long_description = fh.read()

requirements_list = requirements.read("redgrease/requirements.txt")

setuptools.setup(
    name="redgrease",
    version="0.0.1",
    author="Anders Åström",
    author_email="anders@lyngon.com",
    description="RedisGears helper package",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/lyngon/redgrease",
    packages=["redgrease"],  # setuptools.find_packages(),
    # https://pypi.org/classifiers/
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Database",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries :: Application Frameworks",
    ],
    install_requires=requirements_list,
    python_requires='>=3.6',
    keywords="Redis Gears Helper"
)
