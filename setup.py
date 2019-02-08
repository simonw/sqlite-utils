from setuptools import setup, find_packages
import io
import os

VERSION = "0.11"


def get_long_description():
    with io.open(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "README.md"),
        encoding="utf8",
    ) as fp:
        return fp.read()


setup(
    name="sqlite-utils",
    description="Python utility functions for manipulating SQLite databases",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    author="Simon Willison",
    version=VERSION,
    license="Apache License, Version 2.0",
    packages=find_packages(),
    install_requires=["click"],
    setup_requires=["pytest-runner"],
    extras_require={"test": ["pytest", "black"]},
    entry_points="""
        [console_scripts]
        sqlite-utils=sqlite_utils.cli:cli
    """,
    tests_require=["sqlite-utils[test]"],
    url="https://github.com/simonw/sqlite-utils",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Intended Audience :: End Users/Desktop",
        "Topic :: Database",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
)
