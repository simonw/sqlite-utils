from setuptools import setup, find_packages
import io
import os

VERSION = "3.6"


def get_long_description():
    with io.open(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "README.md"),
        encoding="utf8",
    ) as fp:
        return fp.read()


setup(
    name="sqlite-utils",
    description="CLI tool and Python utility functions for manipulating SQLite databases",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    author="Simon Willison",
    version=VERSION,
    license="Apache License, Version 2.0",
    packages=find_packages(exclude=["tests", "tests.*"]),
    install_requires=["sqlite-fts4", "click", "click-default-group", "tabulate"],
    setup_requires=["pytest-runner"],
    extras_require={
        "test": ["pytest", "black", "hypothesis"],
        "docs": ["sphinx_rtd_theme", "sphinx-autobuild"],
    },
    entry_points="""
        [console_scripts]
        sqlite-utils=sqlite_utils.cli:cli
    """,
    tests_require=["sqlite-utils[test]"],
    url="https://github.com/simonw/sqlite-utils",
    project_urls={
        "Documentation": "https://sqlite-utils.datasette.io/en/stable/",
        "Changelog": "https://sqlite-utils.datasette.io/en/stable/changelog.html",
        "Source code": "https://github.com/simonw/sqlite-utils",
        "Issues": "https://github.com/simonw/sqlite-utils/issues",
        "CI": "https://github.com/simonw/sqlite-utils/actions",
    },
    python_requires=">=3.6",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Intended Audience :: End Users/Desktop",
        "Topic :: Database",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
)
