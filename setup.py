#coding: utf-8

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="dbinorm",
    version="0.1.0",
    author="Anton Ampilogov (OOO 'Rabus')",
    author_email="rabus.gh@t48.ru",
    description="Database interface not orm.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/rabus-t48/dbinorm",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 2",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    license='MIT',
    python_requires='>=2.7',
)
