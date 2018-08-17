# coding: utf-8
from setuptools import setup, find_packages
import os


def read(fname):
    with open(os.path.join(os.path.dirname(__file__), fname)) as f:
        return f.read()


__version__ = "0.0.5"

setup(
    name='tradingtime',
    version=__version__,
    keywords='tradingtime',
    description=u'证券市场交易日历 2018.08.17',
    long_description=read("README.md"),

    url='https://github.com/lamter/tradingtime',
    author='lamter',
    author_email='lamter.fu@gmail.com',

    packages=find_packages(),
    package_data={
        "tradingtime": ["*.json"],
    },
    install_requires=read("requirements.txt").splitlines(),
    classifiers=[
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
        'License :: OSI Approved :: MIT License'],
)
