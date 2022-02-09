import os

from setuptools import setup, find_packages


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


long_description = read('README.md') if os.path.isfile("README.md") else ""

setup(
    name='web3-pandas',
    version='0.2.2',
    author='songv',
    author_email='songwei@iftech.io',
    description='Pandas extension utils for Web3',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/tellery/web3-pandas',
    packages=find_packages(),
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9'
    ],
    keywords='web3, pandas',
    python_requires='>=3.6,<4',
    install_requires=[
        'pandas==1.4.0',
        'web3==5.26.0',
        'pandarallel==1.5.5'
    ],
    project_urls={
        'Bug Reports': 'https://github.com/tellery/web3-pandas/issues',
        'Source': 'https://github.com/tellery/web3-pandas',
    },
)
