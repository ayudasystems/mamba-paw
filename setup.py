from setuptools import setup
# To use a consistent encoding
from codecs import open
from os import path

# Basics of pushing a new version:
# - Change version in setup()
# - Clean dist/ folder
# >> python3 setup.py sdist
# >> python3 setup.py bdist_wheel
# >> twine upload dist/*

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='mamba-paw',
    version='0.1.4',
    packages=['paw'],
    url='https://github.com/ayudasystems/mamba-paw',
    license='Apache 2.0',
    author='Maxime Lapointe',
    author_email='maxime@ayudasystems.com',
    description='Uses Azure storage queue/table for a simple worker',
    long_description=long_description,
    install_requires=['azure-storage>==0.34.3'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    python_requires='>=3.5',
)
