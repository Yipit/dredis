# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import re
import os
import dredis
from setuptools import setup, find_packages


def parse_requirements():
    """Rudimentary parser for the `requirements.txt` file

    We just want to separate regular packages from links to pass them to the
    `install_requires` and `dependency_links` params of the `setup()`
    function properly.
    """
    try:
        requirements = \
            map(str.strip, local_file('requirements.txt').splitlines())
    except IOError:
        raise RuntimeError("Couldn't find the `requirements.txt' file :(")

    links = []
    pkgs = []
    for req in requirements:
        if not req:
            continue
        if 'http:' in req or 'https:' in req:
            links.append(req)
            name, version = re.findall(r"\#egg=([^\-]+)-(.+$)", req)[0]
            pkgs.append('{0}=={1}'.format(name, version))
        else:
            pkgs.append(req)

    return pkgs, links


def local_file(f):
    return open(os.path.join(os.path.dirname(__file__), f)).read()


install_requires, dependency_links = parse_requirements()


if __name__ == '__main__':
    setup(
        name="dredis",
        version=dredis.__version__,
        description="Disk-based Redis implementation",
        long_description=local_file('README.md'),
        long_description_content_type='text/markdown',
        author='Yipit Coders',
        author_email='coders@yipitdata.com',
        url='https://github.com/Yipit/dredis',
        packages=find_packages(exclude=['*tests*']),
        install_requires=install_requires,
        dependency_links=dependency_links,
        include_package_data=True,
        classifiers=[
            'Programming Language :: Python',
            'License :: OSI Approved :: MIT License',
            'Topic :: Database',
        ],
        entry_points={
            'console_scripts': [
                'dredis = dredis.server:main',
            ]
        },
        zip_safe=False,
    )
