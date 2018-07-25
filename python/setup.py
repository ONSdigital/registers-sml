import re

from setuptools import setup, find_packages

# determine version
VERSION_FILE = "sml/_version.py"
verstrline = open(VERSION_FILE, "rt").read()
VERSION_REGEX = r"^__version__ = ['\"]([^'\"]*)['\"]"
mo = re.search(VERSION_REGEX, verstrline, re.M)
if mo:
    version_string = mo.group(1)
else:
    raise RuntimeError("Unable to find version string in %s." % (VERSION_FILE,))

JAR_FILE = 'sml-' + version_string + '-SNAPSHOT-jar-with-dependencies.jar'

setup(
    name='sml',
    description='Part of the SML Archetype this is the setup file for the python part of the project',
    author='James Smith',
    author_email='james.a.smith@ext.ons.gov.uk',
    url='http://np2rvlapxx507/DAP-S/SML-Archetye.git',
    version=version_string,
    packages=find_packages(),
    include_package_data=True,
    classifiers=[],
    keywords=['spark', 'SML', 'Archetype'],
    install_requires="",
    test_requires=""
)
