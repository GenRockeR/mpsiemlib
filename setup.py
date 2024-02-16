from setuptools import setup, find_packages
from mpsiemlib import __url__, __version__, __author__, __license__, __title__, __description__

setup(
    name=__title__,
    version=__version__,
    packages=find_packages(exclude=['tests']),
    url=__url__,
    license=__license__,
    author=__author__,
    author_email='',
    description=__description__,
    zip_safe=False,
    python_requires='>=3.7',
    install_requires=["requests",
                      "PyYAML",
                      "pytz",
                      "elasticsearch>=7.10.0"]

)
