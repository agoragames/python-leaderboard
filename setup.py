try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

requirements = [r for r in map(str.strip, open('requirements.txt').readlines())]

setup(
    name='leaderboard',
    version='1.1.5',
    author='Ola Mork, Aaron Westendorf, Vitaly Babiy',
    author_email="ola@agoragames.com, aaron@agoragames.com, vbabiy@agoragames.com",
    url='https://github.com/agoragames/python-leaderboard',
    license='LICENSE.txt',
    packages=["leaderboard"],
    description="Library for interacting with leaderboards backed by Redis",
    long_description=open('README.rst').read(),
    keywords=['python', 'leaderboard', 'redis'],
    install_requires=requirements,
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: BSD License',
        "Intended Audience :: Developers",
        "Operating System :: POSIX",
        "Topic :: Communications",
        "Topic :: Software Development :: Libraries :: Python Modules",
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.0',
        'Programming Language :: Python :: 3.1',
        'Programming Language :: Python :: 3.2',
    ]
)
