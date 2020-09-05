from setuptools import setup, find_packages

setup(
    name="foobar-database",
    description="An interactive tool to map the online food ecosystem.",
    packages=["src", "src/data", "src/features"],
    platforms='any',
    classifiers=[
        'Operating System :: OS Independent',
        'Intended Audience :: Public',
        'Programming Language :: Python',
        'Programming Language :: SQL',
        'Topic :: Scientific/Engineering'
        ]
)
