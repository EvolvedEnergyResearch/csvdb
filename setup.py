from setuptools import setup

setup(
    name='csvdb',
    version=1.0,
    author='Rich Plevin and Ryan Jones',
    description='Python 2.7 library and scripts for interacting with data stored in CSV files.',
    platforms=['Windows', 'MacOS', 'Linux'],

    packages=['csvdb'],
    entry_points={'console_scripts': ['genClasses = csvdb.genClasses:main']},
    install_requires=['futures', 'pandas'],
    include_package_data = False,

    url='https://github.com/EvolvedEnergyResearch/csvdb',
    download_url='https://github.com/EvolvedEnergyResearch/csvdb.git',

    zip_safe=True,
)
