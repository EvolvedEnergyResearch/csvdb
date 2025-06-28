from setuptools import setup

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name='csvdb',
    version=2.0,
    author='Rich Plevin and Ryan Jones',
    author_email="ryan.jones@evolved.energy",
    description="Python library and scripts for interacting with data stored in CSV files.",
    long_description=long_description,
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",  # Adjust as needed
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    python_requires=">=3.8",
    packages=['csvdb'],
    entry_points={'console_scripts': ['genClasses = csvdb.genClasses:main',
                                      'csvdbSchema = csvdb.validation:main']},
    install_requires=[
        "pandas",
        "polars",
    ],
    url='https://github.com/EvolvedEnergyResearch/csvdb',
    download_url='https://github.com/EvolvedEnergyResearch/csvdb.git',
    include_package_data = False,
    zip_safe=False,
)
