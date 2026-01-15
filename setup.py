from setuptools import setup, find_packages
from pathlib import Path

# Read requirements.txt
requirements = [
    line.strip() 
    for line in Path("requirements.txt").read_text().splitlines()
    if line.strip() and not line.startswith("#")
]

setup(
    name="thunderbolt",
    author="Ansh Chaurasia",
    version="0.0.1",
    packages=find_packages(),
    install_requires=requirements,
    entry_points={
        'console_scripts': [
            'thunderbolt-master=thunderbolt.cli:master_cli',
            'thunderbolt-slave=thunderbolt.cli:slave_cli',
        ],
    },
    zip_safe=False,
    python_requires=">=3.8",
)