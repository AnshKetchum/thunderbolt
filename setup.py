from setuptools import setup, find_packages
from pathlib import Path

# Read and clean requirements
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
    zip_safe=False,
    python_requires=">=3.8",
)