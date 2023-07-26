from setuptools import find_packages, setup

setup(
    name="futureproofed",
    packages=find_packages(),
    install_requires=[
        "dagster",
        "dagit",
        "numpy<1.23.0",
        "pandas==2.0.3"
    ],
    extras_require={"dev": []},
)