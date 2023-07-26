from setuptools import find_packages, setup

setup(
    name="futureproofed",
    packages=find_packages(),
    install_requires=[
        "dagster",
        "dagit"
    ],
    extras_require={"dev": []},
)