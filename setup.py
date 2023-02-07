from setuptools import find_packages, setup

setup(
    name="dissect.volume",
    packages=list(map(lambda v: "dissect." + v, find_packages("dissect"))),
    install_requires=[
        "dissect.cstruct>=3.0.dev,<4.0.dev",
        "dissect.util>=3.0.dev,<4.0.dev",
    ],
)
