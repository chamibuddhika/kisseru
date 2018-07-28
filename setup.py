import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="kisseru",
    version="0.1",
    author="Buddhika Chamith",
    author_email="chamibuddhika@gmail.com",
    description=
    "A simply scriptable workflow library for application pipelines.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/chamibuddhika/kisseru",
    packages=setuptools.find_packages(),
    install_requires=[
        'pandas',
        'xlwt',
        'numpy',
    ],
    classifiers=(
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ),
)
