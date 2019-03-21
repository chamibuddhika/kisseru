import setuptools

long_description = """
Kisseru lets you script your data pipelines. No need to learn another framework or another DSL. Just write pipeline tasks as plain python functions. And then chain the function calls the way you need the data to flow in the pipeline and you are done!!

You can take the pipeline and run it locally in your machine (or in a cluster or in a cloud if you want it to scale -- these modes still in the works) and Kisseru will figure out how to run it most efficiently without you having to worry
about it."""

setuptools.setup(
    name="kisseru",
    version="0.2.3",
    author="Buddhika Chamith",
    author_email="chamibuddhika@gmail.com",
    description="A simply scriptable workflow library for data pipelines.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/chamibuddhika/kisseru",
    packages=setuptools.find_packages(),
    install_requires=[
        'pandas',
        'xlwt',
        'xlrd',
        'numpy',
        'jsonpickle',
        'click',
    ],
    scripts=['kisseru/kisseru-cli'],
    classifiers=(
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ),
)
