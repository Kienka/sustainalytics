import setuptools


with open("README.md", "r") as fh:
    long_description = fh.read()


setuptools.setup(
    name = "sustainalytics-Kienka",
    version = "0.0.1",
    description ="This is a package that helps clients access and QA sustainalytics data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author = "Kienka Cromwell Kio",

    url='https://github.com/Kienka/sustainalytics',
    author_email = "kienka.kio@sustainalytics.com",
    packages = setuptools.find_packages(),
    classifiers=[
        'Development Status :: 3 - Alpha',
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=['pandas','requests','tqdm']
)