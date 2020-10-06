import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="boltdb",
    version="0.0.2",
    author="Tao Qingyun",
    author_email="taoqy@ls-a.me",
    description="python port of boltdb",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/qingyunha/boltdb",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
