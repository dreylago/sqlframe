import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="sqlframe-dreylago", # Replace with your own username
    version="0.0.1",
    author="Demetrio Rey",
    author_email="demetrio.rey@gmail.com",
    description="Handle dataframes with pure SQL",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/dreylago/sqlframe",
    packages=setuptools.find_packages(),
    classifiers=[
        "Topic :: Scientific :: Engineering",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)

