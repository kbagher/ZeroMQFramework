from setuptools import setup, find_packages

setup(
    name="ZeroMQFramework",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "loguru==0.7.2",
        "pyzmq==26.0.3",
        "setuptools==70.2.0"
    ],
    include_package_data=True,
    description="A simple and flexible framework designed to simplify the creation of a req/reply routers, servers, clients, and workers using ZeroMQ.",
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author="Kassem Bagher",
    author_email="kassem@bagher.me",
    url="https://github.com/kbagher/ZeroMQFramework",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
