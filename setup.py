from setuptools import setup, find_packages

setup(
    name="esco-semantic",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "weaviate-client>=3.0.0",
        "sentence-transformers>=2.2.0",
        "pandas>=1.5.0",
        "pyyaml>=6.0",
        "click>=8.0.0",
        "torch>=2.0.0",
        "transformers>=4.30.0",
        "tqdm>=4.65.0",
        "numpy>=1.24.0",
        "pyarrow>=12.0.0",
        "tiktoken>=0.6.0",
        "sentencepiece>=0.1.99",
        "protobuf<4.0.0"
    ],
    entry_points={
        "console_scripts": [
            "esco-cli=src.esco_cli:cli",
            "esco-search=src.search_service:main",
        ],
    },
    python_requires=">=3.10",
) 