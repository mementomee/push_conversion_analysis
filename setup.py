from setuptools import setup, find_packages

setup(
    name="push_conversion_analysis",
    version="1.0.0",
    description="Аналіз впливу push-сповіщень на депозити Android-користувачів",
    author="Nazar Petrashchuk",
    author_email="petrasuknazar@gmail.com",
    packages=find_packages(),
    install_requires=[
        "clickhouse-connect>=0.6.0",
        "pandas>=2.0.0",
        "numpy>=1.24.0",
        "matplotlib>=3.7.0",
        "seaborn>=0.12.0",
        "plotly>=5.15.0",
        "jupyter>=1.0.0",
        "ipykernel>=6.25.0",
        "python-dotenv>=1.0.0",
        "openpyxl>=3.1.0",
        "scikit-learn>=1.3.0"
    ],
    python_requires=">=3.8",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
)