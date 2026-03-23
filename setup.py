from setuptools import find_packages, setup

setup(
    name="hpcsizer",
    version="2.0.0",
    description="Automated SLURM Job Profiling and Resource Sizing",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=[
        "click>=8.0",
        "numpy>=1.21",
        "scipy>=1.7",
        "pandas>=1.3",
        "matplotlib>=3.4",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0",
            "ruff>=0.4",
        ],
    },
    scripts=["bin/hpg"],
)
