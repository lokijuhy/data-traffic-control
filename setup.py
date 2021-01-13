from setuptools import setup, find_packages

URL = "https://github.com/uzh-dqbm-cmi/data-traffic-control"
PROJECT_URLS = {
    "Bug Tracker": "https://github.com/uzh-dqbm-cmi/data-traffic-control/issues",
    "Documentation": "https://data-traffic-control.readthedocs.io/en/latest/",
    "Source Code": "https://github.com/uzh-dqbm-cmi/data-traffic-control",
}

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(name='datatc',
      version='0.0.5',
      author="Laura Kinkead",
      description='Automate every-day interactions with your data.',
      long_description=long_description,
      long_description_content_type="text/markdown",
      url=URL,
      project_urls=PROJECT_URLS,
      packages=find_packages(),
      classifiers=[
            "Programming Language :: Python :: 3",
            "License :: OSI Approved :: MIT License",
            "Operating System :: OS Independent",
            "Development Status :: 2 - Pre-Alpha",
            "Intended Audience :: Science/Research",
            "Topic :: Scientific/Engineering",
      ],
      python_requires='>3.7.0',
      install_requires=[
            'dill',
            'flake8',
            'gitpython',
            'pandas',
            'pyyaml',
            'xlrd',
      ],
      extras_require={
            'pdf': ['pymupdf'],
            'docs': ['sphinx_autodoc_typehints'],
      },
      zip_safe=False)
