from setuptools import setup

setup(name='datatc',
      version='0.0.1',
      description='',
      url='https://github.com/uzh-dqbm-cmi/data-traffic-control',
      packages=['datatc',],
      python_requires='>3.5.0',
      install_requires=[
            'dill',
            'flake8',
            'gitpython',
            'pandas',
            'pyyaml',
            'xlrd',
      ],
      extras_require={
            'pdf': ['pymupdf']
      },
      zip_safe=False)
