from setuptools import setup

requirements = [
    'psycopg2-binary>=2',
    'python-dateutil>=2',
    'PyYAML>=3',
    'newrelic>=8.3.0'
]

setup(
    name = "rubydj-pyworker",
    version = '1.0.0',
    description="A pure Python worker for Ruby-based DelayedJobs",
    author="Hossam Hammady",
    author_email="github@hammady.net",
    url="https://github.com/rayyansys/pyworker",
    license="MIT",
    packages=['pyworker'],
    install_requires = requirements
)
