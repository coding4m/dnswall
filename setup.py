from setuptools import setup, find_packages

try:
    import multiprocessing  # noqa
except ImportError:
    pass

setup(
    name="dnswall",
    version="1.0.0",
    packages=find_packages(),
    author="coding4m",
    author_email="coding4m@gmail.com",

    install_requires=['python-etcd>=0.4.3', 'twisted>=15.5.0', 'docker-py>=1.6.0', 'jsonselect>=0.2.3'],

    entry_points={
        'console_scripts': [
            'dnswall-daemon = dnswall.daemon:main',
            'dnswall-agent = dnswall.agent:main'
        ]
    }

)
