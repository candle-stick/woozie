from setuptools import setup

setup(
    name="woozie",
    version="0.1",
    packages=["woozie"],
    include_package_data=True,
    install_requires=["Click"],
    entry_points="""
        [console_scripts]
        woozie=woozie.entrypoints.cli:generate_workflow
    """,
)