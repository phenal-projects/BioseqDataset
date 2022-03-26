from setuptools import setup

with open("./requirements.txt") as fin:
    requirements = [line.strip() for line in fin]

setup(
    name="bioseq_dataset",
    version="2022.03",
    description="A database for biological sequences",
    url="https://github.com/phenal-projects/BioseqDataset",
    author='Ilya Bushmakin',
    author_email='isnicsi@gmail.com',
    license="MIT",
    packages=["bioseq_dataset", "bioseq_dataset.lmdb", "bioseq_dataset.sequence_dataset"],
    install_requires=requirements,
)
