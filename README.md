![Banner](https://github.com/team-fsa-s2ds-alumni/foobar-database/blob/master/static/banner.png)

**F**ood **O**f **O**nline **B**usinesses **A**nd **R**estaurants

<hr>

*An interactive tool to map the online food ecosystem.*

<br>

![CI (pip)](https://github.com/team-fsa-s2ds-alumni/foobar-database/workflows/CI%20(conda)/badge.svg)
[![Project Status: Active](https://www.repostatus.org/badges/latest/active.svg)](https://www.repostatus.org/#active)

`FOOBAR` provides a command-line tool with a built-in web scraping tool to assemble a relational database that maps the landscape of online food platforms.

## Documentation

The official documentation is hosted on this repository's [wiki](https://github.com/team-fsa-s2ds-alumni/foobar-database/wiki).

## Install

```
git clone https://github.com/team-fsa-s2ds-alumni/foobar-database.git
cd foobar-database
conda env create -f environment.yml
conda activate foobar
pip install -e .
```

## Update conda environment with yaml
```
conda activate foobar
conda env update -f environment.yml
```

## Quick Start

```
python FOOBAR.py
```

## Tests
```
pytest
```

## Build Docker image

```
docker build -t foobar .
```

## Run Docker image

```
docker run foobar
```

