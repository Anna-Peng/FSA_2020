FROM continuumio/miniconda3

COPY . . 

RUN conda env create -f environment.yml

RUN pip install -e .

ENTRYPOINT [ "conda", "run", "-n", "foobar-database", "python", "FOOBAR.py"]
