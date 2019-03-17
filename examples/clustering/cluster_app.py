# import env
import time
import pandas as pd

from kisseru import csv
from kisseru import xls
from kisseru import png
from kisseru import task
from kisseru import app
from kisseru import AppRunner

from kmeans import kmeans


@task()
def extract_coords_gz(infile) -> csv:
    '''bash 
        gunzip -c %{infile} > tmp.csv
        sed -i -e '1,3d' tmp.csv
        cat tmp.csv | cut -d, -f2,3 > 2017.csv
        %{outfile}=2017.csv
        '''
    return outfile


@task()
def extract_coords_xls(infile) -> xls:
    outfile = '2016.xls'
    df = pd.read_excel(infile, usecols=[1, 2])
    df.to_excel(outfile, index=False, header=False)
    return outfile


@task()
def join(infile1: csv, infile2: csv) -> csv:
    '''bash
        cat %{infile1} %{infile2} > combined.csv
        %{combined} = combined.csv
    '''
    return combined


@task()
def run_kmeans(infile, plot) -> png:
    kmeans(infile, plot)
    return plot


@app()
def cluster_app():
    url = 'ftp://ftp.ncdc.noaa.gov/pub/data/swdi/database-csv/v2/hail-2017.csv.gz'
    local = 'hail-2016.xls'
    plot = 'hail.png'

    f1 = extract_coords_gz(url)
    f2 = extract_coords_xls(local)
    combined = join(f1, f2)
    plot = run_kmeans(combined, plot)
    return plot

if __name__ == "__main__":
    ar = AppRunner(cluster_app)
    ar.run()
