import time

from kisseru import xls
from kisseru import csv
from kisseru import task
from kisseru import app
from kisseru import AppRunner

outfile = 'result.xls'


@task()
def fetch_excel() -> xls:
    time.sleep(2)
    '''bash 
        %{file} = result.xls
        '''
    return outfile


@task()
def output_content_of_csv(infile: csv) -> str:
    time.sleep(3)
    '''bash 
        %{value} = `cat %{infile}`
        '''
    return value


@app()
def myapp():
    a = fetch_excel()
    b = output_content_of_csv(a)
    return b


if __name__ == "__main__":
    ar = AppRunner(myapp)
    ar.run()
