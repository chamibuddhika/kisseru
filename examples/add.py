import time

from kisseru import task
from kisseru import app
from kisseru import AppRunner


@task()
def add_and_save(a, b, c):
    time.sleep(2)
    d = a + b + c
    '''bash 
        echo %{d} > result.txt
        %{outfile} = result.txt
        '''
    return outfile


@task()
def add1(infile) -> int:
    time.sleep(3)
    '''bash 
        %{value} = `cat %{infile}`
        '''
    return int(value) + 1


@task()
def sub(a, infile) -> int:
    time.sleep(4)
    '''bash
    %{value} = `cat %{infile}`
    '''
    return a - int(value)


@app()
def myapp():
    a = add_and_save(1, 2, 3)
    b = add1(a)
    c = sub(b, a)


if __name__ == "__main__":
    ar = AppRunner(myapp)
    ar.run()
