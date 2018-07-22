from kisseru import task
from kisseru import _CSV


@task(split='dfd')
def add(a: int, b: int, c: int, name='add') -> _CSV:
    b = a + c \
            + a
    '''bash ls -al > %{b}.txt'''
    return d


if __name__ == "__main__":
    print(add(1, 2, 3, name='add'))
