
class Foo(object):

    def foo(self, a, b, c):
        return a + b + c

start = Foo()

if __name__ == "__main__":
    b = { 'df':1, 'sdf':2 }
    p = start> load                                               \ 
               | fil                                              \
               // {'ntasks':2, 'split':csv, 'combine':csv} in cv  \
               %  {'until' = kiseru._check('>', 2.0)} in iters    \
               &  {'join': kiseru._bykey('') in [email, store]    \
               ^  (kiseru._check(2, '>'), iff, kiseru._abort())   \ 
               | save 
    # b = a(2)
    # c = b(3)

    print(a)
