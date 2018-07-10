
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

    models = [LinearRegression(), RandomForest(), KNN(n_neighbors=7), SVR()]
    run_models = ModelRunner(models).setErrorType('RSME') 

    p = start> LoadNetCDF4('data.nc') | SummarizeGrid('5x5') // {'splitBy': cell} in run_models | Save('models.csv')
    p.setName('Weather Model Selection')
    
    repo = PipelineRepo('rest_url')
    repo.submitPipeline(p)
    p = repo.getPipeline('Weather Model Selection')

    p.run('spark://spark_url')       # Runs on Spark using SPARK runner
    p.run('airavata://airavata_url') # Runs on Spark using SPARK runner
    p.run('cluster://helix_url')     # Runs on the cluster as a Helix DAG
    p.run()                          # Runs locally


    # b = a(2)
    # c = b(3)

    print(a)
