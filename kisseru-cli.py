
import sys
import os
import importlib 
import click
from kisseru import AppRunner

def get_backend_config(backend):
    if backend == "slurm":
        return BackendConfig(BackendType.SLURM, "Slurm")
    else if backend == "local":
        return BackendConfig(BackendType.LOCAL, "Local Threaded")
    else if backend == "serial":
        return BackendConfig(BackendType.LOCAL_NON_THREADED, "Serial")

    raise Exception("Unknown backend {}".format(backend))

@click.group()
def cli():
    pass

@cli.command()
@click.argument('filename')
def run(filename):
    click.echo(filename)

@cli.command()
@click.option('--backend', default='slurm')
@click.argument('filename')
def package(backend, filename):
    module_dir, module_file = os.path.split(filename)

    module_name, ext = os.path.splitext(module_file)

    if ext != ".py":
        raise Exception("Expected a python file. Got {}".format(module_file))

    # Add module directory to enviornment
    sys.path.append(os.path.join(os.getcwd(), module_dir))

    module = importlib.import_module(module_name)

    app = getattr(module, 'cluster_app') 

    ar = AppRunner(app, get_backend_config(backend))
    graph = ar.compile()
    # click.echo(backend)
    # click.echo(filename)

if __name__ == "__main__":
    cli()
