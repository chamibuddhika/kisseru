
import sys
import os
import importlib 
import click
from kisseru import AppRunner

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

    # Change to module directory
    sys.path.append(os.path.join(os.getcwd(), module_dir))

    module = importlib.import_module(module_file)
    print(module)

    app = getattr(module, 'cluster_app') 
    print(app)

    ar = AppRunner(app)
    ar.run()
    # click.echo(backend)
    # click.echo(filename)

if __name__ == "__main__":
    cli()
