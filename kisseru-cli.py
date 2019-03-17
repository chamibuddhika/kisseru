
import sys
import os
import importlib 
import click

from backend import BackendType
from backend import BackendConfig
from backend import Backend
from kisseru import AppRunner

def get_backend_config(backend):
    if backend == "slurm":
        return BackendConfig(BackendType.SLURM, "Slurm")
    elif backend == "local":
        return BackendConfig(BackendType.LOCAL, "Local Threaded")
    elif backend == "serial":
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
@click.option('--app', "-a", default='', \
        help="Application to be packaged. If not provided, defaults to an "\
        "application with the same name as the provided file.")
@click.option('--backend', "-b", default='slurm', help="Backend to run the "\
        "application. Defaults to slurm.")
@click.option('--image', "-i", default='singularity', help="Image type to "\
        "generate. Defaults to a singularity image.")
@click.option('--out', "-o", default='.', help="Directory to write the "\
        "generated deployable artifact. Defaults to current directory.")
@click.argument('filename')
def package(app, backend, out, filename):
    module_dir, module_file = os.path.split(filename)

    module_name, ext = os.path.splitext(module_file)

    if ext != ".py":
        raise Exception("Expected a python file. Got {}".format(module_file))

    # Add application directory to the enviornment
    app_dir = os.path.join(os.getcwd(), module_dir)
    sys.path.append(app_dir)

    module = importlib.import_module(module_name)

    # If no specific app name is provided assume app name is the same as module
    # name
    if app == '':
        app = module_name

    app = getattr(module, app)

    out = os.path.abspath(out)
    out = os.path.join(out, module_name + ".tar.gz")

    ar = AppRunner(app, get_backend_config(backend))
    ar.package(app_dir, out)

    return out

@cli.command()
@click.option('--url', "-u", default='', help="Server to be deployed.")
@click.argument('filename')
def deploy(url, filename):
    pass

if __name__ == "__main__":
    cli()
