import click
from woozie.service_layer import services


@click.command()
@click.option("-w", help="Workflow definition YAML file.")
@click.option("-c", help="Workflow configuration YAML file.")
@click.option("-o", required=True, help="Output directory.")
def generate_workflow(w, c, o):
    services.generate_workflow(output_directory=o)


if __name__ == "__main__":
    generate_workflow()