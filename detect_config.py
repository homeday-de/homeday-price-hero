import asyncclick as click
from src.lib.aws import SecretManager

@click.command()
@click.option('--get', is_flag=True, help='get secrets')
@click.option('--update', is_flag=True, help='update secrets')
def main(get, update):
    sm = SecretManager()
    if get:
        sm.create_config_file('config')
    if update:
        sm.update_secret_to_vault('config')


main()
