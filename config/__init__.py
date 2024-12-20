
from dynaconf import Dynaconf

settings = Dynaconf(
    # envvar_prefix="DYNACONF",
    settings_files=['.secrets.json', 'geo_indices.json'],
)

# `envvar_prefix` = export envvars with `export DYNACONF_FOO=bar`.
# `settings_files` = Load these files in the order.
