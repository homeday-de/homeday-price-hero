CREATE_DB = "CREATE DATABASE {}"

CHECK_DB_EXISTENCE = "SELECT 1 FROM pg_database WHERE datname = {};"

RESET_SEQUENCE = "ALTER SEQUENCE {} RESTART WITH {}"
