#!/bin/bash

COMPOSE_FILE="docker-compose.yml"
SERVICE="price_hero"

# Function to run the Python CLI with Docker Compose
start_services() {
    docker-compose -f $COMPOSE_FILE up --build -d
}

restart_services() {
    docker-compose -f $COMPOSE_FILE down && docker-compose -f $COMPOSE_FILE up --force-recreate -d
}

logs() {
    docker-compose -f $COMPOSE_FILE logs -f $SERVICE
}

clean() {
    docker-compose -f $COMPOSE_FILE down -v --rmi all --remove-orphans
}

run_cli() {
    docker-compose -f "$COMPOSE_FILE" exec $SERVICE python cli.py "$@"
}

# Print usage instructions
print_usage() {
    echo "Usage: $0 [run|etl|sync] [options]"
    echo "Commands:"
    echo "  start        Start price hero docker services"
    echo "  stop         Stop docker services"
    echo "  logs         View logs for the service"
    echo "  clean        Remove Docker volumes and unused resources"
    echo "  run          Run the Python CLI with optional arguments, add --help flag to check the optional usage"
    echo "Example:"
    echo "  $0 run --help"
}

# Parse command-line arguments
case "$1" in
    start) start_services ;;
    restart) restart_services ;;
    logs) logs ;;
    clean) clean ;;
    run)
        shift
        run_cli "$@"
        ;;
    *)
        print_usage
        exit 1
        ;;
esac