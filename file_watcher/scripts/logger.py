import logging,os
from google.cloud import logging


def get_logger(logging_project, name):
    print(f"!!! get_logger(): Configuring logging project for: {logging_project}")

    logger_client = logging.Client(project = logging_project)
    log_svc = logging.getLogger(name)
    log_svc.handlers.clear()
    default_level = os.getenv("LOGLEVEL", "INFO").upper()

    if default_level == "DEBUG":
        level = logging.DEBUG
    elif default_level == "WARNING":
        level = logging.WARNING
    elif default_level == "ERROR":
        level = logging.ERROR
    else:
        level = logging.INFO

    log_svc.setLevel(level)

    handler = logger_client.get_default_handler()
    handler.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler()

    formatter = logging.Formatter("%(asctime), %(msecs), %(levelname),-8s [%(filename)s:%(lineno)d - "
                                  "%(threadName)s] %(message)")

    handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(level)

    log_svc.addHandler(handler)
    log_svc.addHandler(console_handler)
    log_svc.propagate = False

    return log_svc


