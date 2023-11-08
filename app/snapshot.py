"""
Take a snapshot of a Kubernetes cluster.
"""

import datetime
import json
import logging
import os
import pathlib
import sys
from typing import Any, Dict

import kubernetes  # type: ignore[import-untyped]

SNAPSHOT_DIR = pathlib.Path(os.environ.get("SNAPSHOT_DIR", "/snapshots"))
VERIFY_SSL = os.environ.get("VERIFY_SSL", "yes")

Snapshot = Dict[str, Dict[str, Any]]


def get_current_time() -> str:
    """
    Returns the current UTC time in ISO 8601 format.
    """
    return datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()


def get_client() -> kubernetes.client.CoreV1Api:
    """
    Returns a client object for interacting with a Kubernetes cluster.
    """
    config = kubernetes.client.Configuration()
    kubernetes.config.load_config(client_configuration=config)

    if VERIFY_SSL == "no":
        config.verify_ssl = False

    return kubernetes.client.CoreV1Api(kubernetes.client.ApiClient(config))


def scan_cronjobs(k8s: kubernetes.client.CoreV1Api, data: Snapshot) -> None:
    raise NotImplementedError


def scan_pods(k8s: kubernetes.client.CoreV1Api, data: Snapshot) -> None:
    raise NotImplementedError


def main() -> None:
    logging.info("Starting")

    data: Snapshot = {
        "cronjobs": {},
        "pods": {},
        "metadata": {"version": "1", "start": get_current_time()},
    }
    k8s = get_client()
    snapshot_file = SNAPSHOT_DIR / f'{data["metadata"]["start"]}.json'

    scan_cronjobs(k8s, data)
    scan_pods(k8s, data)
    data["metadata"]["end"] = get_current_time()

    with open(snapshot_file, encoding="utf-8", mode="w") as fp:
        json.dump(data, fp, indent=2)
    logging.info("Finished!")


def entrypoint() -> None:
    try:
        logging.basicConfig(
            format="%(asctime)s ~ %(message)s",
            level=logging.DEBUG,
        )
        main()
    except Exception:  # pylint: disable=broad-except
        logging.exception("Uncaught exception")
        sys.exit(1)


if __name__ == "__main__":
    entrypoint()
