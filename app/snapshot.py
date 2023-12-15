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

import kubernetes.client as k8s  # type: ignore[import-untyped]
import kubernetes.config  # type: ignore[import-untyped]

NAMESPACE = os.environ.get("NAMESPACE", "NAMESPACE not defined")
VERIFY_SSL = os.environ.get("VERIFY_SSL", "yes")
SNAPSHOT_DIR = pathlib.Path(os.environ.get("SNAPSHOT_DIR", "/snapshots"))

Snapshot = Dict[str, Dict[str, Any]]


def get_current_time() -> str:
    """
    Returns the current UTC time in ISO 8601 format.
    """
    return datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()


def get_api_client() -> k8s.ApiClient:
    """
    Returns an API client configured from the default configuration sources.
    """
    config = k8s.Configuration()
    kubernetes.config.load_config(client_configuration=config)

    if VERIFY_SSL == "no":
        config.verify_ssl = False

    return k8s.ApiClient(config)


def get_json(api_route, *args, **kwargs) -> Any:
    """
    Returns the raw JSON response from an API route.
    """
    response = api_route(*args, _preload_content=False, **kwargs)
    return json.loads(response.data)


def scan_apps(client: k8s.ApiClient, data: Snapshot) -> None:
    api = k8s.AppsV1Api(client)

    items = get_json(api.list_namespaced_deployment, NAMESPACE).get("items", [])
    for item in items:
        data["deployments"][item["metadata"]["name"]] = {"status": item["status"]}

    items = get_json(api.list_namespaced_stateful_set, NAMESPACE).get("items", [])
    for item in items:
        data["statefulsets"][item["metadata"]["name"]] = {"status": item["status"]}


def scan_batch(client: k8s.ApiClient, data: Snapshot) -> None:
    api = k8s.BatchV1Api(client)

    items = get_json(api.list_namespaced_cron_job, NAMESPACE).get("items", [])
    for item in items:
        data["cronjobs"][item["metadata"]["name"]] = {
            "spec": {
                "schedule": item["spec"]["schedule"],
                "suspend": item["spec"]["suspend"],
            },
            "status": item["status"],
        }

    items = get_json(api.list_namespaced_job, NAMESPACE).get("items", [])
    for item in items:
        data["jobs"][item["metadata"]["name"]] = {"status": item["status"]}


def scan_core(client: k8s.ApiClient, data: Snapshot) -> None:
    api = k8s.CoreV1Api(client)

    items = get_json(api.list_namespaced_pod, NAMESPACE).get("items", [])
    for item in items:
        data["pods"][item["metadata"]["name"]] = {
            "metadata": {
                "ownerReferences": item["metadata"].get("ownerReferences", []),
            },
            "status": item["status"],
        }


def main() -> None:
    logging.info("Starting")

    data: Snapshot = {
        "cronjobs": {},
        "deployments": {},
        "jobs": {},
        "pods": {},
        "statefulsets": {},
        "metadata": {"version": "1", "start": get_current_time()},
    }
    client = get_api_client()
    snapshot_file = SNAPSHOT_DIR / f'{data["metadata"]["start"]}.json'

    scan_apps(client, data)
    scan_batch(client, data)
    scan_core(client, data)
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
