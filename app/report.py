"""
Compare two recent snapshots and send an email.
"""

import datetime
import email.mime.multipart
import email.mime.text
import json
import logging
import os
import os.path
import pathlib
import smtplib
import ssl
import sys
from typing import Any, Dict, List

import croniter  # type: ignore[import-untyped]
import dateutil.parser  # type: ignore[import-untyped]
import humanize

SMTP_HOST = os.environ.get("SMTP_HOST", "SMTP_HOST not defined")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "25"))
SMTP_USE_SSL = os.environ.get("SMTP_USE_SSL", "yes")
TO = os.environ.get("TO", "TO not defined")
FROM = os.environ.get("FROM", "FROM not defined")
SUBJECT = os.environ.get("SUBJECT", "k8s status report")
SNAPSHOT_DIR = pathlib.Path(os.environ.get("SNAPSHOT_DIR", "/snapshots"))

Snapshot = Dict[str, Dict[str, Any]]


def get_current_datetime() -> datetime.datetime:
    """
    Returns the current UTC time.
    """
    return datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)


def get_owner_kinds(data: Snapshot) -> List[str]:
    """
    Returns the "kinds" of the objects that own the given object.
    """
    kinds = []
    for owner in data.get("metadata", {}).get("ownerReferences", []):
        if kind := owner.get("kind"):
            kinds.append(kind)
    return kinds


def is_failed_cronjob(data: Snapshot) -> str:
    """
    Returns a string describing the failure state, if any, of a CronJob.
    """
    raw_schedule = data["status"].get("lastScheduleTime")
    raw_successful = data["status"].get("lastSuccessfulTime")

    if data["spec"]["suspend"]:
        return ""
    if not raw_schedule:
        if raw_successful:
            return "Never scheduled (but has run successfully)"
        return "Never scheduled"
    if not raw_successful:
        return "Never successfully ran"

    schedule = dateutil.parser.isoparse(raw_schedule)
    successful = dateutil.parser.isoparse(raw_successful)
    now = get_current_datetime()

    # Check that the job is actually being scheduled.
    # We expect that most jobs are intended to run at least weekly.

    grace_period = datetime.timedelta(days=7)

    if now - schedule > grace_period:
        delta = humanize.naturaldelta(now - schedule)
        cron = croniter.croniter(data["spec"]["schedule"], now)
        expected = cron.get_prev(datetime.datetime)
        if expected <= schedule:
            return f"Has not been scheduled in {delta} (but this might be expected)"
        return f"Has not been scheduled in {delta}"

    # Check that the job has a recorded success, taking into account the
    # fact that the snapshot might be after the job has been scheduled but
    # before that scheduled run has completed.

    grace_period = datetime.timedelta(days=1)  # for jobs that run daily

    if schedule - successful > grace_period:
        delta = humanize.naturaldelta(now - successful)
        return f"Has not run successfully in {delta}"
    return ""


def is_failed_deployment(data: Snapshot) -> str:
    """
    Returns a string describing the failure state, if any, of a Deployment.
    """
    desired = data["status"].get("replicas", 0)
    ready = data["status"].get("readyReplicas", 0)

    return "" if ready == desired else f"{ready}/{desired} Ready"


def is_failed_statefulset(data: Snapshot) -> str:
    """
    Returns a string describing the failure state, if any, of a StatefulSet.
    """
    desired = data["status"].get("replicas", 0)
    ready = data["status"].get("readyReplicas", 0)

    return "" if ready == desired else f"{ready}/{desired} Ready"


def is_failed_pod(data: Snapshot) -> str:
    """
    Returns a string describing the failure state, if any, of a Pod.
    """
    phase: str = data["status"]["phase"]

    return phase if phase in ["Pending", "Unknown"] else ""


def load_snapshot(path: os.PathLike) -> Snapshot:
    """
    Returns a snapshot that was previously created by `app.snapshot`.
    """
    with open(path, encoding="utf-8", mode="r") as fp:
        return json.load(fp)  # type: ignore[no-any-return]


def compare_snapshots(current: Snapshot, previous: Snapshot) -> Snapshot:
    """
    Returns a new snapshot highlighting objects that might need attention.
    """
    data: Snapshot = {
        "cronjobs": {},
        "deployments": {},
        "jobs": {},
        "pods": {},
        "statefulsets": {},
        "metadata": {"now": current["metadata"]["start"]},
    }

    now = current["metadata"]["start"]
    earlier = previous.get("metadata", {}).get("start", now)
    dt_now = datetime.datetime.fromisoformat(now)
    dt_earlier = datetime.datetime.fromisoformat(earlier)
    data["metadata"]["delta"] = dt_now - dt_earlier

    def compare_resource(api_resource, is_failed, ignore_owned_by=None) -> None:
        for name in set(current[api_resource]) | set(previous.get(api_resource, {})):
            descriptors = []
            if ignore_owned_by:
                item = current[api_resource].get(name)
                if not item:
                    item = previous[api_resource][name]
                if set(ignore_owned_by) & set(get_owner_kinds(item)):
                    continue
            if name not in current[api_resource]:
                descriptors.append("Deleted")
            else:
                if name not in previous.get(api_resource, {}):
                    descriptors.append("New")
                if reason := is_failed(current[api_resource][name]):
                    descriptors.append(reason)
            if descriptors:
                data[api_resource][name] = ", ".join(descriptors)

    compare_resource("cronjobs", is_failed_cronjob)
    compare_resource("deployments", is_failed_deployment)
    compare_resource("statefulsets", is_failed_statefulset)
    # Assume that CronJobs and Jobs report on their own "failed" Pods.
    compare_resource("pods", is_failed_pod, ignore_owned_by=["Job"])

    return data


def get_html(data: Snapshot) -> str:
    html = ""

    if data["metadata"]["delta"]:
        delta = humanize.precisedelta(data["metadata"]["delta"])
        now = data["metadata"]["now"]
        html += f"<p>In the {delta} leading up to {now}:</p>\n"

    def get_resource_html(api_resource, api_resource_name) -> str:
        html = ""
        if data[api_resource]:
            html += f"<p>{api_resource_name}:</p>\n<ul>\n"
            for name in sorted(data[api_resource]):
                html += f"<li>{name}: {data[api_resource][name]}</li>\n"
            html += "</ul>\n"
        else:
            html += f"<p>{api_resource_name}: (nothing to report)</p>\n"
        return html

    html += get_resource_html("cronjobs", "CronJobs")
    html += get_resource_html("deployments", "Deployments")
    html += get_resource_html("statefulsets", "StatefulSets")
    html += get_resource_html("pods", "Pods")

    return html


def send_email(data: Snapshot) -> None:
    html = get_html(data)

    logging.debug(html)

    message = email.mime.multipart.MIMEMultipart("alternative")
    message["To"] = TO
    message["Sender"] = FROM
    message["Subject"] = f"{SUBJECT}"
    message.attach(email.mime.text.MIMEText(html, "html"))

    server = smtplib.SMTP(SMTP_HOST, port=SMTP_PORT)
    if SMTP_USE_SSL != "no":
        server.starttls(context=ssl.create_default_context())
    server.send_message(message)
    server.quit()


def main() -> None:
    logging.info("Starting")

    listing = sorted(os.listdir(SNAPSHOT_DIR), reverse=True)
    files = [path for path in listing if os.path.isfile(SNAPSHOT_DIR / path)]
    if not files:
        logging.error("No snapshots found in: %s", SNAPSHOT_DIR)
        sys.exit(1)

    current = load_snapshot(SNAPSHOT_DIR / files[0])
    previous = load_snapshot(SNAPSHOT_DIR / files[1]) if len(files) >= 2 else {}
    data = compare_snapshots(current, previous)

    send_email(data)
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
