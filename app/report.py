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
from typing import Any, Dict

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


def is_failed_cronjob(data: Snapshot) -> str:
    """
    Returns a string describing the failure state, if any, of a CronJob.
    """
    if data["spec"]["suspend"]:
        return ""
    if not (raw_schedule := data["status"].get("lastScheduleTime")):
        return "Never scheduled"
    if not (raw_successful := data["status"].get("lastSuccessfulTime")):
        return "Never successfully ran"

    schedule = dateutil.parser.isoparse(raw_schedule)
    successful = dateutil.parser.isoparse(raw_successful)

    if abs(schedule - successful) >= datetime.timedelta(days=1):
        delta = humanize.naturaldelta(abs(get_current_datetime() - successful))
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


def load_snapshot(path: os.PathLike) -> Snapshot:
    """
    Returns a snapshot that was previously created by `app.snapshot`.
    """
    with open(path, encoding="utf-8", mode="r") as fp:
        return json.load(fp)  # type: ignore[no-any-return]


def compare_snapshots(current: Snapshot, previous: Snapshot) -> Snapshot:
    """
    Returns a new snapshot that is `current` and how it changed from `previous`.
    """
    data: Snapshot = {
        "cronjobs": {},
        "jobs": {},
        "deployments": {},
        "statefulsets": {},
        "pods": {},
        "metadata": {
            "now": current["metadata"]["start"],
        },
    }

    now = current["metadata"]["start"]
    earlier = previous.get("metadata", {}).get("start", now)
    dt_now = datetime.datetime.fromisoformat(now)
    dt_earlier = datetime.datetime.fromisoformat(earlier)
    data["metadata"]["delta"] = dt_now - dt_earlier

    for name in set(current["cronjobs"]) | set(previous.get("cronjobs", {})):
        descriptors = []
        if name not in current["cronjobs"]:
            descriptors.append("Deleted")
        else:
            if name not in previous.get("cronjobs", {}):
                descriptors.append("New")
            if reason := is_failed_cronjob(current["cronjobs"][name]):
                descriptors.append(reason)
        if descriptors:
            data["cronjobs"][name] = ", ".join(descriptors)

    for name in set(current["deployments"]) | set(previous.get("deployments", {})):
        descriptors = []
        if name not in current["deployments"]:
            descriptors.append("Deleted")
        else:
            if name not in previous.get("deployments", {}):
                descriptors.append("New")
            if reason := is_failed_deployment(current["deployments"][name]):
                descriptors.append(reason)
        if descriptors:
            data["deployments"][name] = ", ".join(descriptors)

    for name in set(current["statefulsets"]) | set(previous.get("statefulsets", {})):
        descriptors = []
        if name not in current["statefulsets"]:
            descriptors.append("Deleted")
        else:
            if name not in previous.get("statefulsets", {}):
                descriptors.append("New")
            if reason := is_failed_statefulset(current["statefulsets"][name]):
                descriptors.append(reason)
        if descriptors:
            data["statefulsets"][name] = ", ".join(descriptors)

    return data


def get_html(data: Snapshot) -> str:
    html = ""

    if data["metadata"]["delta"]:
        delta = humanize.precisedelta(data["metadata"]["delta"])
        now = data["metadata"]["now"]
        html += f"<p>In the {delta} leading up to {now}...</p>"

    if data["cronjobs"]:
        html += "<p>Noteworthy CronJobs:</p><ul>"
        for name in sorted(data["cronjobs"]):
            html += f"<li>{name}: {data['cronjobs'][name]}</li>"
        html += "</ul>"
    else:
        html += "<p>Nothing to report for CronJobs.</p>"

    if data["deployments"]:
        html += "<p>Noteworthy Deployments:</p><ul>"
        for name in sorted(data["deployments"]):
            html += f"<li>{name}: {data['deployments'][name]}</li>"
        html += "</ul>"
    else:
        html += "<p>Nothing to report for Deployments.</p>"

    if data["statefulsets"]:
        html += "<p>Noteworthy StatefulSets:</p><ul>"
        for name in sorted(data["statefulsets"]):
            html += f"<li>{name}: {data['statefulsets'][name]}</li>"
        html += "</ul>"
    else:
        html += "<p>Nothing to report for StatefulSets.</p>"

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
