"""Alerting. v1 is log-only: it records the event so the job log shows it.
Wire up email/Slack here once the spec's alerts.target is in use."""
import logging

log = logging.getLogger("sdp.alerts")


def notify(event: str, pipeline: str, message: str) -> None:
    log.warning("[ALERT:%s] pipeline=%s %s", event, pipeline, message)
    print(f"[ALERT:{event}] pipeline={pipeline} {message}")
