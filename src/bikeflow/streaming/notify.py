"""Out-of-scope hook: push an alert to Slack, e-mail or a webhook.

Nothing here is part of Deliverable 1: the alerts are shown on the dashboard,
which reads Redis. The hook exists so that adding notifications later does not
mean touching the job: flip :data:`NOTIFICATIONS_ENABLED` and implement the call.
"""

from __future__ import annotations

NOTIFICATIONS_ENABLED = False


def send_alert_notification(alert: dict) -> None:
    """BLANK: sending alerts outside the dashboard is not implemented.

    What it should do: post ``alert`` (the same hash the dashboard reads) to the
    team's Slack/Telegram webhook, throttled per station and severity. It must
    never raise into the streaming job: a failing notification cannot lose data.
    """
    raise NotImplementedError("alert notifications are not implemented (dashboard only)")
