from dagster.preview.freshness import FreshnessPolicy
from datetime import timedelta

daily_policy = FreshnessPolicy.time_window(fail_window=timedelta(days=1))

weekly_policy = FreshnessPolicy.time_window(fail_window=timedelta(days=7))

monthly_policy = FreshnessPolicy.time_window(fail_window=timedelta(days=30))
