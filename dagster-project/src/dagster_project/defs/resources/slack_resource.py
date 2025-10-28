from dagster import EnvVar
from dagster_slack import SlackResource

slack_resource = SlackResource(token=EnvVar("SLACK_BOT_TOKEN"))