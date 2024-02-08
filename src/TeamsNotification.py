"""Teams based notifications"""

from prefect.blocks.notification import MicrosoftTeamsWebhook
from prefect.context import get_run_context
import sys

class PrefectTeamsNotification:
    """Class to send notifications to Microsoft Teams
    Args:
        logger: logger
        webhook_name: str
        notification_log: list

    Methods:
        send_notification: send notification to teams channel
    This class utilises Prefect MicrosoftTeamsWebhook block to send
    notifications to send notifications to Microsoft Teams, and must be
    configured to use this block.
    """

    def __init__(self, logger, webhook_name, notification_log):
        """Initialise the class"""
        self.logger = logger
        self.webhook_name = webhook_name
        self.notification_log = notification_log
        self.flow_run = get_run_context().flow_run
        self.logger.info("PrefectTeamsNotification object initialised")

        def send_notification(self):
            """Formate and send notification to teams channel"""
            self.logger.info(f"acuiering Prefect teams webhook block: {self.webhook_name}")
            teams_webhook_block = MicrosoftTeamsWebhook.load(self.webhook_name)
            self.logger.info("Webhook block loaded")

            self.logger.info("creating notification string")
            self.notification_string = ""
            for query in self.notification_log:
                self.notification_string += f"{query}\n"
                self.logger.debug(f"Breached query: {query}")
            self.logger.info("Notification string created")

            if self.notification_log != []:
                self.logger.info("sending notification to teams")
                notification_size = sys.getsizeof(self.notification_string)
                if notification_size > (1024*28):
                    self.logger.warn("""The notification size is grear than 28KB,
                                     Teams unable to process notifiacation""")
                    self.logger.info("sending notification to teams")
                    error_header = "Logg monitor detected breaches\n\n"
                    teams_webhook_block.notify(
                        body=f"""{error_header}\nTotal number of
                                            breeches is:{len(notification_log)}\n
                                            Notification size to large to send\n
                                            go to the logs for more information\n
                                            Flow run url: {self.flow_run}"""
                    )
                else:
                    teams_webhook_block.notify(body=f"""{error_header}\n
                                               {self.notification_string}"""
                                               )

            else:
                self.logger.info("No error detected")
