import smtplib
import logging
import re
import arrow
from email.message import EmailMessage
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from datetime import datetime

from email_validator import validate_email, EmailNotValidError

# from snow_ipa.utils.templates import templates
# from snow_ipa.core.exporting import ExportManager
# from snow_ipa.core.configs import (
#     ERROR_TXT_EMAIL_TEMPLATE,
#     ERROR_HTML_EMAIL_TEMPLATE,
#     REPORT_TXT_EMAIL_TEMPLATE,
#     REPORT_HTML_EMAIL_TEMPLATE,
# )
from jinja2 import Template, Environment, PackageLoader, select_autoescape


logger = logging.getLogger(__name__)

template_env = Environment(
    loader=PackageLoader(package_name="observatorio_ipa", package_path="templates"),
    autoescape=select_autoescape(),
)


class EmailService:
    """
    A class for sending emails using SMTP.

    Attributes:
    -----------
    smtp_server : str
        The SMTP server to use for sending emails.
    smtp_port : int
        The port number to use for the SMTP server.
    smtp_username : str
        The username to use for authenticating with the SMTP server.
    smtp_password : str
        The password to use for authenticating with the SMTP server.
    from_address : str
        The email address to use as the sender.
    to_address : List[str]
        The email address to use as the recipient.

    Methods:
    --------
    test_connection() -> bool
        Tests the connection to the SMTP server.
    send_email(subject: str, body: str) -> None
        Sends an email with the given subject and body.
    """

    connection: smtplib.SMTP | None = None

    def __init__(
        self, host: str, port: int, user: str, password: str, **kwargs
    ) -> None:
        """
        Initializes the EmailService class.

        Parameters:
        -----------
        host : str
            The SMTP server to use for sending emails.
        port : int
            The port number to use for the SMTP server.
        user : str
            The username to use for authenticating with the SMTP server.
        password : str
            The password to use for authenticating with the SMTP server.
        """

        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.test_connection()

    def _connect(self) -> None:
        """
        Connects to the SMTP server.
        """
        # NOTE: _connect will not raise an Exception in case server is down.
        # Automated image processing should still run.
        try:
            self.connection = smtplib.SMTP(self.host, self.port)
            self.connection.starttls()
            self.connection.login(self.user, self.password)
        except Exception as e:
            # Catching all errors, no specific action being taken for SMTP errors
            e_message = f"Error connecting to SMTP server: {e}"
            logger.error(e_message)
            self.connection = None
            # raise Exception(e_message)

    def test_connection(self) -> None:
        """
        Tests the connection to the SMTP server.

        Returns:
        --------
        bool
            True if the connection is successful, False otherwise.
        """
        try:
            self._connect()
            if self.connection is None:
                raise Exception("SMTP connection failed. Check logs for specific error")
            response_code, _ = self.connection.noop()
            if response_code == 250:
                logger.debug(
                    f"SMTP connection test successful with response code: {response_code}"
                )
            else:
                raise Exception(
                    f"SMTP connection test failed with response code: {response_code}"
                )
            self._close_connection()
        except Exception as e:
            logger.exception(e)
            raise e

    def send_email(
        self, subject: str, body: str, from_address: str, to_address: str | list[str]
    ) -> None:
        """
        Sends an email with the given subject and body.

        Parameters:
        -----------
        subject : str
            The subject of the email.
        body : str
            The body of the email.
        """
        self._connect()

        if self.connection is None:
            logger.error("SMTP connection could not be established. Email not sent.")
            return

        if isinstance(to_address, str):
            to_address = [to_address]

        for _address in to_address:
            try:
                message = EmailMessage()
                message["From"] = from_address
                message["To"] = _address
                message["Subject"] = subject
                message.set_content(body)
                if self.connection is not None:
                    self.connection.send_message(message)
            except Exception as e:
                logger.error(f"Error sending email [{_address}]: {e}")

        # Close the connection
        self._close_connection()

    def send_html_email(
        self,
        subject: str,
        txt_message: str,
        html_message: str,
        from_address: str,
        to_address: str | list[str],
    ) -> None:
        self._connect()

        if self.connection is None:
            logger.error("SMTP connection could not be established. Email not sent.")
            return

        if isinstance(to_address, str):
            to_address = [to_address]

        for _address in to_address:
            try:
                # Build message
                message = MIMEMultipart("alternative")
                part1 = MIMEText(txt_message, "plain")
                message.attach(part1)
                part2 = MIMEText(html_message, "html")
                message.attach(part2)
                message["From"] = from_address
                message["To"] = _address
                message["Subject"] = subject
                if self.connection is not None:
                    self.connection.sendmail(
                        from_address, _address, message.as_string()
                    )
            except Exception as e:
                logger.error(f"Error sending email [{_address}]: {e}")

        # Close the connection
        self._close_connection()

    def _close_connection(self) -> None:
        """
        Closes the connection to the SMTP server.
        """
        if self.connection is None:
            return

        try:
            # Quit and set connection to none
            self.connection.quit()
            self.connection = None
        except Exception as e:
            logger.error(f"Error closing SMTP connection: {e}")
            self.connection = None

    def __del__(self) -> None:
        """
        Closes the connection to the SMTP server when the object is deleted.
        """
        self._close_connection()


def parse_emails(emails: str | list) -> list[str]:
    """
    Parse a string emails separated by commas or semicolon into a list of email addresses.

    validates if the emails are valid. Omits invalid emails from the result list.

    Args:
        emails_str (str): A string of email addresses separated by comma or semicolon

    Returns:
        List[str]: A list of valid email


    """
    if isinstance(emails, str):
        # Split the string into a list of emails using regex to handle various delimiters
        emails = re.split(r"[;,]", emails.strip())

    valid_emails = []
    for email in emails:
        try:
            valid = validate_email(email, check_deliverability=False)
            valid_emails.append(valid.normalized)
        except EmailNotValidError:
            logger.warning(f"Invalid email address will be skipped: {email}")
    return valid_emails


# def send_error_message(
#     script_start_time: datetime,
#     exception: Exception,
#     email_service: EmailService,
#     from_address: str,
#     to_address: str | list[str],
# ) -> str | None:

#     start_time = arrow.get(script_start_time)
#     end_time = arrow.get(datetime.now())
#     runtime = start_time.humanize(end_time, only_distance=True)

#     try:
#         email_context = {
#             "status": "Failed - Error",
#             "start_time": start_time.format("YYYY-MM-DD HH:mm:ss (dddd)"),
#             "runtime": runtime,
#             "error_message": str(exception),
#         }
#         txt_template = template_env.get_template(ERROR_TXT_EMAIL_TEMPLATE)
#         txt_message = txt_template.render(email_context)
#         html_template = template_env.get_template(ERROR_HTML_EMAIL_TEMPLATE)
#         html_message = html_template.render(email_context)

#     except Exception as e:
#         logger.error(f"Error reading or rendering email template: {e}")
#         message = f"Error Message: {str(exception)}"

#     subject = "Snow IPA Export Report: Failed"
#     email_service.send_html_email(
#         subject=subject,
#         txt_message=txt_message,
#         html_message=html_message,
#         from_address=from_address,
#         to_address=to_address,
#     )

#     return None


# def send_report_message(
#     export_manager: ExportManager,
#     script_start_time: datetime,
#     email_service: EmailService,
#     from_address: str,
#     to_address: str | list[str],
# ) -> None:

#     image_prefix = export_manager.image_prefix

#     # Runtime Info
#     start_time = arrow.get(script_start_time)
#     end_time = arrow.get(datetime.now())
#     runtime = start_time.humanize(end_time, only_distance=True)

#     # MODIS Status
#     modis_status = export_manager.modis_status

#     # General Export Plan
#     general_plan = export_manager.export_plan["final_plan"]

#     if general_plan:
#         general_plan = [f"{image_prefix}_{m[:7]}" for m in general_plan]
#     else:
#         general_plan = ["No new images to export"]

#     # General Export Plan Exceptions
#     general_exceptions = export_manager.export_plan["excluded"]
#     if len(general_exceptions.keys()) > 0:
#         general_exceptions = [
#             f"{image_prefix}_{month[:7]}: {month_status}"
#             for month, month_status in general_exceptions.items()
#         ]
#     else:
#         general_exceptions = None

#     # GEE export plan
#     export_to_gee = export_manager.export_to_gee
#     gee_path = export_manager.gee_assets_path
#     gee_export_results = [
#         f"{task.image}: {task.status} {task.error if task.error else ''}"
#         for task in export_manager.export_tasks.export_tasks
#         if task.target == "gee"
#     ]
#     if len(gee_export_results) == 0:
#         gee_export_results = ["No new images to export"]

#     # GDRIVE export plan
#     export_to_gdrive = export_manager.export_to_gdrive
#     gdrive_path = export_manager.gdrive_assets_path
#     gdrive_export_results = [
#         f"{task.image}: {task.status} {task.error if task.error else ''}"
#         for task in export_manager.export_tasks.export_tasks
#         if task.target == "gdrive"
#     ]
#     if len(gdrive_export_results) == 0:
#         gdrive_export_results = ["No new images to export"]

#     # Status
#     total_exports = len(export_manager.export_tasks.export_tasks)
#     n_existing_exports = len(
#         [
#             img
#             for img in export_manager.export_tasks.export_tasks
#             if img.status == "ALREADY_EXISTS"
#         ]
#     )
#     n_complete_exports = len(
#         [
#             img
#             for img in export_manager.export_tasks.export_tasks
#             if img.status == "COMPLETED"
#         ]
#     )
#     n_other_exports = total_exports - n_existing_exports - n_complete_exports

#     if n_other_exports > 0:
#         status = f"Completed - with errors"
#     elif n_complete_exports > 0:
#         status = f"Completed - {n_complete_exports} images exported"
#     else:
#         status = f"Completed - No new images to export"

#     # Export Summary
#     export_summary = export_manager.export_tasks.export_summary()
#     export_summary = [{"status": k, "count": v} for k, v in export_summary.items()]

#     # Render TEXT and HTTP email template
#     try:
#         email_context = {
#             "status": status,
#             "start_time": start_time.format("YYYY-MM-DD HH:mm:ss (dddd)"),
#             "runtime": runtime,
#             "export_summary": export_summary,
#             "general_plan": general_plan,
#             "general_exceptions": general_exceptions,
#             "export_to_gee": export_to_gee,
#             "gee_path": gee_path,
#             "gee_export_results": gee_export_results,
#             "export_to_gdrive": export_to_gdrive,
#             "gdrive_path": gdrive_path,
#             "gdrive_export_results": gdrive_export_results,
#             "modis": modis_status,
#         }
#         txt_template = template_env.get_template(REPORT_TXT_EMAIL_TEMPLATE)
#         txt_message = txt_template.render(email_context)
#         html_template = template_env.get_template(REPORT_HTML_EMAIL_TEMPLATE)
#         html_message = html_template.render(email_context)
#     except Exception as e:
#         print(str(e))
#         logger.error(f"Error reading or rendering email template: {str(e)}")

#     subject = f"Snow IPA Export Report: {status}"
#     email_service.send_html_email(
#         subject=subject,
#         txt_message=txt_message,
#         html_message=html_message,
#         from_address=from_address,
#         to_address=to_address,
#     )

#     return None
