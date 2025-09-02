import smtplib
import logging
import re
import arrow
from email_validator import validate_email, EmailNotValidError

from email.message import EmailMessage
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from observatorio_ipa.core.config import (
    TXT_REPORT_TEMPLATE,
    HTML_REPORT_TEMPLATE,
    LOGGER_NAME,
)
from jinja2 import (
    Template,
    Environment,
    PackageLoader,
    FileSystemLoader,
    select_autoescape,
)


logger = logging.getLogger(LOGGER_NAME)

template_env = Environment(
    loader=PackageLoader(
        package_name="observatorio_ipa.core", package_path="templates"
    ),
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


    Methods:
    --------
    test_connection() -> bool
        Tests the connection to the SMTP server.
    send_email(subject: str, body: str) -> None
        Sends an email with the given subject and body.
    """

    smtp_connection: smtplib.SMTP | None = None

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
        # TODO: Let it re raise the error.
        # NOTE: _connect will not raise an Exception in case server is down.
        try:
            self.smtp_connection = smtplib.SMTP(self.host, self.port)
            self.smtp_connection.starttls()
            self.smtp_connection.login(self.user, self.password)
        except Exception as e:
            # Catching all errors, no specific action being taken for SMTP errors
            e_message = f"Error connecting to SMTP server: {e}"
            logger.error(e_message)
            self.smtp_connection = None
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
            if self.smtp_connection is None:
                raise Exception("SMTP connection failed. Check logs for specific error")
            response_code, _ = self.smtp_connection.noop()
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

        args:
            subject (str): The subject of the email.
            body (str): The body of the email.
            from_address (str): The email address of the sender.
            to_address (str | list[str]): The email address(es) of the recipient(s).

        """
        self._connect()

        if self.smtp_connection is None:
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
                if self.smtp_connection is not None:
                    self.smtp_connection.send_message(message)
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
        """
        Sends an email with both plain text and HTML content.

        args:
            subject (str): The subject of the email.
            txt_message (str): The plain text version of the email.
            html_message (str): The HTML version of the email.
            from_address (str): The email address of the sender.
            to_address (str | list[str]): The email address(es) of the recipient(s).
        """
        self._connect()

        if self.smtp_connection is None:
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
                if self.smtp_connection is not None:
                    self.smtp_connection.sendmail(
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
        if self.smtp_connection is None:
            return

        try:
            # Quit and set connection to none
            self.smtp_connection.quit()
            self.smtp_connection = None
        except Exception as e:
            logger.error(f"Error closing SMTP connection: {e}")
            self.smtp_connection = None

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


# Useful functions
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
# TODO: Consider raising error to retry email
def send_report_message(
    email_service: EmailService,
    from_address: str,
    to_address: str | list[str],
    context: dict,
) -> None:

    # Render TEXT and HTTP email template
    try:
        txt_template = template_env.get_template(TXT_REPORT_TEMPLATE)
        txt_message = txt_template.render(context)
        html_template = template_env.get_template(HTML_REPORT_TEMPLATE)
        html_message = html_template.render(context)
    except Exception as e:
        print(str(e))
        logger.error(f"Error reading or rendering email template: {str(e)}")
        return None

    try:
        subject = f"OSN IPA Export Report: {context.get('job_status', 'Unknown')}"
        email_service.send_html_email(
            subject=subject,
            txt_message=txt_message,
            html_message=html_message,
            from_address=from_address,
            to_address=to_address,
        )
    except Exception as e:
        print(str(e))
        logger.error(f"Error sending email: {str(e)}")

    return None
