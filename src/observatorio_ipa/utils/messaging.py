import smtplib
import re
import logging
from email.message import EmailMessage
from datetime import datetime
from email_validator import validate_email, EmailNotValidError
from observatorio_ipa.core.config import LOGGER_NAME

logger = logging.getLogger(LOGGER_NAME)


class EmailSender:
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

    smtp_connection = None
    from_address: str = ""
    to_address: list[str] = []

    def __init__(
        self,
        smtp_server: str,
        smtp_port: int,
        smtp_username: str,
        smtp_password: str,
        from_address: str,
        to_address: list[str] | str,
    ) -> None:
        """
        Initializes the EmailSender class.

        Parameters:
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
        to_address : str
            The email address to use as the recipient.
        """
        if not isinstance(from_address, str):
            raise ValueError("from_address must be a string")

        if not isinstance(to_address, str | list):
            raise ValueError("from_address must be a list or string")

        if isinstance(to_address, str):
            to_address = [to_address]

        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.smtp_username = smtp_username
        self.smtp_password = smtp_password
        self.from_address = from_address
        self.to_address = to_address
        if not self.test_connection():
            raise Exception("Failed to create EmailSender class: Connection failed.")

    def _connect(self) -> None:
        """
        Connects to the SMTP server.
        """
        try:
            self.smtp_connection = smtplib.SMTP(self.smtp_server, self.smtp_port)
            self.smtp_connection.starttls()
            self.smtp_connection.login(self.smtp_username, self.smtp_password)
        except Exception as e:
            print(f"Error connecting to SMTP server: {e}")
            self.smtp_connection = None

    def test_connection(self) -> bool:
        """
        Tests the connection to the SMTP server.

        Returns:
        --------
        bool
            True if the connection is successful, False otherwise.
        """
        self._connect()

        if self.smtp_connection is None:
            return False
        status = self.smtp_connection.noop()[0]
        self._close_connection()
        return True if status == 250 else False

    def send_email(self, subject: str, body: str) -> None:
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
        if self.smtp_connection is None:
            return

        for _address in self.to_address:
            try:
                message = EmailMessage()
                message["From"] = self.from_address
                message["To"] = _address
                message["Subject"] = subject
                message.set_content(body)
                self.smtp_connection.send_message(message)
            except Exception as e:
                logger.error(f"Error sending email: {e}")

        # Close the connection
        self._close_connection()

    def _close_connection(self) -> None:
        """
        Closes the connection to the SMTP server.
        """
        if self.smtp_connection:
            try:
                self.smtp_connection.quit()
            except Exception as e:
                logger.error(f"Error closing SMTP connection: {e}")
            finally:
                self.smtp_connection = None

    def __del__(self) -> None:
        """
        Closes the connection to the SMTP server when the object is deleted.
        """
        self._close_connection()


def init_email_service(config: dict) -> EmailSender | None:
    """
    Initializes an instance of EmailSender if email messaging is enabled in the configuration.

    Args:
        config (Config): A configuration dictionary containing the variables for email messaging.

    Returns:
        An instance of EmailSender if email messaging is enabled, otherwise None.
    """
    # TODO: Print Warning to console if email messaging init fails

    logger.debug("Initializing email messaging")

    if not config.get("enable_email", False):
        logger.debug("Email messaging disabled")
        return None

    try:
        email_sender = EmailSender(
            smtp_server=config["smtp_server"],
            smtp_port=config["smtp_port"],
            smtp_username=config["smtp_user"],
            smtp_password=config["smtp_password"],
            from_address=config["smtp_from_address"],
            to_address=config["smtp_to_address"],
        )

        logger.debug("Email messaging enabled")
        return email_sender

    except Exception as e:
        logger.warning(f"Initializing email service failed: {e}")

    return None


# def build_email_message(message, start_time, end_time):
#     """
#     Build an email message to send to the user.
#     """
#     text = f"""\
#     Start time: {start_time}
#     End time: {end_time}

#     {message}
#     """
#     html = f"""\
#     <html>
#         <body>
#             <p>Start time: {start_time}<br>
#              End time: {end_time}</p>
#             <p>{message}</p>
#         </body>
#     </html>
#     """
#     return text, html


# def send_email(server: EmailSender, message, start_time, end_time):
#     """
#     Send an email to the user.
#     """
#     subject = "Snow Image Processing Automation"
#     text, html = build_email_message(message, start_time, end_time)
#     server.send_email(
#         subject=subject,
#         body=text,
#     )
#     return


def parse_emails(emails_str: str) -> list[str]:
    """
    Parse a string emails separated by commas or semicolon into a list of email addresses.

    validates if the emails are valid. Omits invalid emails from the result list and logs a warning
    for each invalid email.

    Args:
        emails_str (str): A string of email addresses separated by comma or semicolon

    Returns:
        List[str]: A list of valid email


    """
    emails = re.split(r"[,;]\s*", emails_str)
    valid_emails = []
    for email in emails:
        try:
            # Validate the email using email-validator package
            valid = validate_email(email)
            # Append the email to the list of valid emails
            valid_emails.append(valid.normalized)
        except EmailNotValidError:
            # If the email is not valid, skip it
            logger.warning(f"Invalid email address skipped: {email}")
    return valid_emails


# def get_template(template: str, default_template: str) -> str:
#     try:
#         file = resources.files(templates) / template
#         with file.open() as f:
#             return f.read()

#     except FileNotFoundError:
#         logger.error(f"Template file not found: {file}")
#         return default_template
#     except Exception as e:
#         logger.error(f"Error reading template file: {e}")
#         return default_template


# def email_results(
#     email_service: EmailSender,
#     results: str,
#     script_start_time: str | None = None,
# ) -> None:
#     """
#     Send an email with export results using the provided email service.

#     Args:
#         email_service (EmailSender): An instance of EmailSender to use for sending the email.
#         script_start_time (str): The start time of the script.
#         body (str): The body of the email.
#         template (str, optional): The template to use for the email body. Defaults to "default.html".

#     Returns:
#         None

#     """

#     if not script_start_time:
#         script_start_time = "Not logged"
#     script_end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

#     if email_service is not None:

#         default_template = "[results]"
#         message = get_template(RESULTS_EMAIL_TEMPLATE, default_template)
#         message = message.replace("[results]", results)
#         message = message.replace("[start_time]", script_start_time)
#         message = message.replace("[end_time]", script_end_time)

#         subject = "OSN Image Processing Automation"
#         email_service.send_email(subject=subject, body=message)

#     return
