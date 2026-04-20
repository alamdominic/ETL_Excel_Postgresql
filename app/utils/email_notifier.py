"""
Email notification module for ETL pipeline.

Provides explicit email sending functionality for pipeline status notifications.
This is NOT an OOP implementation - uses simple functions for clarity.
"""

import logging
import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


def send_etl_notification(
    subject: str,
    body: str,
    is_success: bool,
    recipient_email: Optional[str] = None,
) -> bool:
    """Send email notification about ETL pipeline execution status.

    Args:
        subject: Email subject line.
        body: Email body content (plain text or HTML).
        is_success: Whether the pipeline succeeded (affects email styling).
        recipient_email: Optional recipient email. If None, uses EMAIL_RECIPIENT from .env.

    Returns:
        True if email was sent successfully, False otherwise.

    Environment variables required:
        EMAIL_SENDER: Sender email address
        EMAIL_PASSWORD: Email account password or app password
        EMAIL_RECIPIENT: Default recipient email (optional if recipient_email provided)
        SMTP_HOST: SMTP server hostname (default: smtp-mail.outlook.com)
        SMTP_PORT: SMTP server port (default: 587)

    Example:
        >>> success = send_etl_notification(
        ...     subject="ETL Pipeline - Success",
        ...     body="Pipeline completed successfully. 150 rows inserted.",
        ...     is_success=True
        ... )
    """
    load_dotenv()

    # Get email configuration from environment
    sender_email = os.getenv("EMAIL_SENDER")
    sender_password = os.getenv("EMAIL_PASSWORD")
    recipient = recipient_email or os.getenv("EMAIL_RECIPIENT", sender_email)
    smtp_host = os.getenv("SMTP_HOST", "smtp-mail.outlook.com")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))

    # Validate required credentials
    if not all([sender_email, sender_password]):
        logger.error("Email credentials missing in environment variables")
        return False

    try:
        # Create email message
        message = MIMEMultipart("alternative")
        message["Subject"] = subject
        message["From"] = sender_email
        message["To"] = recipient
        message["Date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Add body (supports both plain text and HTML)
        text_part = MIMEText(body, "plain")
        message.attach(text_part)

        # Optional: Add HTML version with styling
        if is_success:
            html_body = f"""
            <html>
              <body style="font-family: Arial, sans-serif;">
                <h2 style="color: #28a745;">✓ ETL Pipeline Success</h2>
                <p>{body.replace(chr(10), "<br>")}</p>
                <hr>
                <small>Timestamp: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</small>
              </body>
            </html>
            """
        else:
            html_body = f"""
            <html>
              <body style="font-family: Arial, sans-serif;">
                <h2 style="color: #dc3545;">✗ ETL Pipeline Failure</h2>
                <p>{body.replace(chr(10), "<br>")}</p>
                <hr>
                <small>Timestamp: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</small>
              </body>
            </html>
            """

        html_part = MIMEText(html_body, "html")
        message.attach(html_part)

        # Connect to SMTP server and send email
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()  # Secure the connection
            server.login(sender_email, sender_password)
            server.send_message(message)

        logger.info(f"Email notification sent successfully to {recipient}")
        return True

    except smtplib.SMTPAuthenticationError as e:
        logger.error(f"Email authentication failed: {e}")
        return False
    except smtplib.SMTPException as e:
        logger.error(f"SMTP error occurred: {e}")
        return False
    except Exception as e:
        logger.error(f"Failed to send email notification: {e}")
        return False


def send_success_notification(rows_inserted: int, execution_time: float) -> bool:
    """Send success notification with pipeline statistics.

    Args:
        rows_inserted: Number of rows successfully inserted into database.
        execution_time: Pipeline execution time in seconds.

    Returns:
        True if email sent successfully, False otherwise.

    Example:
        >>> send_success_notification(rows_inserted=150, execution_time=12.5)
    """
    subject = "ETL Pipeline - Execution Successful"
    body = f"""
ETL Pipeline executed successfully.

Summary:
- Rows inserted: {rows_inserted}
- Execution time: {execution_time:.2f} seconds
- Status: SUCCESS

No action required.
    """.strip()

    return send_etl_notification(subject, body, is_success=True)


def send_failure_notification(error_message: str, stage: str = "Unknown") -> bool:
    """Send failure notification with error details.

    Args:
        error_message: Detailed error message explaining the failure.
        stage: Pipeline stage where failure occurred (Extraction, Transformation, Loading).

    Returns:
        True if email sent successfully, False otherwise.

    Example:
        >>> send_failure_notification(
        ...     error_message="Database connection timeout",
        ...     stage="Loading"
        ... )
    """
    subject = f"ETL Pipeline - Execution Failed ({stage})"
    body = f"""
ETL Pipeline execution FAILED.

Failure Stage: {stage}
Error Message: {error_message}

Action Required:
- Review pipeline logs for detailed error trace
- Verify data source availability
- Check database connectivity and credentials

Timestamp: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    """.strip()

    return send_etl_notification(subject, body, is_success=False)
