import smtplib
from email.message import EmailMessage
import os
from datetime import date,datetime
from dotenv import load_dotenv
import pandas as pd
from io import BytesIO


from src.analysis  import analysis


today= date.today()


load_dotenv(".env", override=True)

def send_email(results,bytes_excel):

   

    # SMTP config is provider-agnostic: host/port/credentials come from env.
    # Defaults keep Gmail working out of the box, and the GMAIL_* vars are still
    # honored as a fallback so existing setups don't break.

    smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", "465"))
    smtp_user = os.getenv("SMTP_USER") or os.getenv("GMAIL_USER")
    smtp_password = os.getenv("SMTP_PASSWORD") or os.getenv("GMAIL_APP_PASSWORD")

    body = f"""\
    Hello,

    Please find attached the RBA Analysis Report for {today}.

    The report includes: overview KPIs, attacks by hour, top countries, 
    device profiling, most targeted users, and RTT comparison.

    Best regards
    """

    msg = EmailMessage()
    msg["Subject"] = f"REPORT: Rba analysis of {today}"
    msg["From"] = smtp_user
    msg["To"] = smtp_user
    msg.set_content(body)
    
    msg.add_attachment(
        bytes_excel,
        maintype="application",
        subtype="xlsx",
        filename=f"rba_analysis_{today}.xlsx"
    )

    try:
        # Port 465 uses implicit SSL (Gmail); other ports (e.g. 587 for
        # Outlook/Office365) use STARTTLS over a plain connection.
        if smtp_port == 465:
            with smtplib.SMTP_SSL(smtp_host, smtp_port) as s:
                s.login(smtp_user, smtp_password)
                s.send_message(msg)
        else:
            with smtplib.SMTP(smtp_host, smtp_port) as s:
                s.starttls()
                s.login(smtp_user, smtp_password)
                s.send_message(msg)
    except Exception as e:
        print(f"Email failed: {e}", flush=True)