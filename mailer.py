import smtplib
import pandas as pd
#from email.mime.multipart import MIMEMultipart
import datetime
from email.mime.multipart import MIMEMultipart
# from email.MIMEBase import MIMEBase
# #from mailer_code import mailer
# from email import Encoders
import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (Mail, Attachment, FileContent, FileName, FileType, Disposition)
import base64

def mailer(body,recipients,Subject="Password reset link"):

    message = Mail(
    from_email='contact@tecogno.com',
    to_emails=recipients,
    subject=Subject,
    html_content=body)
    try:
        sg = SendGridAPIClient("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")
        response = sg.send(message)
        print(response.status_code)
        print(response.body)
        print(response.headers)
    except Exception as e:
        print(e.message)

def mailer_with_attachment(body,recipients,Subject,filename,filename2=""):

    message = Mail(
    from_email='contact@tecogno.com',
    to_emails=recipients,
    subject=Subject,
    html_content=body)
    with open(filename, 'rb') as f:
        data = f.read()
        f.close()
    encoded_file = base64.b64encode(data).decode()
    attachedFile = Attachment(
        FileContent(encoded_file),
        FileName(filename),
        FileType('application/csv'),
        Disposition('attachment'))
    message.attachment = attachedFile
    if len(filename2) != 0:
        with open(filename2, 'rb') as f:
            data = f.read()
            f.close()
        encoded_file = base64.b64encode(data).decode()
        attachedFile = Attachment(
            FileContent(encoded_file),
            FileName(filename2),
            FileType('application/csv'),
            Disposition('attachment'))
        message.attachment = attachedFile
    try:
        sg = SendGridAPIClient("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")
        response = sg.send(message)
        print(response.status_code)
        print(response.body)
        print(response.headers)
    except Exception as e:
        print(e.message)