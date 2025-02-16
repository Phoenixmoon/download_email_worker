import os
import email
import time
import re
from email.utils import parsedate_tz, mktime_tz


class Eml:
    """
    subject [str]: the subject line of the email
    sender [str]
    receiver [str]
    date [int]: date email was sent in UNIX time
    text [str]: the body of the email. also includes subject, sender and receiver
    """

    def __init__(self, subject, sender, receiver, date, text):
        self.subject = subject
        self.sender = sender
        self.receiver = receiver
        self.date = date
        self.text = text

    def cleanse_eml(self):
        """
        Removes any urls and images from the email
        """
        url_pattern = r"(https?|ftp)://\S+|www\.\S+|[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(/\S*)?"
        new_txt = re.sub(url_pattern, "", self.text)
        cleaned_text = re.sub(r'(\[image:.*?\]|<image.*?>|<img.*?>)', '', new_txt, flags=re.IGNORECASE | re.DOTALL)
        self.text = cleaned_text


def convert_date_to_unix(date_string):
    """
    Converts a date string in RFC 2822/5322 format to Unix time.

    Inputs:
        date_string [str]: The date string to convert.

    Returns [int]: the Unix timestamp corresponding to the date string.
    """

    parsed_date = parsedate_tz(date_string)
    if parsed_date is None:
        raise ValueError("Invalid date string format")

    unix_time = mktime_tz(parsed_date)
    return int(unix_time)


def read_eml(file_path):
    """
    Opens and reads an email and creates the eml object.

    Inputs:
        file_path [str]: file path to the email

    Returns [Eml]: the eml object
    """

    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"The file {file_path} does not exist.")

    with open(file_path, 'rb') as f:  # Open in binary mode
        msg = email.message_from_bytes(f.read())  # parse eml

    text = ["Subject: " + msg.get("Subject", "No Subject"), "From: " + msg.get("From", "No Sender"),
            "To: " + msg.get("To", "No Recipient")]

    date = msg.get('Date')

    if msg.is_multipart():
        for part in msg.walk():
            # if part text/plain or text/html, get the body
            if part.get_content_type() == "text/plain":
                body = part.get_payload(decode=True).decode(part.get_content_charset(), errors='replace')
                text.append("Body:\n" + body)
                break
    else:
        # Not multipart, just get the payload
        body = msg.get_payload(decode=True).decode(msg.get_content_charset(), errors='replace')
        text.append("Body:\n" + body)

    res = Eml(msg.get("Subject", "No Subject"), msg.get("From", "No Sender"), msg.get("To", "No Recipient"),
              convert_date_to_unix(date), "\n".join(text))

    return res
