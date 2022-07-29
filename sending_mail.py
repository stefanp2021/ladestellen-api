
# Import needed packages
## Jetzt noch auto E-Mail verschicken
import smtplib #nur für versand, das email muss zuvor noch aufgebaut werden
import email.mime.text
from email.mime.text import MIMEText
from email.message import EmailMessage
#from email.mime.base import MIMEBase



import os, smtplib, traceback
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

#https://stackoverflow.com/questions/16968758/sending-email-to-a-microsoft-exchange-group-using-python
#

def sendMail(sender,
             subject,
             recipient,
             username,
             password,
             message=None,
             xlsx_files=None):

    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = sender
    if type(recipient) == list:
        msg["To"] = ", ".join(recipient)
    else:
        msg["To"] = recipient
    message_text = MIMEText(message, 'html')
    msg.attach(message_text)

    if xlsx_files:
        #for f in xlsx_files:
        #    attachment = open(f, 'rb')
        #    file_name = os.path.basename(f)
        #    part = MIMEApplication(attachment.read(), _subtype='xlsx')
        #    part.add_header('Content-Disposition', 'attachment', filename=file_name)
        #    msg.attach(part)
        attachment = open(xlsx_files, 'rb')
        #print(attachment)
        file_name = os.path.basename(xlsx_files)
        #print(file_name)
        part = MIMEApplication(attachment.read(), _subtype='xlsx')
        part.add_header('Content-Disposition', 'attachment', filename=file_name)
        msg.attach(part)
    try:
        #server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server = smtplib.SMTP("192.168.10.250",port=25) #SMTP Object bekommt server daten + Port
        server.connect("192.168.10.250",25)
        server.ehlo()
        server.starttls()
        server.ehlo()
        #server.login(username, password)
        server.sendmail(sender, recipient, msg.as_string())
        server.close()
    except Exception as e:
        error = traceback.format_exc()
        #print(error)
        #print(e)