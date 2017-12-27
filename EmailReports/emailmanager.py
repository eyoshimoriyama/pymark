import smtplib
from local_config import *
from smtplib import SMTP
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import Encoders
from datetime import datetime

class EmailManager():

	def __init__(self):
		self.emailfrom = email_from
		self.password = email_password

	def sendEmail(self, recepients, subject, body, file_name):
		self.file_name = file_name
		self.createMsg(recepients, subject, body)
		self.addAttachment()
		self.establishServer()
		self.mailserver.sendmail(self.emailfrom, recepients, self.msg.as_string())

	def createMsg(self, recepients, subject, body):
		self.msg = MIMEMultipart()
		self.msg["From"] = self.emailfrom
		self.msg["To"] = ", ".join(recepients)
		self.msg["Subject"] = subject
		self.msg.attach(MIMEText(body, 'plain'))
	
	def addAttachment(self):
		file_content = open(self.file_name, "rb").read()
		part = MIMEBase("application", "octet-stream")
		part.set_payload(file_content)
		Encoders.encode_base64(part)
		part.add_header("Content-Disposition", "attachment; filename=" + self.file_name.split('/')[-1])
		self.msg.attach(part)

	def establishServer(self):
		self.mailserver = smtplib.SMTP('smtp.gmail.com',587)
		self.mailserver.ehlo()
		self.mailserver.starttls()
		self.mailserver.ehlo()
		self.mailserver.login(self.emailfrom, self.password)
