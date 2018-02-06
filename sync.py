#!/usr/bin/env/ python3
# coding=utf-8
#
# Sync Alma - ILLiad Database
# Developer: Ganesh Anand Velu
# Version: 0.5
#
# Description: Downloads email report sent from Alma and extracts the document(.txt report). Then,
# creates a new document with UTF-8 encoding, parses the hard-coded UserValidation lines and
# and the report itself. And finally, uploads the generated documented to the specified FTP server.
# Note: Entries with barcodes having "-" are discarded due to import error w/ ILLiad.  (line: 91-108)
#       Usage: python3 sync.py

import sys, os, codecs, datetime, time, logging
import csv
import imaplib, email, ftplib, smtplib
from zipfile import ZipFile as zip
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

# Input and Output File Declaration
TARGET_FILE = ('ILLiad UserValidation.txt')  # File in the report archive(zip)
OUTPUT_FILE = ('UserValidation.txt')  # Desired File

LOGGING = True
BACKGROUND_ENABLED = True
SLEEP_INTERVAL = 900  # Seconds (BACKGROUND MUST BE ENABLED!)

# IMAP & SMTP Login Credentials
EMAIL_USER = ""
EMAIL_PASS = ""
IMAP_SERVER = "outlook.office365.com"
IMAP_PORT = 993
SMTP_SERVER = "smtp-mail.outlook.com"
SMTP_PORT = 587
EMAIL_SENDER = ""  # E-mail address of the incoming reports
EMAIL_AGE_LIMIT = 5     # Email message age cut-off (days)
# Destination FTP Credentials
FTP_SERVER = ''
FTP_PORT = 21
FTP_USERNAME = ''
FTP_PASSWORD = ''
FTP_DIRECTORY = ''

# ILLiad File Import Notes: Carriage-Return, Line-feed \r\n (CRLF)
# Hardcoded ILLiad Validation text Identifier
illiad_header = (
    'separator=,\r\nUserName, UserValidationType, LastName, FirstName, SSN, Status, EMailAddress'
    ', Phone, MobilePhone,Department, NVTGC, Password, NotificationMethod, DeliveryMethod, LoanDeliveryMethod,'
    ' AuthorizedUsers, Web, Address, Address2, City, State, Zip, Site, Number, Organization, Fax, '
    'ArticleBillingCategory, LoanBillingCategory, Country, SAddress, SAddress2, SCity, SState, SZip, PasswordHint,'
    ' SCountry, Blocked, PlainTextPassword, UserRequestLimit, UserInfo1, UserInfo2, UserInfo3, UserInfo4, UserInfo5\r\n'
    )
# E-mail attachment download folder
DOWNLOAD_FOLDER = os.getcwd()
# Folder to upload file from
UPLOAD_FOLDER = os.getcwd()

new_mail = False
BIN_FOLDER = os.path.dirname(os.path.realpath(__file__))
from_addr = EMAIL_USER


class Logger:
    def write(self, *args, **kwargs):
        self.out1.write(*args, **kwargs)
        self.out2.write(*args, **kwargs)
        self.out1.flush()
        self.out2.flush()

    def flush(self):
        pass

    def __init__(self, out1, out2):
        self.out1 = out1
        self.out2 = out2


def convert_alma_illiad():
    conv_success = False

    # CSV Approach for Alma Analytics Report (.txt)
    with codecs.open(UPLOAD_FOLDER + "/" + OUTPUT_FILE, 'wb+', encoding='utf-8') as illiad_file:        # utf16/8/cp1252 output
        illiad_file.write(illiad_header)
        with open(UPLOAD_FOLDER + "/" + TARGET_FILE, newline='', encoding='utf-16-le') as alma_input:   # Alma report(utf-16-le)
            alma_data = csv.reader(alma_input, dialect="excel-tab")
            # alma_data = csv.reader(alma_input, delimiter='\t')
            for index, line in enumerate(alma_data):
                # anomaly detector (illiad import detects error)
                if "-" in line[1]:                              # barcode anomaly (-)
                    continue
                if "," in line[2]:                              # last name anomaly (,)
                    anamoly = line[2].find(",")
                    if (len(line[2]) - anamoly) == 1:
                        line[2] = line[2].replace(",", "")
                    elif line[2][anamoly+1] == " ":
                        line[2] = line[2].replace(",", "")
                    else:
                        line[2] = line[2].replace(",", " ")
                if "," in line[3]:                              # first name anomaly (,)
                    anamoly = line[3].find(",")
                    if (len(line[3]) - anamoly) == 1:
                        line[3] = line[3].replace(",", "")
                    elif line[3][anamoly+1] == " ":
                        line[3] = line[3].replace(",", "")
                    else:
                        line[3] = line[3].replace(",", " ")
                # anomaly detector ends here
                if line[0] == "Barcode":
                    lastname = line[2].lower()                          # convert the last name to lowercase (hard coded output)
                    illiad_file.write(
                        "{},Auth,{},{},{},{},,,,,,ILL,,E-Mail,Hold for Pickup,Hold for Pickup,,,,,,,,,,,,,,,,,,,,Your last name,,,{},,,,,,\r\n"
                        .format(line[1], line[2], line[3], line[4], line[5], lastname))
                    conv_success = True
                else:
                    if line[0] == "Identifier":
                        print("Bad data at index: "+(str(index+1)))
            if conv_success:
                logprint("[info] processed total of " + str((index)) + " entries.")
                logprint("[info] translation completed!")


def upload_ftp():
    logprint("[ftp-info] establishing connection to: %s" % FTP_SERVER)
    session = ftplib.FTP()
    try:
        session.connect(FTP_SERVER, FTP_PORT)
        session.login(FTP_USERNAME, FTP_PASSWORD)
        logprint("[ftp-info] connected and logged into FTP server")
        session.cwd(FTP_DIRECTORY)                                  # change the directory
        logprint("[ftp-info] uploading file....")
        file = open(UPLOAD_FOLDER + "/" + OUTPUT_FILE, 'rb')         # file to upload
        session.storbinary('STOR %s' % OUTPUT_FILE, file)           # upload the file
        file.close()                                                # close file and FTP
        logprint("[ftp-info] transfer complete!")
    except ftplib.all_errors as e:
        # print(str(e).split(None, 1)[0])                           # get only error code
        logprint("[ftp-error] FTP: %s" % e)                         # display the error
    session.quit()


def extractAll(zipName, dir):
    z = zip(zipName)
    for f in z.namelist():
        if f.endswith('/'):
            os.makedirs(f)
        else:
            if os.path.exists(dir):
                z.extract(f, dir)
            else:
                z.extract(f)


def get_mail():
    global new_mail
    global UPLOAD_FOLDER

    try:
        m = imaplib.IMAP4_SSL(IMAP_SERVER, IMAP_PORT)
        m.login(EMAIL_USER, EMAIL_PASS)
        m.select('Inbox')
        att_path = "[mail-info] no attachment found!"
        (result, messages) = m.search(None, ('UNSEEN'), '(FROM {0})'.format(EMAIL_SENDER.strip()), '(SUBJECT "ILLiad UserValidation")')
        if result == "OK":
            if len(messages[0].split()) > 0:
                logprint("[mail-info] total new mail(s) %s" % len(messages[0].split()))
            for message_index, message in enumerate(messages[0].split()):
                try:
                    resp, data = m.fetch(message, '(RFC822)')
                except Exception as e:
                    logprint("[mail-error] unable to load mail, %s", % e)
                    m.close()
                    exit()
                msg = email.message_from_bytes(data[0][1])

                # check mail's age (time difference)
                date_tuple = email.utils.parsedate_tz(msg['Date'])
                if date_tuple:
                    local_mail_date = datetime.datetime.fromtimestamp(email.utils.mktime_tz(date_tuple))
                    time_diff = datetime.datetime.now() - local_mail_date
                    if time_diff.days > EMAIL_AGE_LIMIT:
                        logprint("[mail-info] skipping email %s received more than %s days ago ( %s )" % (message_index+1
                            , EMAIL_AGE_LIMIT, local_mail_date.strftime("%Y-%m-%d %H:%M")))
                        continue

                for part in msg.walk():
                    if part.get_content_maintype() == 'multipart':
                        continue
                    if part.get('Content-Disposition') is None:
                        continue
                    filename = part.get_filename()
                    if "zip" in filename:
                        try:
                            date_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
                            os.mkdir(DOWNLOAD_FOLDER + "/" + date_time)
                            UPLOAD_FOLDER = DOWNLOAD_FOLDER + "/" + date_time
                            att_path = os.path.join(UPLOAD_FOLDER, filename)
                            if os.path.isfile(att_path):  # delete older archives
                                os.remove(att_path)
                            # if not os.path.isfile(att_path):
                            fp = open(att_path, 'wb')
                            fp.write(part.get_payload(decode=True))
                            fp.close()
                            os.chdir(UPLOAD_FOLDER)
                            extractAll(filename)
                            os.chdir(os.path.dirname(os.path.realpath(__file__)))
                            new_mail = True
                            logprint('[mail-info] attachment: %s' % att_path)
                        except Exception as e:
                            logprint("[mail-error] cannot download attachment, %s" % e)
        m.quit()
    except KeyboardInterrupt:
        logprint("[mail-error] login interrupted")
        os._exit(0)
    except Exception as e:
        logprint("[mail-error] unknown exception occurred, %s" % e)
        os._exit(0)


def send_mail(from_addr, to_addrs, msg):
    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(from_addr, to_addrs, msg.as_string())
        server.quit()
    except Exception as e:
        logprint("[mail-error] unknown exception error occurred while sending email, %s", % e)


# implement error in the same function
def send_notification(status):
    # Read email list txt
    email_list = [line.strip() for line in open(BIN_FOLDER + '/email.txt')]

    for to_addrs in email_list:
        msg = MIMEMultipart("alternative")

        msg['Subject'] = "Alma - ILLiad Sync Notification"
        msg['From'] = from_addr
        msg['To'] = to_addrs
        if status == "success":
            html = open(BIN_FOLDER + "/success.html", "rb").read()
        else:
            html = open(BIN_FOLDER + "/failed.html", "rb").read()
        # Attach HTML to the email
        body = MIMEText(html, 'html', 'UTF-8')
        msg.attach(body)
        try:
            send_mail(from_addr, to_addrs, msg)
            logprint("[mail-info]  email successfully sent to " + to_addrs)
        except SMTPAuthenticationError as e:
            logprint("[mail-error]  %s" % e)


def logprint(stdout):
    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\t" + stdout)


if __name__ == '__main__':
    if LOGGING:
        if not os.path.exists("./logs"):
            os.mkdir("./logs")
        log_file = open("./logs/sync-log_" + datetime.datetime.now().strftime("%Y-%m-%d_%H-%M") + ".txt", "w")
        sys.stdout = Logger(log_file, sys.stdout)

    logprint("[info] initiating...")
    while True:
            logprint("[info] logging into email (IMAP)")
            # implement socket here

            get_mail()
            if new_mail:
                if os.path.isfile(TARGET_FILE):
                    # logprint("[info] time:\t\t" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M"))
                    # logprint("[info] input file:\t" + TARGET_FILE + " (" + datetime.datetime.fromtimestamp(os.path.getmtime(TARGET_FILE)).strftime("%Y-%m-%d %H:%M")+")")
                    # logprint("[info] output file:\t" + OUTPUT_FILE)
                    # backup files and move them appropriate folders
                    convert_alma_illiad()

                    # if conversion succeeded, upload to ftp server
                    upload_ftp()

                    new_mail = False
                    send_notification("success")

                    logprint("[info] process completed")
                else:
                    logprint("[error] invalid attachment in the mail!")
            else:
                logprint("[info] no new mail!")
            if BACKGROUND_ENABLED:
                try:
                    time.sleep(SLEEP_INTERVAL)
                except KeyboardInterrupt:
                    logprint("[debug] interrupted")
                    os._exit(0)
            else:
                break
