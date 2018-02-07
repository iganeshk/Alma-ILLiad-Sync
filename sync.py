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
TARGET_FILE = ('ILLiad UserValidation.txt')  # filename in the report archive(zip)
OUTPUT_FILE = ('UserValidation.txt')  # output filename

LOGGING = True
BACKGROUND_ENABLED = True
SLEEP_INTERVAL = 900  # Seconds (BACKGROUND MUST BE ENABLED!)

# EMAIL (IMAP & SMTP) Login Credentials
EMAIL_USER = ""
EMAIL_PASS = ""
IMAP_SERVER = "outlook.office365.com"
IMAP_PORT = 993
SMTP_SERVER = "smtp-mail.outlook.com"
SMTP_PORT = 587
EMAIL_SENDER = ""  # e-mail address of the incoming reports
EMAIL_AGE_LIMIT = 5     # message age cut-off (days)
# destination FTP Credentials
FTP_SERVER = ''
FTP_PORT = 21
FTP_USERNAME = ''
FTP_PASSWORD = ''
FTP_DIRECTORY = ''

# ILLiad import note: carriage-Return, line-feed(CRLF) \r\n on line end
# hardcoded ILLiad Validation text Identifier
illiad_header = (
    'separator=,\r\nUserName, UserValidationType, LastName, FirstName, SSN, Status, EMailAddress'
    ', Phone, MobilePhone,Department, NVTGC, Password, NotificationMethod, DeliveryMethod, LoanDeliveryMethod,'
    ' AuthorizedUsers, Web, Address, Address2, City, State, Zip, Site, Number, Organization, Fax, '
    'ArticleBillingCategory, LoanBillingCategory, Country, SAddress, SAddress2, SCity, SState, SZip, PasswordHint,'
    ' SCountry, Blocked, PlainTextPassword, UserRequestLimit, UserInfo1, UserInfo2, UserInfo3, UserInfo4, UserInfo5\r\n'
    )

new_mail = False
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


def parse_alma_data(target_path):
    conv_success = False

    # CSV Approach for Alma Analytics Report (.txt)
    with codecs.open(target_path + "/" + OUTPUT_FILE, 'wb+', encoding='utf-8') as illiad_file:        # utf16/8/cp1252 output
        illiad_file.write(illiad_header)
        with open(target_path + "/" + TARGET_FILE, newline='', encoding='utf-16-le') as alma_input:   # Alma report(utf-16-le)
            alma_data = csv.reader(alma_input, dialect="excel-tab")
            for index, line in enumerate(alma_data):
                # discard anomalies from input (illiad import case)
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


def upload_ftp(target_path):
    logprint("[ftp-info] establishing connection to: %s" % FTP_SERVER)
    session = ftplib.FTP()
    try:
        session.connect(FTP_SERVER, FTP_PORT)
        session.login(FTP_USERNAME, FTP_PASSWORD)
        logprint("[ftp-info] connected and logged into FTP server")
        session.cwd(FTP_DIRECTORY)                                  # change the directory
        logprint("[ftp-info] uploading file....")
        file = open(target_path, 'rb')         # file to upload
        session.storbinary('STOR %s' % OUTPUT_FILE, file)           # upload the file
        file.close()                                                # close file and FTP
        logprint("[ftp-info] file successfully uploaded")
    except ftplib.all_errors as e:
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


def get_mail(target_path):
    global new_mail
    att_path = ""
    m = imaplib.IMAP4_SSL(IMAP_SERVER, IMAP_PORT)
    try:
        logprint("[mail-info] logging into mail")
        m.login(EMAIL_USER, EMAIL_PASS)
        m.select('Inbox')
        (result, messages) = m.search(None, ('UNSEEN'), '(FROM {0})'.format(EMAIL_SENDER.strip()), '(SUBJECT "ILLiad UserValidation")')
        if result == "OK":
            if len(messages[0].split()) > 0:
                logprint("[mail-info] total new %s mail(s)" % len(messages[0].split()))
            for message_index, message in enumerate(messages[0].split()):
                try:
                    resp, data = m.fetch(message, '(RFC822)')
                except Exception as e:
                    logprint("[mail-error] unable to load mail, %s" % e)
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
                            if not os.path.isdir(target_path):
                                os.mkdir(target_path)
                            att_path = os.path.join(target_path, filename)
                            try:
                                fp = open(att_path, 'wb')
                                fp.write(part.get_payload(decode=True))
                                fp.close()
                                extractAll(att_path, target_path)
                                new_mail = True
                                logprint('[mail-info] attachment: %s' % att_path)
                            except Exception as e:
                                logprint("%s" % e)
                                att_path = "[mail-error] unable to extract attachment"
    except KeyboardInterrupt:
        logprint("[mail-error] login interrupted")
        os._exit(0)
    except Exception as e:
        logprint("[mail-error] %s" % e)
        os._exit(0)
    # logprint("[mail] terminating imap connection")
    m.shutdown()
    return att_path


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
        logprint("[mail-error] unknown exception error occurred while sending email\n%s" % e)


def send_notification(status):
    # Read email list
    email_list = [line.strip() for line in open('./email.txt')]

    for to_addrs in email_list:
        msg = MIMEMultipart("alternative")

        msg['Subject'] = "Alma - ILLiad Sync Notification"
        msg['From'] = from_addr
        msg['To'] = to_addrs
        if status == "success":
            html = open('./success.html', 'rb').read()
        else:
            html = open('./failed.html', "rb").read()
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

    if not (EMAIL_USER and EMAIL_PASS and IMAP_SERVER and IMAP_PORT):
        print("\n[error] email credentials or server parameters empty!\n")
        os._exit(0)

    if not (FTP_SERVER and FTP_PORT and FTP_USERNAME and FTP_PASSWORD):
        print("\n[error] ftp server credentials or parameters empty!\n")
        os._exit(0)

    if LOGGING:
        if not os.path.exists("./logs"):
            os.mkdir("./logs")
        log_file = open("./logs/sync-log_" + datetime.datetime.now().strftime("%Y-%m-%d_%H-%M") + ".txt", "w")
        sys.stdout = Logger(log_file, sys.stdout)

    logprint("[info] initiating...")
    while True:
            # implement socket here
            date_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
            DOWNLOAD_FOLDER = os.path.join(os.path.dirname(os.path.realpath(__file__)), date_time)
            TARGET_PATH = get_mail(DOWNLOAD_FOLDER)    # attachment file target path
            if os.path.isfile(TARGET_PATH):
                # convert the data
                parse_alma_data(DOWNLOAD_FOLDER)
                # upload to ftp server
                upload_ftp(os.path.join(DOWNLOAD_FOLDER, OUTPUT_FILE))
                # send email notifications
                send_notification("success")
                # unset new email flag
                new_mail = False
                logprint("[info] process completed")
            elif TARGET_PATH == "":
                logprint("[info] no new mail!")
            else:
                logprint(TARGET_PATH)
            if BACKGROUND_ENABLED:
                try:
                    time.sleep(SLEEP_INTERVAL)
                except KeyboardInterrupt:
                    logprint("[debug] interrupted")
                    os._exit(0)
            else:
                break
