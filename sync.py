#!/usr/bin/env/ python3
# coding=utf-8
#
# Sync Alma - ILLiad Database
# Developer: Ganesh Anand Velu
# Version: 0.7 (03092018)
#
# Description: Downloads email report sent from Alma and extracts the document(.txt report). Then,
# creates a new document with UTF-8 encoding, parses the hard-coded UserValidation lines and
# and the report itself. And finally, uploads the generated documented to the specified FTP server.
# Note: Entries with barcodes having "-" are discarded due to import error w/ ILLiad.  (line: 91-108)
#       Usage: python3 sync.py

import sys, os, codecs, datetime, time, logging, signal
import argparse
import csv
from configparser import ConfigParser, NoOptionError, NoSectionError, DuplicateSectionError
import imaplib, email, ftplib, smtplib
from zipfile import ZipFile as zip
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

if sys.version_info <= (3, 0):
    sys.stdout.write("Sorry, requires Python 3.x, not Python 2.x\n")
    sys.exit(1)

# Input and Output file declarationn
TARGET_FILE = (
    'ILLiad UserValidation.txt')  # filename in the report archive(zip)
OUTPUT_FILE = ('UserValidation.txt')  # output filename

LOGGING = True
BACKGROUND_ENABLED = True
SLEEP_INTERVAL = 5  # seconds (BACKGROUND MUST BE ENABLED!)

# EMAIL (IMAP & SMTP) Login Credentials
EMAIL_USER = ""
EMAIL_PASS = ""
IMAP_SERVER = ""
IMAP_PORT = 993
SMTP_SERVER = ""
SMTP_PORT = 587
# Alma Report Dispatcher's email address
EMAIL_SENDER = "Your.Department@organization.com"  # e-mail address of the incoming reports
EMAIL_AGE_LIMIT = 5  # email expiry (days)

# Payload destination FTP credentials
FTP_SERVER = ''
FTP_PORT = 21
FTP_USERNAME = ''
FTP_PASSWORD = ''
FTP_DIRECTORY = '/illiad/import/'

# ILLiad File Import Notes: Carriage-Return, Line-feed \r\n (CRLF)
# hardcoded ILLiad Validation text Identifier
illiad_header = (
    'separator=,\r\nUserName, UserValidationType, LastName, FirstName, SSN, Status, EMailAddress'
    ', Phone, MobilePhone,Department, NVTGC, Password, NotificationMethod, DeliveryMethod, LoanDeliveryMethod,'
    ' AuthorizedUsers, Web, Address, Address2, City, State, Zip, Site, Number, Organization, Fax, '
    'ArticleBillingCategory, LoanBillingCategory, Country, SAddress, SAddress2, SCity, SState, SZip, PasswordHint,'
    ' SCountry, Blocked, PlainTextPassword, UserRequestLimit, UserInfo1, UserInfo2, UserInfo3, UserInfo4, UserInfo5\r\n'
)
cg = ConfigParser()
parser = argparse.ArgumentParser()
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

    # CSV approach for Alma analytics report (.txt)
    with codecs.open(
            target_path + "/" + OUTPUT_FILE, 'wb+',
            encoding='utf-8') as illiad_file:  # utf16/8/cp1252 output
        illiad_file.write(illiad_header)
        with open(
                target_path + "/" + TARGET_FILE, newline='',
                encoding='utf-16-le') as alma_input:  # Alma report(utf-16-le)
            alma_data = csv.reader(alma_input, dialect="excel-tab")
            for index, line in enumerate(alma_data):
                error_strings = "Line " + (str(index + 1))
                # discard anomalies from input (illiad import case)
                if "-" in line[1]:  # barcode anomaly (-)
                    error_strings = error_strings + " [Barcode: " + line[1] + "]"
                    continue
                if "," in line[3]:  # last name anomaly (,)
                    error_strings = error_strings + " [Last Name: " + line[3] + "]"
                    anamoly = line[3].find(",")
                    if (len(line[3]) - anamoly) == 1:
                        line[3] = line[3].replace(",", "")
                    elif line[3][anamoly + 1] == " ":
                        line[3] = line[3].replace(",", "")
                    else:
                        line[3] = line[3].replace(",", " ")
                if "," in line[4]:  # first name anomaly (,)
                    error_strings = error_strings + " [First Name: " + line[4] + "]"
                    anamoly = line[4].find(",")
                    if (len(line[4]) - anamoly) == 1:
                        line[4] = line[4].replace(",", "")
                    elif line[4][anamoly + 1] == " ":
                        line[4] = line[4].replace(",", "")
                    else:
                        line[4] = line[4].replace(",", " ")
                # anomaly detector ends here
                if line[0] == "Barcode":
                    lastname = line[3].lower(
                    )  # convert the last name to lowercase (hard coded output)
                    illiad_file.write(
                        "{},Auth,{},{},{},{},,,,,ILL,,E-Mail,Hold for Pickup,Hold for Pickup,,,,,,,,,,,,,,,,,,,,Your last name,,,{},,,,,,\r\n"
                        .format(line[1], line[3], line[4], line[5], line[6],
                                lastname))
                    conv_success = True
                else:
                    if line[0] == "Identifier":
                        print("Bad data at index: " + (str(index + 1)))
                if error_strings != "Line " + (str(index + 1)):
                    with open(target_path + "/" + "errors.txt",
                              "a") as error_dump:
                        error_dump.write(error_strings + "\r\n")
            if conv_success:
                logprint("[info]","processed total of " + str(
                    (index)) + " entries.")
                logprint("[info]","translation completed!")


def upload_ftp(target_path):
    logprint("[ftp-info]","establishing connection to: %s" % FTP_SERVER)
    session = ftplib.FTP()
    try:
        session.connect(FTP_SERVER, FTP_PORT)
        session.login(FTP_USERNAME, FTP_PASSWORD)
        logprint("[ftp-info]","connected and logged into FTP server")
        session.cwd(FTP_DIRECTORY)  # change the directory
        logprint("[ftp-info]","uploading file....")
        file = open(target_path, 'rb')  # file to upload
        session.storbinary('STOR %s' % OUTPUT_FILE, file)  # upload the file
        file.close()  # close file and FTP
        logprint("[ftp-info]","file successfully uploaded")
    except ftplib.all_errors as e:
        # print(str(e).split(None, 1)[0])                           # get only error code
        logprint("[ftp-error]","FTP: %s" % e)  # display the error
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
        logprint("[mail-info]","logging into mail")
        m.login(EMAIL_USER, EMAIL_PASS)
        m.select('Inbox')
        (result, messages) = m.search(None, ('UNSEEN'), '(FROM {0})'.format(
            EMAIL_SENDER.strip()), '(SUBJECT "ILLiad UserValidation")')
        if result == "OK":
            if len(messages[0].split()) > 0:
                logprint("[mail-info]","total new %s mail(s)" % len(
                    messages[0].split()))
            for message_index, message in enumerate(messages[0].split()):
                try:
                    resp, data = m.fetch(message, '(RFC822)')
                except Exception as e:
                    logprint("[mail-error]","unable to load mail, %s" % e)
                    m.close()
                    exit()
                msg = email.message_from_bytes(data[0][1])

                # check mail's age (time difference)
                date_tuple = email.utils.parsedate_tz(msg['Date'])
                if date_tuple:
                    local_mail_date = datetime.datetime.fromtimestamp(
                        email.utils.mktime_tz(date_tuple))
                    time_diff = datetime.datetime.now() - local_mail_date
                    if time_diff.days > EMAIL_AGE_LIMIT:
                        logprint(
                            "[mail-info] skipping email %s received more than %s days ago ( %s )"
                            % (message_index + 1, EMAIL_AGE_LIMIT,
                               local_mail_date.strftime("%Y-%m-%d %H:%M")))
                        continue

                for part in msg.walk():
                    if part.get_content_maintype() == 'multipart':
                        continue
                    if part.get('Content-Disposition') is None:
                        continue
                    filename = part.get_filename()
                    if "zip" in filename:
                        # set pid flag
                        if not os.path.isdir(target_path):
                            os.mkdir(target_path)
                        att_path = os.path.join(target_path, filename)
                        with open('sync.pid', 'w') as pidfile:
                            cg.set('sync', 'clean_exit', 'false')
                            cg.set('sync', 'file', '%s' % att_path)
                            cg.set('sync', 'path', '%s' % target_path)
                            cg.write(pidfile)
                        try:
                            fp = open(att_path, 'wb')
                            fp.write(part.get_payload(decode=True))
                            fp.close()
                            extractAll(att_path, target_path)
                            new_mail = True
                            logprint('[mail-info]','attachment: %s' % att_path)
                        except Exception as e:
                            logprint('[mail-info]',"%s" % e)
                            att_path = "unable to extract attachment"
    except KeyboardInterrupt:
        logprint("[mail-error]","login interrupted")
        os._exit(0)
    except Exception as e:
        logprint("[mail-error]","%s" % e)
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
        logprint(
            "[mail-error]", "unknown exception error occurred while sending email\n%s"
            % e)


def send_notification(status, log_time, target_path):
    # read email list
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
            logs = MIMEApplication(open('./logs/sync-log_' + log_time))
        # Attach HTML to the email
        body = MIMEText(html, 'html', 'UTF-8')
        msg.attach(body)
        if os.path.isfile(target_path + "/errors.txt"):
            error_list = MIMEApplication(
                open(target_path + "/errors.txt", "rb").read())
            error_list.add_header(
                'Content-Disposition', 'attachment', filename="error_log.txt")
            msg.attach(error_list)
            msg['Subject'] = "Alma - ILLiad Sync Notification"
        try:
            send_mail(from_addr, to_addrs, msg)
            logprint("[mail-info]","email sent to " + to_addrs)
        except SMTPAuthenticationError as e:
            logprint("[mail-error]", e)


def logprint(code, stdout):
    # print(
    #     datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\t" + stdout)
    print("%-*s %-*s %s" %
          (23, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 15, code,
           stdout))


def sync_process(att_file_path, down_folder):
    global new_mail
    if os.path.isfile(att_file_path):
        # convert the data
        parse_alma_data(down_folder)
        # upload to ftp server
        #upload_ftp(os.path.join(down_folder, OUTPUT_FILE))
        # send email notifications
        #send_notification("success", date_time, down_folder)
        # unset new email flag
        with open('sync.pid', 'w') as pidfile:
            cg.set('sync', 'clean_exit', 'true')
            cg.write(pidfile)
        new_mail = False
        logprint("[info]", "process completed")
    elif att_file_path == "":
        logprint("[info]", "no new mail!")
        # heartbeat - BG TIME
        with open('sync.pid', 'w') as pidfile:
            cg.set('sync', 'PID', '%s' % str(os.getpid()))
            cg.write(pidfile)
    else:
        logprint("[mail-error]", att_file_path)


def process_args():
    # parser.add_argument('--pid-file', help='PID file path. Default: Current Directory')
    parser.add_argument('--daemon', help='Run in daemon mode', action='store_true')
    parser.add_argument('--logging', help='Run in daemon mode', action='store_true')
    parser.add_argument(
        '--stop', help='Shutdown the current process', action='store_true')
    args = parser.parse_args()
    if(args.stop):
        print("trying to kill process %s" %(args.daemon))
        os.kill(os.getpid(), signal.SIGTERM)
    # initialize pid file
    try:
        cg.read("sync.pid")
        if (not(cg.getboolean('sync', 'clean_exit'))):
            logprint(
                "[warn]",
                "previous run did not exit clean, attempting to process again")
            # finish it before proceeding
            sync_process(str(cg.get('sync', 'file')), str(cg.get('sync', 'path')))
    except NoOptionError:
        # move on
        pass
    except NoSectionError:
        pass
    with open('sync.pid', 'w') as pidfile:
        try:
            cg.add_section('sync')
        except DuplicateSectionError:
            pass
        cg.set('sync', 'PID', '%s' % str(os.getpid()))
        cg.set('sync', 'clean_exit', 'true')
        cg.write(pidfile)


if __name__ == '__main__':

    # Read & Process arguments
    process_args()

    if not (EMAIL_USER and EMAIL_PASS and IMAP_SERVER and IMAP_PORT):
        logprint("\n[error]","email credentials or server parameters empty!\n")
        os._exit(0)

    if not (FTP_SERVER and FTP_PORT and FTP_USERNAME and FTP_PASSWORD):
        logprint("\n[error]","ftp server credentials or parameters empty!\n")
        os._exit(0)

    if LOGGING:
        if not os.path.exists("./logs"):
            os.mkdir("./logs")
        log_file = open(
            "./logs/sync-log_" +
            datetime.datetime.now().strftime("%Y-%m-%d_%H-%M") + ".txt", "w")
        sys.stdout = Logger(log_file, sys.stdout)

    logprint("[info]","initiating...")

    while True:
        # Before write implement read and restore state
        # implement socket here

        date_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
        DOWNLOAD_FOLDER = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), date_time)
        TARGET_PATH = get_mail(DOWNLOAD_FOLDER)  # attachment file target path
        sync_process(TARGET_PATH, DOWNLOAD_FOLDER)
        if (BACKGROUND_ENABLED or cg.daemon):
            try:
                time.sleep(SLEEP_INTERVAL)
            except KeyboardInterrupt:
                logprint("[debug]", "interrupted")
                os._exit(0)
        else:
            break
