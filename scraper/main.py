import argparse
import sys
import smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from scraper.scraper import scrape, scrape_debug

def send_mail(
    next_date,
    url,
    type,
    sender_email=None,
    password=None,
    receiver_email=None,
    smtp_server=None,
    smtp_port=None):
    server = None
    # Try to log in to server and send email
    try:
        server = smtplib.SMTP_SSL(smtp_server, smtp_port)
        server.set_debuglevel(3)
        server.connect(smtp_server)
        server.ehlo()
        server.login(sender_email, password)  # On s'authentifie
        # Create message
        message = ""
        message += f'Nouveau RDV de type {type}<a href="{url}">{next_date}</a>\n'
        print("Sending mail with content : "+message)

        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = receiver_email
        msg['Subject'] = f'Nouvelle date dispo pour {type} le {next_date}'
        msg.attach(MIMEText(message))
        server.sendmail(sender_email, receiver_email, msg.as_string())
    except Exception as e:
        # Print any error messages to stdout
        print(e)
    finally:
        if server:
            server.quit()


def main():  # pragma: no cover
    parser = argparse.ArgumentParser()
    parser.add_argument("--platform", "-p", help="scrape platform. (eg: doctolib,keldoc or all)")
    parser.add_argument("--url", "-u", action="append", help="scrape one url, can be repeated")
    parser.add_argument("--url-file", type=argparse.FileType("r"), help="scrape urls listed in file (one per line)")
    # email config
    parser.add_argument("-S", "--smtp_server", help='SMTP server', type=str)
    parser.add_argument("-P", "--port", help='port', type=int)
    parser.add_argument("-s", "--sender_email", help='sender email', type=str)
    parser.add_argument("-w", "--password", help='password', type=str)
    parser.add_argument("-R", "--receiver_email", help='receiver email', type=str)
    args = parser.parse_args()

    if args.url_file:
        args.url = [line.rstrip() for line in args.url_file]
    if args.url:
        result = scrape_debug(args.url)
        if result.next_availability:
            next_date = result.next_availability
            url = result.request.url
            consult_type = result.request.vaccine_type[0]
            send_mail(next_date, url, consult_type, 
            smtp_server=args.smtp_server,
            smtp_port=args.port,
            sender_email=args.sender_email,
            password=args.password,
            receiver_email=args.receiver_email
            )

        return
    platforms = []
    if args.platform and args.platform != "all":
        platforms = args.platform.split(",")
    scrape(platforms=platforms)


if __name__ == "__main__":  # pragma: no cover
    main()
