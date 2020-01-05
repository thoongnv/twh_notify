#!/usr/bin/env python3

import configparser
import csv
import logging
import os
import sqlite3
import traceback
from datetime import datetime

import erppeek
import requests

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))

# setup logging
logging.basicConfig(
    format='twh_notify %(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %I:%M:%S')
_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)

# load config
config = configparser.ConfigParser()
config.read('{0}/config.ini'.format(CURRENT_DIR))

DEFAULT_DATE_FORMAT = '%Y-%m-%d'
DEFAULT_WORKING_HOUR = 8.0


def main(check_date=None):
    if check_date:
        try:
            check_date = datetime.strptime(check_date, DEFAULT_DATE_FORMAT)
        except Exception:
            raise ValueError('Wrong format for check date!')
    else:
        check_date = datetime.today()

    # skip if weekend
    if check_date.weekday() in [5, 6]:
        return True
    check_date = check_date.strftime(DEFAULT_DATE_FORMAT)

    client = erppeek.Client(
        config['erppeek']['server'],
        db=config['erppeek']['database'],
        user=config['erppeek']['username'],
        password=config['erppeek']['password'])
    conn = init_database_connection()
    users = get_notify_users(conn)

    for user in users:
        _logger.info('Checking working hours for user %s on %s'
                     % (user[1], check_date))
        email = user[2]
        user_id = user[0]

        c = conn.cursor()
        c.execute('''
            SELECT *
            FROM crons
            WHERE user_id = %s AND check_date = '%s'
            ORDER BY id DESC
            LIMIT 1
        ''' % (user_id, check_date))
        cron = c.fetchone()
        if cron and not cron[4]:
            _logger.info('Fully input, can skip now!')
            continue

        working_hours = get_working_hours(client, email, check_date)
        total_hour = sum(hour['duration_hour'] for hour in working_hours)
        missing_hour = 0 if total_hour == DEFAULT_WORKING_HOUR else 1
        send_notify = 0
        if missing_hour:
            notify_email = user[3]
            to_email = notify_email if notify_email else email
            subject = "Missing working hours %s" % check_date
            text = "Today you input %s hours, please recheck it!" % total_hour

            _logger.info('Sending notification to %s ...' % (to_email))
            sent = send_email(to_email, subject, text)
            send_notify = 1 if sent else 0

        c.execute('''
            INSERT INTO crons (
                user_id,
                check_date,
                total_hour,
                missing_hour,
                send_notify
            ) VALUES ({0}, '{1}', {2}, {3}, {4})
        '''.format(user_id, check_date, total_hour, missing_hour, send_notify))
        conn.commit()

    return True


def init_database_connection():
    conn = sqlite3.connect('{0}/twh_notify.db'.format(CURRENT_DIR))
    c = conn.cursor()
    # create tables if not exists
    c.executescript('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            notify_email TEXT,
            phone TEXT,
            UNIQUE (email, notify_email, phone)
        );
        CREATE TABLE IF NOT EXISTS crons (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            check_date TEXT NOT NULL,
            total_hour REAL,
            missing_hour INTEGER,
            send_notify INTEGER,
            FOREIGN KEY (user_id) REFERENCES users (id)
        );
    ''')
    return conn


def get_notify_users(conn):
    c = conn.cursor()
    c.execute('SELECT * FROM users')
    users = c.fetchall()
    if not users:
        import_default_data(conn)
        # refetch users
        c.execute('SELECT * FROM users')
        users = c.fetchall()
    return users


def import_default_data(conn):
    c = conn.cursor()
    with open('{0}/twh_notify_users.csv'.format(CURRENT_DIR)) as f:
        _logger.info('Importing default users ...')
        csv_reader = csv.reader(f, delimiter=',')
        headers = []
        for row in csv_reader:
            if not headers:
                headers = row
                continue

            c.execute('''
                INSERT INTO users ({0}) VALUES {1}
            '''.format(', '.join(headers), str(tuple(row))))
            conn.commit()
    return True


def get_working_hours(client, email, check_date):
    _logger.info('Get working hours from TMS ...')
    domain = [
        ('date', '=', check_date),
        ('user_id.email', '=', email),
    ]
    fields = ('user_id', 'duration_hour', 'date')
    working_hours = client.model('tms.working.hour').read(domain, fields)
    return working_hours


def send_email(to_email, subject, text):
    response = requests.post(
        "%s/messages" % config['mailgun']['domain'],
        auth=("api", config['mailgun']['api_key']),
        data={
            "from": config['mailgun']['from_email'],
            "to": to_email,
            "subject": subject,
            "text": text,
        }
    )
    if response and response.status_code == 200:
        return True
    return False


if __name__ == "__main__":
    try:
        main()
    except Exception:
        text = traceback.format_exc()
        send_email('thoongnv@gmail.com', 'TWH exceptions', text)
