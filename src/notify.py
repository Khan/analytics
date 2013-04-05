#!/usr/bin/env python

"""Module to support email and HipChat notifications to analytics team."""

import os
import sys

import hipchat.room
import hipchat.config

EMAIL_RECIPIENTS = ['jace@khanacademy.org', 'analytics-admin@khanacademy.org']


def send_email(subject, body, addresses=EMAIL_RECIPIENTS):
    addresses = ' '.join(addresses)
    # TODO(jace): use subprocess module instead of this echo pipe ugliness
    body_command = "echo '%s'" % body.replace("'", '"')
    command = "%s | mailx -s '%s' %s" % (body_command, subject, addresses)
    os.system(command)


def send_hipchat(message, rooms=["analytics"]):

    cfg_location = os.path.join(os.path.dirname(__file__), 'hipchat.cfg')
    try:
        hipchat.config.init_cfg(cfg_location)
    except Exception:
        # No hipchat.cfg file found - the token will be empty and handled below
        print "No hipchat cfg found."
        pass

    if not hipchat.config.token:
        print >> sys.stderr, (
            'Can\'t find HipChat token. Make a hipchat.cfg file ' +
            'with a single line "token = <token_value>" ' +
            '(don\'t forget to chmod 600) either in this directory ' +
            'or in your $HOME directory')
        return

    for room in rooms:
        result = ""
        msg_dict = {
            "room_id": room,
            "from": "Mr Monkey",
            "message": message,
            "color": "purple",
        }

        try:
            result = str(hipchat.room.Room.message(**msg_dict))
        except:
            pass

        if "sent" in result:
            print "Notified Hipchat room %s" % room
        else:
            print "Failed to send message to Hipchat: %s" % message
