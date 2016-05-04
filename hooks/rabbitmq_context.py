from charmhelpers.contrib.ssl.service import ServiceCA

from charmhelpers.core.hookenv import (
    open_port,
    close_port,
    config,
    log,
    ERROR,
)

import sys
import pwd
import grp
import os
import base64

import ssl_utils

ssl_key_file = "/etc/rabbitmq/rabbit-server-privkey.pem"
ssl_cert_file = "/etc/rabbitmq/rabbit-server-cert.pem"
ssl_ca_file = "/etc/rabbitmq/rabbit-server-ca.pem"


def convert_from_base64(v):
    # Rabbit originally supported pem encoded key/cert in config, play
    # nice on upgrades as we now expect base64 encoded key/cert/ca.
    if not v:
        return v
    if v.startswith('-----BEGIN'):
        return v
    try:
        return base64.b64decode(v)
    except TypeError:
        return v


class RabbitMQSSLContext(object):

    def enable_ssl(self, ssl_key, ssl_cert, ssl_port,
                   ssl_ca=None, ssl_only=False, ssl_client=None):

        uid = pwd.getpwnam("root").pw_uid
        gid = grp.getgrnam("rabbitmq").gr_gid

        for contents, path in (
                (ssl_key, ssl_key_file),
                (ssl_cert, ssl_cert_file),
                (ssl_ca, ssl_ca_file)):

            if not contents:
                continue

            with open(path, 'w') as fh:
                fh.write(contents)

            os.chmod(path, 0o640)
            os.chown(path, uid, gid)

        data = {
            "ssl_port": ssl_port,
            "ssl_cert_file": ssl_cert_file,
            "ssl_key_file": ssl_key_file,
            "ssl_client": ssl_client,
            "ssl_ca_file": "",
            "ssl_only": ssl_only
        }

        if ssl_ca:
            data["ssl_ca_file"] = ssl_ca_file

        return data

    def __call__(self):
        """
        The legacy config support adds some additional complications.

        ssl_enabled = True, ssl = off -> ssl enabled
        ssl_enabled = False, ssl = on -> ssl enabled
        """
        ssl_mode, external_ca = ssl_utils.get_ssl_mode()

        ctxt = {
            'ssl_mode': ssl_mode,
        }

        if ssl_mode == 'off':
            close_port(config('ssl_port'))
            ssl_utils.reconfigure_client_ssl()
            return ctxt

        ssl_key = convert_from_base64(config('ssl_key'))
        ssl_cert = convert_from_base64(config('ssl_cert'))
        ssl_ca = convert_from_base64(config('ssl_ca'))
        ssl_port = config('ssl_port')

        # If external managed certs then we need all the fields.
        if (ssl_mode in ('on', 'only') and any((ssl_key, ssl_cert)) and
                not all((ssl_key, ssl_cert))):
            log('If ssl_key or ssl_cert are specified both are required.',
                level=ERROR)
            sys.exit(1)

        if not external_ca:
            ssl_cert, ssl_key, ssl_ca = ServiceCA.get_service_cert()

        ctxt.update(self.enable_ssl(
            ssl_key, ssl_cert, ssl_port, ssl_ca,
            ssl_only=(ssl_mode == "only"), ssl_client=False
        ))

        ssl_utils.reconfigure_client_ssl(True)
        open_port(ssl_port)

        return ctxt


class RabbitMQClusterContext(object):

    def __call__(self):
        return {
            'cluster_partition_handling': config('cluster-partition-handling'),
        }
