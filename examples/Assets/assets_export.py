import argparse
import logging
import sys

from mpsiemlib.common import *
from mpsiemlib.modules import MPSIEMWorker

FORMAT = '%(asctime)s - [%(filename)s][%(funcName)s] - %(levelname)s - %(message)s'

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--core', help='core address or fqdn', required=True)
    parser.add_argument('--username', help='Username', required=True)
    parser.add_argument('--password', help='Username', required=True)

    auth_type = parser.add_mutually_exclusive_group()
    auth_type.add_argument('--ldap', action='store_true', help='Use LDAP auth')
    auth_type.add_argument('--local', action='store_true', help='Use local auth (default)')

    parser.add_argument('--pdql', help='pdql filter (example: select(@host))', required=True)
    parser.add_argument('--filename', help='filename', required=True)

    group_log = parser.add_mutually_exclusive_group()
    group_log.add_argument('-d', '--debug', help='increase output verbosity',
                           action='store_true')
    group_log.add_argument('-q', '--quiet', help='Log only errors',
                           action='store_true')
    parser.add_argument('--timeout', type=int, help='request timeout')
    args = parser.parse_args()

    if args.debug:
        loglevel = logging.DEBUG
    elif args.quiet:
        loglevel = logging.ERROR
    else:
        loglevel = logging.INFO

    logging.basicConfig(level=loglevel, format=FORMAT)
    log = logging.getLogger('assets_export')
    log.setLevel(loglevel)

    creds = Creds()
    creds.core_hostname = args.core

    if args.ldap:
        creds.core_auth_type = AuthType.LDAP
    else:
        creds.core_auth_type = AuthType.LOCAL

    creds.core_login = args.username
    creds.core_pass = args.password

    settings = Settings()

    if args.timeout:
        settings.connection_timeout = args.timeout

    mpsiemworker = MPSIEMWorker(creds, settings)
    try:
        module = mpsiemworker.get_module(ModuleNames.ASSETS)
    except:
        e = sys.exc_info()[0]
        log.error(e)
        sys.exit(255)

    token = module.create_assets_request(pdql=args.pdql, group_ids=[])
    content = module.get_assets_list_csv(token)
    if content is not None:
        with open(args.filename, 'w', encoding='utf-8-sig') as output_file:
            # next(content)
            for line in content:
                output_file.write(line + "\n")

    module.close()
