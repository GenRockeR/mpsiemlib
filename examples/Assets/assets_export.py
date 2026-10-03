import argparse
import logging
import sys

from mpsiemlib.common import AuthType, Creds, ModuleNames, Settings
from mpsiemlib.modules import MPSIEMWorker

FORMAT = "%(asctime)s - [%(filename)s][%(funcName)s] - %(levelname)s - %(message)s"

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--core", help="core address or fqdn", required=True)

    auth_type = parser.add_mutually_exclusive_group(required=True)
    auth_type.add_argument("--token", help="PAT token (recommended, MP SIEM 26.0+)")
    auth_type.add_argument("--ldap", action="store_true", help="Use LDAP auth")
    auth_type.add_argument("--local", action="store_true", help="Use local auth")
    parser.add_argument("--username", help="Username (for LDAP/local auth)")
    parser.add_argument("--password", help="Password (for LDAP/local auth)")

    parser.add_argument(
        "--pdql", help="pdql filter (example: select(@host))", required=True
    )
    parser.add_argument("--filename", help="filename", required=True)

    group_log = parser.add_mutually_exclusive_group()
    group_log.add_argument(
        "-d", "--debug", help="increase output verbosity", action="store_true"
    )
    group_log.add_argument("-q", "--quiet", help="Log only errors", action="store_true")
    parser.add_argument("--timeout", type=int, help="request timeout")
    args = parser.parse_args()

    if args.debug:
        loglevel = logging.DEBUG
    elif args.quiet:
        loglevel = logging.ERROR
    else:
        loglevel = logging.INFO

    logging.basicConfig(level=loglevel, format=FORMAT)
    log = logging.getLogger("assets_export")
    log.setLevel(loglevel)

    creds = Creds()
    creds.core_hostname = args.core

    if args.token:
        creds.core_auth_type = AuthType.PAT_TOKEN
        creds.pat_token = args.token
    else:
        if not args.username or not args.password:
            log.error("--username and --password are required for LDAP/local auth")
            sys.exit(254)
        creds.core_auth_type = AuthType.LDAP if args.ldap else AuthType.LOCAL
        creds.core_login = args.username
        creds.core_pass = args.password

    settings = Settings()

    if args.timeout:
        settings.connection_timeout = args.timeout

    try:
        mpsiemworker = MPSIEMWorker(creds, settings)
        module = mpsiemworker.get_module(ModuleNames.ASSETS)
    except Exception as e:
        log.error(e)
        sys.exit(255)

    token = module.create_assets_request(pdql=args.pdql, group_ids=[])
    content = module.get_assets_list_csv(token)
    if content is not None:
        with open(args.filename, "w", encoding="utf-8-sig") as output_file:
            output_file.writelines(line + "\n" for line in content)

    module.close()
