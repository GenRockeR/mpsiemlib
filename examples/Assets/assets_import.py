import argparse
import logging
import sys

from mpsiemlib.common import AuthType, Creds, ModuleNames, Settings, setup_logging
from mpsiemlib.modules import MPSIEMWorker

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--core", help="core address or fqdn", required=True)

    auth_type = parser.add_mutually_exclusive_group(required=True)
    auth_type.add_argument("--token", help="PAT token (recommended, MP SIEM 26.0+)")
    auth_type.add_argument("--ldap", action="store_true", help="Use LDAP auth")
    auth_type.add_argument("--local", action="store_true", help="Use local auth")
    parser.add_argument("--username", help="Username (for LDAP/local auth)")
    parser.add_argument("--password", help="Password (for LDAP/local auth)")

    parser.add_argument("--filename", help="filename", required=True)
    parser.add_argument("--scope", help="Scope name", required=True)
    parser.add_argument("--group", help="Group name", required=True)

    group_log = parser.add_mutually_exclusive_group()
    group_log.add_argument(
        "-d", "--debug", help="increase output verbosity", action="store_true"
    )
    group_log.add_argument("-q", "--quiet", help="Log only errors", action="store_true")

    parser.add_argument(
        "--showerrors", help="Get and display import error log", action="store_true"
    )

    args = parser.parse_args()

    if args.debug:
        loglevel = logging.DEBUG
    elif args.quiet:
        loglevel = logging.ERROR
    else:
        loglevel = logging.INFO

    setup_logging(default_level=loglevel)

    log = logging.getLogger("assets_import")
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

    try:
        mpsiemworker = MPSIEMWorker(creds, settings)
        module = mpsiemworker.get_module(ModuleNames.ASSETS)
    except Exception as e:
        log.error(e)
        sys.exit(255)

    scope = module.get_scope_id_by_name(args.scope)
    if scope is None:
        log.error(f'No scope id for scope name "{args.scope}"')
        raise SystemExit()

    group = module.get_group_id_by_name(args.group)
    if group is None:
        log.error(f'No group id for group name "{args.group}"')
        raise SystemExit()

    with open(args.filename, "rb") as content:
        status, count, logfile = module.import_assets_from_csv(
            content=content, scope_id=scope, group_id=group
        )
        if args.showerrors:
            log.info("rows with errors for source file:")
            print(logfile)

    module.close()
