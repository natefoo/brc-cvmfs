#!/usr/bin/env python3
import argparse
import os

import requests
from bioblend.galaxy import GalaxyInstance


parser = argparse.ArgumentParser(description="")
parser.add_argument(
    "-g", "--galaxy-url", default="http://localhost:8080", help="The Galaxy server URL"
)
parser.add_argument(
    "-u", "--galaxy-user", default="brc@galaxyproject.org", help="Galaxy user email"
)
parser.add_argument(
    "-p", "--galaxy-password", default='brcbrc', help="Galaxy user password"
)
parser.add_argument(
    "-a", "--admin-api-key", default='c0ffee', help="Admin API key"
)
args = parser.parse_args()

#api_key = args.galaxy_api_key or os.environ.get("EPHEMERIS_API_KEY")
#password = args.galaxy_password or os.environ.get("IDC_USER_PASS")
#if api_key:
#    auth_kwargs = {"key": api_key}
#elif password:
#    auth_kwargs = {"email": args.galaxy_user, "password": password}
#else:
#    raise RuntimeError("No Galaxy credentials supplied")

auth_kwargs = {"email": args.galaxy_user, "password": args.galaxy_password}
try:
    gi = GalaxyInstance(url=args.galaxy_url, **auth_kwargs)
except Exception:
    # why does user creation require an admin API key?
    gi = GalaxyInstance(url=args.galaxy_url, key=args.admin_api_key)
    gi.users.create_local_user(
        username=args.galaxy_user.split('@')[0],
        user_email=args.galaxy_user,
        password=args.galaxy_password
    )
    gi = GalaxyInstance(url=args.galaxy_url, **auth_kwargs)

u = gi.users.get_current_user()
r = gi.users.get_or_create_user_apikey(u["id"])
print(r)
