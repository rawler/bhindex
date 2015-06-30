from bithorde import Client, parseHashIds

import logging
logging.basicConfig()

if __name__ == '__main__':
    import sys

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--fetch", action='store_true', help="fetch the given assets")
    parser.add_argument("hashIds", action='append', help='ids for assets to fetch')
    args = parser.parse_args()

    client = Client({
        'pressure': 10,
        'address': '/tmp/bithorde.source',
        'asset_timeout': 3000
    })

    def check_asset(arg):
        ids = parseHashIds(arg)
        if ids:
            with client.open(ids) as asset: # Bithorde assets are context-managers, YAY!
                return asset.status()
        else:
            print "Warning, failed to parse: ", arg

    def read_asset(arg):
        ids = parseHashIds(arg)
        if ids:
            with client.open(ids) as asset: # Bithorde assets are context-managers, YAY!
                offset = 0
                status = asset.status()
                if not status and status.status() == message.Status.SUCCESS:
                    sys.stderr.write("Failed to fetch: %s\n" % arg)
                    return
                while offset < status.size:
                    chunk = asset.read(offset, 64*1024)
                    if not chunk:
                        sys.stderr.write("Interupted mid-stream: %s\n" % arg)
                        return
                    sys.stdout.write(chunk)

                    offset += len(chunk)
        else:
            print "Warning, failed to parse: ", addr

    if args.fetch:
        for status in args.hashIds:
            read_asset(status)
    else:
        # Run a bunch in parallel, controlled by a pool
        for status in client.pool().imap(check_asset, args.hashIds):
            if status:
                print status
