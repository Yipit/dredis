import argparse
import datetime
import json
import logging
import shutil
import sys

from dredis import db
from dredis.keyspace import Keyspace

logger = logging.getLogger(__name__)
BACKEND = 'leveldb'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dir', help='dredis data directory', required=True)
    parser.add_argument('--output', choices=['leveldb', 'rdb'], help='output file type', required=True)
    parser.add_argument('--backend-option', action='append',
                        help='database backend options (e.g., --backend-option map_size=BYTES)')
    args = parser.parse_args()

    db_backend_options = {}
    if args.backend_option:
        for option in args.backend_option:
            if '=' not in option:
                logger.error('Expected `key=value` pairs for --backend-option parameter')
                sys.exit(1)
            key, value = map(str.strip, option.split('='))
            db_backend_options[key] = json.loads(value)

    logger.info("Copying LevelDB files...")
    output_dir = copy_dirs(args.dir, db_backend_options)

    if args.output == 'rdb':
        logger.info("Saving RDB file...")
        save_rdb(output_dir, db_backend_options)
        shutil.rmtree(output_dir)

    logger.info("Done!")


def save_rdb(output_dir, db_backend_options):
    db.DB_MANAGER.setup_dbs(output_dir, BACKEND, db_backend_options)
    keyspace = Keyspace()
    keyspace.save()


def copy_dirs(input_basedir, db_backend_options):
    output_basedir = datetime.datetime.utcnow().strftime('leveldb-backup_%Y-%m-%dT%H:%M:%S')
    shutil.copytree(input_basedir, output_basedir)
    return output_basedir


def setup_logging(level):
    logger.setLevel(level)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    logger.addHandler(handler)


if __name__ == '__main__':
    setup_logging(logging.INFO)
    main()
