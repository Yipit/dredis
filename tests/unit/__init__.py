from dredis.db import DB_MANAGER

DB_MANAGER.setup_dbs('', backend='memory', backend_options={})
