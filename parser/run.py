from config.config import sql_params, esi_auth
from logging import CRITICAL, ERROR, WARN, INFO, DEBUG

from nea_EsiParser import Spawner
from nea_EsiParser.tools import init_root_logger

spawn = Spawner(sql_params, esi_auth)

if __name__ == '__main__':
    logger = init_root_logger(WARN)
    spawn.run()
