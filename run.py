from config.config import sql_params, verbose
from nea_esiParser import Spawner

spawn = Spawner(sql_params, verbose=verbose)

if __name__ == '__main__':
    spawn.run()