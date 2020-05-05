from config.config import sql_params, mongo_params, auth_char_id, verbose
from nea_esiParser import Spawner

spawn = Spawner(sql_params, mongo_params, auth_char_id, verbose=verbose)

if __name__ == '__main__':
    spawn.run()