from configparser import ConfigParser
from pathlib import Path

HERE = Path('config.ini').parent.resolve()
CONFIG_PATH = HERE / 'config.ini'



def config(filename=CONFIG_PATH, section="postgresql"):
    # Create parser
    parser = ConfigParser()

    # Read config file
    parser.read(filenames=filename)
    db = {}
    if parser.has_section(section=section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(
            'Section {0} is not found in the {1} file.'.format(section, filename)
        )

    return db