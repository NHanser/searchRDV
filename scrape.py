import argparse
from multiprocessing import freeze_support
import sys
from scraper.doctolib_radio.doctolib_filters import set_config

from scraper import main

if __name__ == "__main__":
    freeze_support()
    orig_args = sys.argv.copy()
    parser = argparse.ArgumentParser()
    parser.add_argument("-c","--config", help='config "echo" or "radio" (default is "echo")', type=str)
    args, unknown = parser.parse_known_args()
    print(args)
    if args.config:
        set_config(args.config)
        for o in ['-c', '--config']:
            if o in orig_args:
                idx = orig_args.index(o)
                del orig_args[idx]
                del orig_args[idx]
    
    sys.argv = orig_args
    main()
