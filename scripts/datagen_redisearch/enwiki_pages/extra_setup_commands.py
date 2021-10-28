import json
import os
import sys

with open(sys.argv[1]) as json_f:
    a = json.load(json_f)
    for cmd in a["setup"]["commands"]:
        print(" ".join(cmd))
