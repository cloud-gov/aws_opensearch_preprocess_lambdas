import os
import sys

# Each Lambda is deployed as a flat set of `.py` files at the root of its zip,
# so the handlers import the shared module as a top-level `org_partitioning`.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "lambda_functions"))
