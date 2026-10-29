"""MyBell-style application payment event producer (synthetic).

Run: python -m generators.mybell_producer
"""

from generators.runner import main
from generators.simulation import APP

if __name__ == "__main__":
    main(APP)
