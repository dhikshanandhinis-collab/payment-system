"""External payment processor transaction event producer (synthetic).

Run: python -m generators.processor_producer
"""

from generators.runner import main
from generators.simulation import PROCESSOR

if __name__ == "__main__":
    main(PROCESSOR)
