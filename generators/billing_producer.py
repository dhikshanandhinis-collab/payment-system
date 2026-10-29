"""Telecom billing ledger event producer (synthetic).

Run: python -m generators.billing_producer
"""

from generators.runner import main
from generators.simulation import LEDGER

if __name__ == "__main__":
    main(LEDGER)
