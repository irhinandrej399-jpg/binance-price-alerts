#!/usr/bin/env python3
"""Disposable market-data environment using the OPS-047 runner and lifecycle."""
import signal
import sys

from check_api_process import main

if __name__ == '__main__':
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
    try:
        main(owner='market_data')
    except KeyboardInterrupt:
        sys.exit('market-data process check interrupted')
    except Exception as exc:
        sys.exit('market-data process check failed: ' +
                 (str(exc) if isinstance(exc, RuntimeError) else type(exc).__name__))
