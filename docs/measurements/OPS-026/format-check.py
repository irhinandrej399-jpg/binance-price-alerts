"""Reject corrupted copies without changing measured evidence or raw files."""
import copy
import json
from pathlib import Path
import shutil
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'scripts'))
import verify_notifications_measurement as verifier


def main():
    cases = {
        'unknown_metric': ('postgres/report.json', lambda x: x['phases']['empty']['operation_delta'].__setitem__('wal_insert_bytes', None)),
        'missing_phase': ('identities.json', lambda x: x['phases'].pop()),
        'foreign_hash': ('identities.json', lambda x: x.__setitem__('source_sha256', '0'*64)),
        'wrong_body': ('identities.json', lambda x: x['items'][0].__setitem__('body_hex', '00')),
        'physical_offset': ('identities.json', lambda x: x['records'][2].__setitem__('offset', x['records'][1]['offset'])),
        'pending_ack': ('identities.json', lambda x: x['counts'].__setitem__('uncertain_pending', 1)),
        'foreign_attempt': ('identities.json', lambda x: x['items'][0].__setitem__('current_attempt_id', 'foreign')),
        'missing_http': ('identities.json', lambda x: x.__setitem__('http_calls', 1)),
    }
    result = {}
    with tempfile.TemporaryDirectory(prefix='ops026-format-') as temp:
        target = Path(temp) / 'copy'
        source = Path(__file__).resolve().parent / 'reports-a'
        shutil.copytree(source, target)
        for name, (file, mutate) in cases.items():
            dest = target / file
            original = dest.read_bytes()
            data = json.loads(original)
            mutate(data)
            dest.write_text(json.dumps(data))
            try:
                verifier.verify(target)
            except (AssertionError, KeyError, ValueError, TypeError):
                result[name] = 'rejected'
            else:
                raise AssertionError('corruption accepted: '+name)
            finally:
                dest.write_bytes(original)
    assert len(result) == 8
    (Path(__file__).resolve().parent / 'format-check.json').write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
