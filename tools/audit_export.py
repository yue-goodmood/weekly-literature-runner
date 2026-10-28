"""Fail closed if an export contains non-allowlisted files or obvious secrets."""
import ast
import hashlib
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
ALLOWED = {'README.md', '.gitignore', '.github/workflows/literature.yml',
           '.github/workflows/test.yml', 'tools/audit_export.py',
           'weekly-literature/config/topics.json'}
ALLOWED.update('weekly-literature/src/'+name+'.py' for name in (
    'pipeline','publish','supplements','report_format','verify_publication',
    'retune_queue','recover','public_runner','partial_publish'))
ALLOWED.update('weekly-literature/tests/test_'+name+'.py' for name in (
    'pipeline','publish','report_format','retune_queue','supplements','public_runner','partial_publish'))
ALLOWED.update('weekly-literature/migrations/000'+str(n)+'.sql' for n in range(1,6))


def audit():
    paths = {p.relative_to(ROOT).as_posix():p for p in ROOT.rglob('*') if p.is_file()
             and '.git' not in p.relative_to(ROOT).parts
             and '__pycache__' not in p.relative_to(ROOT).parts}
    if set(paths) != ALLOWED:
        raise ValueError('Export allowlist mismatch; inspect privately')
    markers = [r'(?i)github_pat_[A-Za-z0-9_]{20,}', r'gh[pousr]_[A-Za-z0-9]{20,}',
               r'(?:ntn_|secret_)[A-Za-z0-9]{20,}',
               r'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----',
               r'\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b',
               r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}',
               r'https://(?:www\.)?notion\.(?:so|site)/']
    for name, path in paths.items():
        text = path.read_text()
        # A quoted public origin contains no page identifier. Concrete private
        # URLs are still rejected; only this exact reusable literal is exempt.
        inspected = re.sub(r'''(['"])https://www[.]notion[.]so/\1''', 'PUBLIC_NOTION_ORIGIN', text)
        if any(re.search(pattern, inspected) for pattern in markers):
            raise ValueError('Sensitive export marker; inspect privately')
        if name.endswith('.py'):
            ast.parse(text)
    workflows = [paths[name].read_text() for name in paths if name.endswith('.yml')]
    forbidden = ('pull_request_target:', 'secrets: inherit', 'upload-artifact',
                 'ACTIONS_STEP_DEBUG', 'set -x', 'repository_dispatch:')
    if any(x in content for x in forbidden for content in workflows):
        raise ValueError('Unsafe public workflow feature')
    manifest = {name:hashlib.sha256(path.read_bytes()).hexdigest()
                for name,path in sorted(paths.items())}
    return {'files':len(paths), 'manifest_sha256':hashlib.sha256(
        json.dumps(manifest, sort_keys=True).encode()).hexdigest()}


if __name__ == '__main__':
    print(json.dumps(audit(), sort_keys=True))

