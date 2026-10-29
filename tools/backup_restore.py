#!/usr/bin/env python3
"""Verify a local PostgreSQL custom-format backup by restoring into a temporary database.

Does not replace the service database. Only the temporary verification database is dropped.
Store archives outside the repository; they can contain audio identifiers and event payloads.
"""
import argparse
import json
from pathlib import Path
import subprocess
import time


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--docker', default='docker')
    p.add_argument('--archive', default='backups/repeats.dump')
    p.add_argument('--report', default='reports/backup-restore.json')
    args=p.parse_args()
    archive=Path(args.archive)
    if archive.exists():
        p.error('archive exists; choose a new path')
    archive.parent.mkdir(parents=True, exist_ok=True)
    cmd=[args.docker,'compose','exec','-T','postgres']
    started=time.monotonic()
    with archive.open('xb') as out:
        subprocess.run(cmd+['pg_dump','-U','repeats','-d','repeats','-Fc','--no-owner','--no-acl'],stdout=out,check=True,timeout=300)
    name='restore_check_'+str(time.time_ns())
    created=False
    try:
        subprocess.run(cmd+['createdb','-U','repeats',name],check=True,timeout=30)
        created=True
        with archive.open('rb') as data:
            subprocess.run(cmd+['pg_restore','-U','repeats','-d',name,'--exit-on-error','--no-owner','--no-acl'],stdin=data,check=True,timeout=300)
        sql="""SELECT json_build_object(
          'tasks',(SELECT count(*) FROM rd_tasks),
          'prints',(SELECT count(*) FROM rd_prints),
          'outbox',(SELECT count(*) FROM rd_outbox),
          'pending',(SELECT count(*) FROM rd_outbox WHERE delivered_at IS NULL),
          'orphan_prints',(SELECT count(*) FROM rd_prints p LEFT JOIN rd_tasks t USING(scope_id,task_id) WHERE t.task_id IS NULL),
          'orphan_events',(SELECT count(*) FROM rd_outbox o LEFT JOIN rd_tasks t USING(scope_id,task_id) WHERE t.task_id IS NULL));"""
        counts=json.loads(subprocess.check_output(cmd+['psql','-U','repeats','-d',name,'-At','-v','ON_ERROR_STOP=1','-c',sql],timeout=30))
        if counts['orphan_prints'] or counts['orphan_events']:
            raise RuntimeError('restored database has broken references')
        report=dict(archive=str(archive),archive_bytes=archive.stat().st_size,restored=counts,
                    elapsed_seconds=time.monotonic()-started,verified=True,
                    limitations='Local full backup restore; remote storage, encryption, PITR and recovery time objectives not tested.')
        path=Path(args.report);path.parent.mkdir(parents=True,exist_ok=True)
        path.write_text(json.dumps(report,indent=2))
        print(json.dumps(report,indent=2))
    finally:
        if created:
            subprocess.run(cmd+['dropdb','-U','repeats',name],check=True,timeout=30)


if __name__=='__main__':
    main()
