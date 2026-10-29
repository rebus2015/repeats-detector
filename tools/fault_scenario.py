#!/usr/bin/env python3
"""Kill/recover only this project's local Compose services during a paced test.

Run explicitly against a disposable local stack. Persistent volumes are retained.
The acceptance command reconnects independently; results are checked after outbox delivery.
"""
import argparse
import json
import os
from pathlib import Path
import re
import selectors
import subprocess
import sys
import time


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--manifest', default='fixtures/long/manifest.json')
    p.add_argument('--docker', default='docker')
    p.add_argument('--output', default='reports/faults.json')
    args = p.parse_args()
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    report = output.with_name(output.stem+'-acceptance.json')
    command = [args.docker, 'compose', '-f', 'compose.yaml', '-f', 'compose.minio.yaml']
    env = dict(os.environ)
    for key,value in dict(DATABASE_URL='postgres://repeats:repeats@localhost:55432/repeats?sslmode=disable',
                          RABBITMQ_URL='amqp://guest:guest@localhost:5672/', AWS_REGION='us-east-1',
                          AWS_ACCESS_KEY_ID='repeats-local', AWS_SECRET_ACCESS_KEY='repeats-local-secret').items():
        env.setdefault(key,value)
    proc = subprocess.Popen(['bin/acceptance', '-manifest', args.manifest, '-queries', '30',
                             '-concurrency', '2', '-query-delay', '1s', '-output', str(report)],
                            env=env, stderr=subprocess.PIPE, text=True, bufsize=1)
    selector = selectors.DefaultSelector()
    selector.register(proc.stderr, selectors.EVENT_READ)
    events, logs = [], []
    plan = [(4, 'service'), (12, 'rabbitmq'), (20, 'postgres')]
    started = time.monotonic()
    try:
        while proc.poll() is None:
            if time.monotonic()-started > 900:
                raise TimeoutError('fault scenario exceeded 15 minutes')
            for key,_ in selector.select(timeout=1):
                line = key.fileobj.readline()
                if not line:
                    continue
                logs.append(line.rstrip())
                print(line, end='', flush=True)
                match = re.match(r'queries (\d+),', line)
                if match and plan and int(match.group(1)) >= plan[0][0]:
                    _,service = plan.pop(0)
                    event = dict(service=service, signal='SIGKILL', started_seconds=time.monotonic()-started)
                    for action in [['kill', '-s', 'SIGKILL', service], ['up', '-d', service]]:
                        completed = subprocess.run(command+action, capture_output=True, text=True, timeout=90)
                        if completed.returncode:
                            raise RuntimeError(completed.stderr)
                    event['restarted_seconds'] = time.monotonic()-started
                    events.append(event)
                    print('Recovered container:',service,flush=True)
        remaining = proc.stderr.read()
        if remaining:
            logs.extend(remaining.splitlines())
            print(remaining, end='')
    finally:
        selector.close()
        # Restore availability even if the test fails; no volumes/queues are removed.
        subprocess.run(command+['up','-d','postgres','rabbitmq','service'], capture_output=True, timeout=90)
        if proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=10)
        data = dict(events=events, acceptance_exit=proc.returncode, unfinished_faults=plan,
                    elapsed_seconds=time.monotonic()-started, logs=logs,
                    limitations='Local single-node process crashes; not a network partition or a broker-cluster failover.')
        output.write_text(json.dumps(data, indent=2))
    if proc.returncode or plan:
        sys.exit(1)


if __name__ == '__main__':
    main()
