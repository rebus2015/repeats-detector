#!/usr/bin/env python3
"""Run independent local acceptance scopes concurrently and retain per-run reports."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--manifest',default='fixtures/long/manifest.json')
    p.add_argument('--parallel',type=int,default=2)
    p.add_argument('--queries',type=int,default=30)
    p.add_argument('--output',default='reports/multiscope')
    args=p.parse_args()
    if not 1<=args.parallel<=8 or args.queries<1:
        p.error('parallel must be 1..8; queries positive')
    directory=Path(args.output)
    directory.mkdir(parents=True,exist_ok=True)
    env=dict(os.environ)
    for k,v in dict(DATABASE_URL='postgres://repeats:repeats@localhost:55432/repeats?sslmode=disable',
                    RABBITMQ_URL='amqp://guest:guest@localhost:5672/',AWS_REGION='us-east-1',
                    AWS_ACCESS_KEY_ID='repeats-local',AWS_SECRET_ACCESS_KEY='repeats-local-secret').items():
        env.setdefault(k,v)
    processes=[]
    started=time.monotonic()
    try:
        for i in range(args.parallel):
            log=(directory/f'{i}.log').open('w')
            path=directory/f'{i}.json'
            proc=subprocess.Popen(['bin/acceptance','-manifest',args.manifest,'-queries',str(args.queries),
                                   '-concurrency','2','-output',str(path)],env=env,stderr=log)
            processes.append((proc,log,path))
        codes=[]
        for proc,_,_ in processes:
            codes.append(proc.wait(timeout=900))
        reports=[json.loads(path.read_text()) for _,_,path in processes if path.exists()]
        summary=dict(parallel=args.parallel,elapsed_seconds=time.monotonic()-started,exit_codes=codes,
                     reports=[{k:v for k,v in report.items() if k!='results'} for report in reports],
                     limitations='Short local concurrent-scope load, not a sustained soak test or a throughput ceiling.')
        (directory/'summary.json').write_text(json.dumps(summary,indent=2))
        print(json.dumps(summary,indent=2))
        if any(codes) or len(reports)!=args.parallel:
            raise SystemExit(1)
    finally:
        for proc,log,_ in processes:
            if proc.poll() is None:
                proc.terminate()
                proc.wait(timeout=10)
            log.close()


if __name__=='__main__':
    main()
