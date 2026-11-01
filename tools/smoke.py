#!/usr/bin/env python3
"""Exercise the local Compose stack. Results are inspected and requeued."""
import base64
import argparse
import json
import time
import uuid
import urllib.request
from pathlib import Path
from stack_config import setting, rabbit_management
from urllib.parse import quote


def request(url, body=None, auth=False, method=None):
    headers = {}
    if auth:
        headers['Authorization'] = 'Basic ' + base64.b64encode(rabbit_management()[1].encode()).decode()
    if isinstance(body, dict):
        body = json.dumps(body).encode()
        headers['Content-Type'] = 'application/json'
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=10) as response:
        data = response.read()
        return json.loads(data) if data else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reference', type=Path, default=Path('fixtures/reference.wav'))
    parser.add_argument('--query', type=Path, default=Path('fixtures/crop.wav'))
    args = parser.parse_args()
    service = setting('SERVICE_URL', 'http://localhost:8080').rstrip('/')
    s3 = setting('SMOKE_S3_ENDPOINT', 'http://localhost:9000').rstrip('/')
    bucket = setting('SMOKE_BUCKET', 'audio')
    rabbit, _, vhost = rabbit_management()
    input_queue = setting('INPUT_QUEUE', 'audio.tasks')
    output_queue = setting('OUTPUT_QUEUE', 'audio.results')
    request(service+'/readyz')
    scope = 'smoke-' + uuid.uuid4().hex[:12]
    expected = {}
    for i, path in enumerate([args.reference, args.query]):
        name = 'reference.wav' if i == 0 else 'query.wav'
        if not path.exists():
            raise SystemExit('Run python3 tools/generate_corpus.py fixtures first')
        key = scope + '-' + name
        request(s3+'/'+quote(bucket, safe='')+'/' + quote(key, safe=''), path.read_bytes(), method='PUT')
        task_id = scope + '-' + str(i)
        expected[task_id] = 'unique' if i == 0 else 'duplicate'
        task = dict(schema_version=1, task_id=task_id, track_id=key,
                    scope_id=scope, s3_url='s3://'+bucket+'/' + key)
        published = request(rabbit+'/api/exchanges/'+vhost+'/amq.default/publish',
                            dict(properties={'delivery_mode': 2}, routing_key=input_queue,
                                 payload=json.dumps(task), payload_encoding='string'), auth=True)
        if not published.get('routed'):
            raise SystemExit('Task not routed')
        deadline = time.monotonic() + 30
        found = None
        while time.monotonic() < deadline:
            messages = request(rabbit+'/api/queues/'+vhost+'/'+quote(output_queue, safe='')+'/get',
                               dict(count=100, ackmode='ack_requeue_true', encoding='auto'), auth=True)
            for message in messages:
                result = json.loads(message['payload'])
                if result.get('task_id') == task_id:
                    found = result
                    break
            if found:
                break
            time.sleep(0.2)
        if not found or found['status'] != expected[task_id]:
            raise SystemExit('Unexpected result: ' + json.dumps(found))
        print(json.dumps(found, ensure_ascii=False))
    print('Compose smoke test passed; result events remain in '+output_queue)


if __name__ == '__main__':
    main()
