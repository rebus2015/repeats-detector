#!/usr/bin/env python3
"""Read-only health and provisioning check for the local Compose stack."""
import base64
import json
import os
import urllib.parse
import urllib.request


def get(url, credentials=None):
    req=urllib.request.Request(url)
    if credentials:
        req.add_header('Authorization','Basic '+base64.b64encode(credentials.encode()).decode())
    with urllib.request.urlopen(req,timeout=10) as response:
        data=response.read()
        return json.loads(data) if data else None


def main():
    get('http://localhost:8080/readyz')
    get('http://localhost:9002/minio/health/ready')
    dashboard=get('http://localhost:3000/api/dashboards/uid/repeats-detector',
                  'admin:'+os.getenv('GRAFANA_ADMIN_PASSWORD','repeats-local-grafana'))
    assert len(dashboard['dashboard']['panels'])==12
    rules=get('http://localhost:9090/api/v1/rules')
    count=sum(len(group['rules']) for group in rules['data']['groups'])
    assert count==5
    metrics={}
    for name in ['up','audio_outbox_pending','audio_outbox_oldest_seconds','audio_go_heap_bytes','rabbitmq_queue_messages_ready']:
        data=get('http://localhost:9090/api/v1/query?'+urllib.parse.urlencode(dict(query=name)))
        assert data['status']=='success' and data['data']['result'],name
        metrics[name]=data['data']['result']
    queues=get('http://localhost:15672/api/queues/%2F','guest:guest')
    summary=dict(ready=True,minio=True,dashboard_panels=12,alert_rules=count,
                 queues={q['name']:dict(ready=q.get('messages_ready',0),unacknowledged=q.get('messages_unacknowledged',0))
                         for q in queues if q['name'] in ['audio.tasks','audio.tasks.dead','audio.results']},
                 metrics=metrics)
    print(json.dumps(summary,indent=2))


if __name__=='__main__':
    main()
