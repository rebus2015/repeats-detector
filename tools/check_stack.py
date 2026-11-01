#!/usr/bin/env python3
"""Read-only health and provisioning check for the local Compose stack."""
import base64
import json
import urllib.parse
import urllib.request
from stack_config import setting, rabbit_management


def get(url, credentials=None):
    req=urllib.request.Request(url)
    if credentials:
        req.add_header('Authorization','Basic '+base64.b64encode(credentials.encode()).decode())
    with urllib.request.urlopen(req,timeout=10) as response:
        data=response.read()
        return json.loads(data) if data else None


def main():
    service=setting('SERVICE_URL','http://localhost:8080').rstrip('/')
    minio=setting('S3_ENDPOINT','http://localhost:9002').rstrip('/')
    grafana=setting('GRAFANA_URL','http://localhost:3000').rstrip('/')
    prometheus=setting('PROMETHEUS_URL','http://localhost:9090').rstrip('/')
    rabbit,credentials,vhost=rabbit_management()
    get(service+'/readyz')
    get(minio+'/minio/health/ready')
    dashboard=get(grafana+'/api/dashboards/uid/repeats-detector',
                  setting('GRAFANA_ADMIN_USER','admin')+':'+setting('GRAFANA_ADMIN_PASSWORD','repeats-local-grafana'))
    assert len(dashboard['dashboard']['panels'])==12
    rules=get(prometheus+'/api/v1/rules')
    count=sum(len(group['rules']) for group in rules['data']['groups'])
    assert count==5
    metrics={}
    for name in ['up','audio_outbox_pending','audio_outbox_oldest_seconds','audio_go_heap_bytes','rabbitmq_queue_messages_ready']:
        data=get(prometheus+'/api/v1/query?'+urllib.parse.urlencode(dict(query=name)))
        assert data['status']=='success' and data['data']['result'],name
        metrics[name]=data['data']['result']
    queues=get(rabbit+'/api/queues/'+vhost,credentials)
    summary=dict(ready=True,minio=True,dashboard_panels=12,alert_rules=count,
                 queues={q['name']:dict(ready=q.get('messages_ready',0),unacknowledged=q.get('messages_unacknowledged',0))
                         for q in queues if q['name'] in [setting('INPUT_QUEUE','audio.tasks'),setting('DLQ_QUEUE','audio.tasks.dead'),setting('OUTPUT_QUEUE','audio.results')]},
                 metrics=metrics)
    print(json.dumps(summary,indent=2))


if __name__=='__main__':
    main()
