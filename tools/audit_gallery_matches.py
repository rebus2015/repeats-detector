#!/usr/bin/env python3
"""Audit unexpected crop matches using source PCM correlation; does not rewrite labels.

High correlation supports shared content; low correlation does not prove a false
match (alignment, resampling and codec history can lower waveform correlation).
"""
import argparse
import array
import json
import math
import wave
from pathlib import Path


def load(path):
    with wave.open(str(path)) as w:
        a = array.array('h', w.readframes(w.getnframes()))
        return a


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('manifest', type=Path)
    p.add_argument('report', type=Path)
    p.add_argument('output', type=Path)
    args = p.parse_args()
    manifest = json.loads(args.manifest.read_text())
    report = json.loads(args.report.read_text())
    refs = {r['track_id']: r['path'] for r in manifest['gallery']}
    results = []
    for c in report['cases']:
        if c['expected_track'] or not c.get('match') or c['transform'] != 'crop':
            continue
        m = c['match']
        query = load(args.manifest.parent/c['path'])
        ref = load(refs[m['original_track_id']])
        offset = round((m['original_start_sec']-m['query_start_sec'])*16000)
        start = round(m['query_start_sec']*16000)
        end = min(len(query), start+5*16000, start+round(m['matched_duration_sec']*16000))
        best = (-1, 0)
        for lag in range(-1024, 1025, 8):
            pairs = [(query[i], ref[i+offset+lag]) for i in range(start, end, 8)
                     if 0 <= i+offset+lag < len(ref)]
            if not pairs:
                continue
            n = len(pairs)
            aq, ar = sum(x for x,y in pairs)/n, sum(y for x,y in pairs)/n
            numerator = sum((x-aq)*(y-ar) for x,y in pairs)
            den = math.sqrt(sum((x-aq)**2 for x,y in pairs)*sum((y-ar)**2 for x,y in pairs))
            corr = numerator/den if den else 0
            if corr > best[0]:
                best = corr, lag
        results.append(dict(name=c['name'], matched_track=m['original_track_id'],
                            normalized_correlation=best[0], lag_samples=best[1],
                            compared_seconds=(end-start)/16000,
                            interpretation='shared PCM content supported' if best[0]>.7 else 'requires further audit'))
    args.output.write_text(json.dumps(dict(method='Centered PCM correlation, 8x subsampling, ±64 ms lag, up to 5s of matched interval.',
                                          changes_to_labels=False, cases=results), indent=2))
    print(json.dumps(results, indent=2))


if __name__ == '__main__':
    main()
