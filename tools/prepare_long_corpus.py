#!/usr/bin/env python3
"""Build a bounded long-recording gallery and reproducible WAV queries, without changing sources."""
import argparse
import hashlib
import json
import random
import struct
import wave
from pathlib import Path
from generate_corpus import noisy, pcm, write


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('source', type=Path)
    p.add_argument('output', type=Path)
    p.add_argument('--gallery-seconds', type=float, default=3600)
    p.add_argument('--negative-recordings', type=int, default=20)
    args = p.parse_args()
    if args.gallery_seconds <= 0 or args.negative_recordings < 1:
        p.error('positive gallery duration and negative count required')
    args.output.mkdir(parents=True, exist_ok=True)
    paths = sorted(args.source.glob('*.wav'))
    random.Random(20261029).shuffle(paths)
    gallery, queries, inventory = [], [], []
    total = 0
    for path in paths:
        with wave.open(str(path)) as w:
            if (w.getframerate(), w.getnchannels(), w.getsampwidth(), w.getcomptype()) != (16000, 1, 2, 'NONE'):
                raise ValueError(f'unsupported input: {path}')
            duration = w.getnframes()/16000
        in_gallery = total < args.gallery_seconds
        if not in_gallery and len(inventory)-len(gallery) >= args.negative_recordings:
            break
        sha = hashlib.sha256(path.read_bytes()).hexdigest()
        inventory.append(dict(path=str(path.resolve()), sha256=sha, duration_sec=duration, gallery=in_gallery))
        if in_gallery:
            gallery.append(dict(track_id=path.stem, path=str(path.resolve()), duration_sec=duration))
            total += duration
        with wave.open(str(path)) as w:
            # Non-frame-aligned offsets exercise crop invariance throughout the recording.
            for seconds in (2, 5, 10, 30):
                if duration < seconds+2:
                    continue
                for position, fraction in [('start', 0.01), ('middle', 0.5), ('end', 0.98)]:
                    start = min(int((duration-seconds)*fraction*16000)+37, w.getnframes()-seconds*16000)
                    w.setpos(start)
                    data = w.readframes(seconds*16000)
                    samples = [s[0]/32768 for s in struct.iter_unpack('<h', data)]
                    for transform, snr in [('crop', None), ('noise18', 18), ('noise8', 8)]:
                        name = f'{path.stem}-{seconds}s-{position}-{transform}'
                        dest = args.output / (name+'.wav')
                        if snr is None:
                            encoded = data
                        else:
                            random.seed(int(hashlib.sha256(name.encode()).hexdigest()[:16], 16))
                            encoded = pcm(noisy(samples, snr))
                        write(dest, encoded)
                        queries.append(dict(name=name, path=dest.name, expected_track=path.stem if in_gallery else '',
                                            expected_offset_sec=start/16000, seconds=seconds, transform=transform,
                                            position=position, provisional_negative=not in_gallery))
    if not gallery or len(inventory) == len(gallery):
        raise ValueError('need gallery and independent negative source files')
    manifest = dict(source=str(args.source.resolve()), seed=20261029, gallery_seconds=total,
                    negative_labels='Different source files; shared content has not been manually audited.',
                    gallery=gallery, queries=queries, inventory=inventory)
    (args.output/'manifest.json').write_text(json.dumps(manifest, indent=2))
    print(json.dumps(dict(gallery_files=len(gallery), gallery_hours=total/3600,
                         negative_files=len(inventory)-len(gallery), queries=len(queries)), indent=2))


if __name__ == '__main__':
    main()
