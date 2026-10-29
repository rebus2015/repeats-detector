#!/usr/bin/env python3
"""Prepare reproducible transformations; leave original WAV files untouched.

Different recordings are provisional negatives and must be manually audited.
All variants of a source stay in the same deterministic train/test split.
"""
import argparse
import hashlib
import json
import random
import subprocess
import shutil
import wave
from pathlib import Path
from generate_corpus import RATE, pcm, noisy, write


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--aac", action="store_true", help="also encode AAC using macOS afconvert")
    args = parser.parse_args()
    if args.aac and not shutil.which("afconvert"):
        parser.error("--aac requires afconvert")
    paths = sorted(args.source.glob("*.wav"))
    if len(paths) < 2:
        parser.error("at least two WAV files required")
    args.output.mkdir(parents=True, exist_ok=True)
    cases = {"development": [], "holdout": []}
    inventory = []
    for i, path in enumerate(paths):
        raw = path.read_bytes()
        sha = hashlib.sha256(raw).hexdigest()
        split = "holdout" if int(sha[:8], 16) % 3 == 0 else "development"
        with wave.open(str(path)) as wav:
            if (wav.getnchannels(), wav.getsampwidth(), wav.getframerate()) != (1, 2, RATE):
                raise ValueError(f"unsupported WAV: {path}")
            data = wav.readframes(wav.getnframes())
        import struct
        samples = [s[0]/32768 for s in struct.iter_unpack("<h", data)]
        start = int(1.357*RATE)
        crop = samples[start:start+5*RATE]
        if len(crop) < 3*RATE:
            raise ValueError(f"recording too short: {path}")
        ref = args.output / (path.stem+"-ref.wav")
        write(ref, data)
        variants = {"exact": data, "crop": pcm(crop), "noise18": pcm(noisy(crop, 18)),
                    "noise8": pcm(noisy(crop, 8)), "noise0": pcm(noisy(crop, 0))}
        try:
            import audioop
            variants["mulaw"] = audioop.ulaw2lin(audioop.lin2ulaw(pcm(crop), 2), 2)
            variants["mulaw-noise8"] = audioop.ulaw2lin(audioop.lin2ulaw(pcm(noisy(crop, 8)), 2), 2)
        except ImportError:
            pass
        for transform, encoded in variants.items():
            query = args.output / (path.stem+"-"+transform+".wav")
            write(query, encoded)
            cases[split].append(dict(name=path.stem+"/"+transform, reference=ref.name,
                                    query=query.name, expected=True,
                                    expected_original_start_sec=0 if transform=="exact" else start/RATE))
        if args.aac:
            for transform in ["crop", "noise8"]:
                source_wav = args.output / (path.stem+"-"+transform+".wav")
                compressed = args.output / (path.stem+"-"+transform+".m4a")
                decoded = args.output / (path.stem+"-aac-"+transform+".wav")
                subprocess.run(["afconvert", "-f", "m4af", "-d", "aac", "-b", "24000", str(source_wav), str(compressed)], check=True, capture_output=True)
                subprocess.run(["afconvert", "-f", "WAVE", "-d", "LEI16@16000", str(compressed), str(decoded)], check=True, capture_output=True)
                cases[split].append(dict(name=path.stem+"/aac-"+transform, reference=ref.name, query=decoded.name, expected=True, expected_original_start_sec=start/RATE))
        inventory.append(dict(source=path.name, sha256=sha, split=split, duration_sec=len(samples)/RATE))
    # Negatives stay within the same split, avoiding cross-split source leakage.
    for split in cases:
        group = [x for x in inventory if x["split"] == split]
        for i, item in enumerate(group):
            for offset in [1, 3]:
                other = group[(i+offset)%len(group)]
                if item is other:
                    continue
                a, b = Path(item["source"]).stem, Path(other["source"]).stem
                cases[split].append(dict(name=a+"/negative-"+str(offset), reference=a+"-ref.wav",
                                        query=b+"-ref.wav", expected=False))
        (args.output/(split+".json")).write_text(json.dumps(cases[split], indent=2))
    (args.output/"inventory.json").write_text(json.dumps(inventory, indent=2))
    print(json.dumps({"sources":len(paths),"cases":{k:len(v) for k,v in cases.items()}}, indent=2))


if __name__ == "__main__":
    main()
