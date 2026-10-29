#!/usr/bin/env python3
"""Deterministic mechanical corpus, NOT a speech quality benchmark.

Python <=3.12 audioop is optional. With ffmpeg on PATH, also tests MP3 and Opus.
No external audio is downloaded. Run: python3 tools/generate_corpus.py fixtures
"""
import argparse
import json
import math
import random
import shutil
import struct
import subprocess
import wave
from pathlib import Path

RATE = 16000

def signal(seed, seconds):
    rng = random.Random(seed)
    frequencies = [[rng.uniform(150, 5150) for _ in range(4)] for _ in range(int(seconds * 8) + 2)]
    phases = [0.0] * 4
    samples = []
    for i in range(int(seconds * RATE)):
        for j in range(4):
            phases[j] += 2 * math.pi * frequencies[i // 2000][j] / RATE
        samples.append(sum(0.12 * math.sin(p) for p in phases))
    return samples

def noisy(samples, snr):
    rng = random.Random(42)
    rms = math.sqrt(sum(x*x for x in samples) / len(samples))
    scale = rms / 10**(snr/20)
    return [x + rng.gauss(0, scale) for x in samples]

def pcm(samples):
    return b"".join(struct.pack("<h", round(max(-1, min(1, x))*32767)) for x in samples)

def write(path, data):
    with wave.open(str(path), "wb") as wav:
        wav.setparams((1, 2, RATE, 0, "NONE", "not compressed"))
        wav.writeframes(data)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    original = signal(42, 10)
    start = int(1.357 * RATE)
    crop = original[start:start + 6*RATE]
    variants = {"exact": (original, True), "crop": (crop, True),
                "noise18": (noisy(crop, 18), True), "noise8": (noisy(crop, 8), True),
                "unrelated": (signal(99, 6), False), "silence": ([0.0]*6*RATE, False),
                "short": (crop[:RATE//2], False)}
    write(args.output / "reference.wav", pcm(original))
    cases = []
    for name, (samples, expected) in variants.items():
        write(args.output / (name + ".wav"), pcm(samples))
        case = dict(name=name, reference="reference.wav", query=name+".wav", expected=expected)
        if expected:
            case["expected_original_start_sec"] = 0 if name == "exact" else start/RATE
        cases.append(case)
    try:
        import audioop
        data = audioop.ulaw2lin(audioop.lin2ulaw(pcm(crop), 2), 2)
        write(args.output / "mulaw.wav", data)
        cases.append(dict(name="G711-mulaw", reference="reference.wav", query="mulaw.wav", expected=True, expected_original_start_sec=start/RATE))
    except ImportError:
        print("audioop unavailable: G.711 case skipped")
    if shutil.which("ffmpeg"):
        for codec, extension in [("libmp3lame", "mp3"), ("libopus", "opus")]:
            compressed = args.output / ("encoded." + extension)
            decoded = args.output / (extension + ".wav")
            subprocess.run(["ffmpeg", "-v", "error", "-y", "-i", str(args.output / "crop.wav"), "-c:a", codec, "-b:a", "24k", str(compressed)], check=True)
            subprocess.run(["ffmpeg", "-v", "error", "-y", "-i", str(compressed), "-ar", str(RATE), "-ac", "1", "-c:a", "pcm_s16le", str(decoded)], check=True)
            cases.append(dict(name=extension, reference="reference.wav", query=decoded.name, expected=True, expected_original_start_sec=start/RATE))
    else:
        print("ffmpeg unavailable: MP3/Opus cases skipped")
    (args.output / "manifest.json").write_text(json.dumps(cases, indent=2))
    print(args.output / "manifest.json")

if __name__ == "__main__":
    main()
