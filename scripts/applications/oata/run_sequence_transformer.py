from scripts.applications.oata.common import TRACKS, run_sequence_transformer

if __name__ == "__main__":
    for version in ("full", "prefix"):
        for track in TRACKS:
            print(run_sequence_transformer(track, version))
