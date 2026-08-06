from scripts.applications.oata.common import TRACKS, run_soft_dtw

if __name__ == "__main__":
    for version in ("full", "prefix"):
        for track in TRACKS:
            print(run_soft_dtw(track, version))
