from scripts.applications.oata.common import TRACKS, run_optimal_matching

if __name__ == "__main__":
    for version in ("full", "prefix"):
        for track in TRACKS:
            print(run_optimal_matching(track, version))
