from scripts.applications.oata.common import TRACKS, run_gower_pam

if __name__ == "__main__":
    for version in ("full", "prefix"):
        for track in TRACKS:
            print(run_gower_pam(track, version))
