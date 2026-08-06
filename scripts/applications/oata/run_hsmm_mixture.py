from scripts.applications.oata.common import TRACKS, run_hsmm_mixture

if __name__ == "__main__":
    for version in ("full", "prefix"):
        for track in TRACKS:
            print(run_hsmm_mixture(track, version))
