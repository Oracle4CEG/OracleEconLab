"""Required entry point; the implemented nonnegative multi-view model is integrative NMF."""
from scripts.applications.oata.common import TRACKS, run_multiview_nmf

if __name__ == "__main__":
    for version in ("full", "prefix"):
        for track in TRACKS:
            print(run_multiview_nmf(track, version))
