import json
from scripts.applications.rebuild import APPOUT, build_reports
if __name__ == "__main__":
    manifest=json.loads((APPOUT/"applications_rebuild_manifest.json").read_text())
    s=manifest["summaries"]
    build_reports(manifest["release"],s["mechanism"],s["network"],s["outliers"],s["conversion"],s["capital"],s["latency"],s["concentration"],s["semantic"],s["geography"],s["geographic_validation"])
