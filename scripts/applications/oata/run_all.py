from scripts.applications.oata.common import (
    build_annotation_sample, build_consensus_archetypes, build_episodes,
    build_reports, build_tracks_and_splits, build_trajectory_views,
    evaluate_stability_and_transfer, expand_weights_to_all_episodes,
    map_states, render_figures, run_all_models,
)

if __name__ == "__main__":
    print(build_episodes())
    print(map_states())
    print(build_trajectory_views())
    print(build_tracks_and_splits())
    print(run_all_models())
    print(build_consensus_archetypes())
    print(evaluate_stability_and_transfer())
    print(expand_weights_to_all_episodes())
    print(build_annotation_sample())
    render_figures()
    print(build_reports())
