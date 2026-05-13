"""Cluster cost weight computation for ROI-based task ranking."""

from typing import Optional


def compute_cost_weight(
    node_type: str,
    num_workers: Optional[int],
    is_photon: bool,
    weight_map: dict,
    photon_multiplier: float = 1.5,
) -> float:
    """Returns the relative DBU cost weight for a cluster configuration.

    Formula: node_weight × (num_workers + 1) × photon_multiplier
    - node_weight:        from weight_map[node_type], default 1.0 if unknown
    - (num_workers + 1):  driver counts as one node; None treated as 1 worker
    - photon_multiplier:  applied only when is_photon=True

    Examples:
        Standard_DS3_v2, 1 worker, no Photon, weights={DS3: 1.0}  → 1.0 × 2 = 2.0
        Standard_E8d_v4, 4 workers, no Photon, weights={E8: 3.0}  → 3.0 × 5 = 15.0
        Standard_DS3_v2, 1 worker, Photon,     weights={DS3: 1.0} → 1.0 × 2 × 1.5 = 3.0
    """
    node_weight = float(weight_map.get(node_type, 1.0))
    workers = num_workers if num_workers is not None else 1
    weight = node_weight * (workers + 1)
    if is_photon:
        weight *= photon_multiplier
    return round(weight, 2)
