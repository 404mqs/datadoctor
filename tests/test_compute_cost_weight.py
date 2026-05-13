import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'modules'))
from datadoc_cost import compute_cost_weight

WEIGHTS = {"Standard_DS3_v2": 1.0, "Standard_E8d_v4": 3.0}


def test_baseline_one_worker():
    # 1.0 × (1 + 1) = 2.0
    assert compute_cost_weight("Standard_DS3_v2", 1, False, WEIGHTS) == 2.0


def test_memory_node_four_workers():
    # 3.0 × (4 + 1) = 15.0
    assert compute_cost_weight("Standard_E8d_v4", 4, False, WEIGHTS) == 15.0


def test_unknown_node_type_defaults_to_1():
    # 1.0 × (2 + 1) = 3.0
    assert compute_cost_weight("Unknown_Xyz_v99", 2, False, WEIGHTS) == 3.0


def test_photon_multiplier_applied():
    # 1.0 × (1 + 1) × 1.5 = 3.0
    assert compute_cost_weight("Standard_DS3_v2", 1, True, WEIGHTS, 1.5) == 3.0


def test_photon_not_applied_when_false():
    # is_photon=False → multiplier ignored
    assert compute_cost_weight("Standard_DS3_v2", 1, False, WEIGHTS, 1.5) == 2.0


def test_null_workers_treated_as_one():
    # None → 1 worker → (1 + 1) = 2
    assert compute_cost_weight("Standard_DS3_v2", None, False, WEIGHTS) == 2.0


def test_zero_workers_single_node_cluster():
    # 0 workers = driver only → (0 + 1) = 1 total node
    assert compute_cost_weight("Standard_DS3_v2", 0, False, WEIGHTS) == 1.0


def test_custom_photon_multiplier():
    # 1.0 × (1 + 1) × 2.0 = 4.0
    assert compute_cost_weight("Standard_DS3_v2", 1, True, WEIGHTS, 2.0) == 4.0


def test_empty_weight_map_defaults_to_1():
    assert compute_cost_weight("Standard_DS3_v2", 2, False, {}) == 3.0  # 1.0 × 3
