"""
Base notifier interface for Data Doctor.

To add a custom notifier (Teams, email, etc.):
  1. Create a new file in this directory (e.g., teams.py)
  2. Subclass BaseNotifier and implement send_proposal() and send_result()
  3. Set notifications.type in datadoctor_config.yml to your notifier name
  4. Instantiate your notifier in notebooks/datadoc.py where SlackNotifier is instantiated
"""

from abc import ABC, abstractmethod
from typing import List, Optional


class BaseNotifier(ABC):

    @abstractmethod
    def send_proposal(
        self,
        proposals: List[dict],
        run_ids: List[int],
        top_n: int,
        performance_gains: Optional[List[dict]] = None,
    ) -> dict:
        """
        Send optimization proposals to the team for review.

        Args:
            proposals: list of proposal dicts, each containing:
                task_key, duration_min, tier, proposal_id, v2_path,
                validation_status, analisis, approve_url, reject_url, review_url
            run_ids: list of Databricks run IDs used for averaging durations
            top_n: how many notebooks were analyzed
            performance_gains: list of previously approved optimizations to report on

        Returns:
            dict with at least {"ok": bool}
        """
        ...

    @abstractmethod
    def send_result(self, action: str, task_key: str, proposal_id: str,
                    notes: str = "") -> dict:
        """
        Send approve/reject/rollback confirmation to the audit channel.

        Args:
            action: "approve", "reject", or "rollback"
            task_key: the Databricks task key that was acted on
            proposal_id: UUID of the proposal
            notes: optional human notes from the approver

        Returns:
            dict with at least {"ok": bool}
        """
        ...


class NullNotifier(BaseNotifier):
    """No-op notifier — logs to stdout. Use when notifications.type = 'none'."""

    def send_proposal(self, proposals, run_ids, top_n, performance_gains=None):
        print(f"[NullNotifier] {len(proposals)} proposal(s) ready. "
              f"Query datadoc.proposals to review.")
        return {"ok": True}

    def send_result(self, action, task_key, proposal_id, notes=""):
        print(f"[NullNotifier] {action} — {task_key} ({proposal_id[:8]})")
        return {"ok": True}
