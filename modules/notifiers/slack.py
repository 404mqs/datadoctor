"""
Slack notifier for Data Doctor.

Sends Block Kit messages to a Slack channel with approve/reject/review buttons.
Approval buttons redirect to ephemeral Databricks jobs pre-loaded with the proposal_id.

Requirements:
  - A Slack app with chat:write, users:read, channels:read, groups:read scopes
  - Bot token stored in a Databricks secret (see datadoctor_config.yml notifications.slack)
  - Bot invited to both channels (channel_proposals and channel_audit)
"""

import json
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Dict, List, Optional

# sys.path must include the modules/ directory. The main notebook adds it at startup.
from notifiers.base import BaseNotifier


TIER_EMOJI  = {"green": "🟢", "yellow": "🟡"}
TIER_COLOR  = {"green": "#36a64f", "yellow": "#daa520"}
VALIDATION_EMOJI = {
    "passed":         "✅",
    "diffs_detected": "⚠️",
    "failed":         "❌",
    "skipped":        "⏭️",
    "no_changes":     "💤",
    "llm_error":      "💥",
    "export_error":   "🚫",
}


class SlackNotifier(BaseNotifier):

    def __init__(self, cfg: dict, dbutils):
        """
        Args:
            cfg: the full parsed datadoctor_config.yml dict
            dbutils: Databricks dbutils object (for secrets)
        """
        slack_cfg = cfg["notifications"]["slack"]
        self._channel_proposals = slack_cfg["channel_proposals"]
        self._channel_audit     = slack_cfg["channel_audit"]
        self._secret_scope      = slack_cfg["secret_scope"]
        self._secret_key        = slack_cfg["secret_key"]
        self._dbutils           = dbutils
        self._token: Optional[str] = None

    def _get_token(self) -> str:
        if self._token is None:
            self._token = self._dbutils.secrets.get(
                scope=self._secret_scope, key=self._secret_key
            )
        return self._token

    def _api(self, endpoint: str, method: str = "GET",
             body: Optional[dict] = None, params: Optional[dict] = None) -> dict:
        token = self._get_token()
        url = f"https://slack.com/api/{endpoint}"
        if params:
            url += "?" + urllib.parse.urlencode(params)
        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Authorization", f"Bearer {token}")
        req.add_header("Content-Type", "application/json; charset=utf-8")
        with urllib.request.urlopen(req) as r:
            result = json.loads(r.read().decode("utf-8"))
        if not result.get("ok"):
            raise RuntimeError(f"Slack API error: {result.get('error', 'unknown')}")
        return result

    def _post(self, channel_id: str, payload: dict) -> dict:
        body = {"channel": channel_id, **payload,
                "unfurl_links": False, "unfurl_media": False}
        return self._api("chat.postMessage", method="POST", body=body)

    def _build_proposal_payload(self, proposals: List[dict], run_ids: List[int],
                                 top_n: int, performance_gains: Optional[List[dict]]) -> dict:
        now = datetime.utcnow()
        if not run_ids:
            run_label = "No runs recorded"
        elif len(run_ids) > 1:
            run_label = f"Runs `{run_ids[0]}` + `{run_ids[1]}` (avg 2)"
        else:
            run_label = f"Run `{run_ids[0]}`"

        attachments = [{
            "color": "#4a9ed9",
            "blocks": [
                {"type": "section",
                 "text": {"type": "mrkdwn", "text": "⚙️  *Data Doctor*"}},
                {"type": "context",
                 "elements": [{"type": "mrkdwn",
                               "text": f"Daily optimization report  ·  {now.strftime('%Y-%m-%d')}  ·  {now.strftime('%H:%M')} UTC"}]},
                {"type": "context",
                 "elements": [{"type": "mrkdwn",
                               "text": f"{run_label}  ·  Top {top_n} slowest notebooks"}]},
            ]
        }]

        if performance_gains:
            gain_lines = []
            for g in performance_gains:
                key  = f"`{g['task_key']}`"
                user = g["applied_by"]
                if g["status"] == "no_ran":
                    gain_lines.append(f"⏭️  {key} — did not run in this execution _(approved by {user})_")
                elif g["status"] == "improved":
                    gain_lines.append(
                        f"✅  {key}: *{g['original_min']:.1f} min* → *{g['current_min']:.1f} min* "
                        f"_(−{abs(g['delta_pct']):.0f}%)_  approved by {user}"
                    )
                elif g["status"] == "regressed":
                    gain_lines.append(
                        f"🔴  {key}: *{g['original_min']:.1f} min* → *{g['current_min']:.1f} min* "
                        f"_(+{abs(g['delta_pct']):.0f}% — please review)_  approved by {user}"
                    )
                else:
                    gain_lines.append(
                        f"➡️  {key}: *{g['original_min']:.1f} min* → *{g['current_min']:.1f} min* "
                        f"_(no significant change)_  approved by {user}"
                    )
            attachments.append({
                "color": "#7c3aed",
                "blocks": [
                    {"type": "section",
                     "text": {"type": "mrkdwn", "text": "*📊 Applied optimization follow-up*"}},
                    {"type": "section",
                     "text": {"type": "mrkdwn", "text": "\n".join(gain_lines)}},
                ]
            })

        for i, p in enumerate(proposals, 1):
            tier_emoji = TIER_EMOJI.get(p["tier"], "❔")
            val_emoji  = VALIDATION_EMOJI.get(p["validation_status"], "❔")
            color      = TIER_COLOR.get(p["tier"], "#95a5a6")
            analisis   = p.get("analisis") or {}
            vstatus    = p["validation_status"]
            att        = []

            att.append({"type": "section",
                        "text": {"type": "mrkdwn",
                                 "text": f"*{i}. `{p['task_key']}`*  {tier_emoji} {val_emoji}\n*{p['duration_min']:.1f} min*"}})

            qh = analisis.get("que_hace") or analisis.get("what_it_does")
            if qh:
                att.append({"type": "section",
                            "text": {"type": "mrkdwn", "text": f"_{qh}_"}})

            if vstatus == "no_changes":
                att.append({"type": "section",
                            "text": {"type": "mrkdwn",
                                     "text": "💤 _No changes — LLM found no worthwhile optimizations._"}})
            elif vstatus in ("llm_error", "export_error"):
                err = analisis.get("error", "see datadoc.proposals")
                att.append({"type": "section",
                            "text": {"type": "mrkdwn",
                                     "text": f"_{val_emoji} Error: {err[:180]}_"}})
            else:
                cambios = analisis.get("cambios_aplicados") or analisis.get("changes_applied") or []
                if cambios:
                    bullets = "\n".join(
                        f"• {c.get('descripcion', c.get('description', ''))}  "
                        f"_(savings: {c.get('ahorro_estimado', c.get('estimated_savings', '?'))})_"
                        for c in cambios[:5]
                    )
                    att.append({"type": "section",
                                "text": {"type": "mrkdwn",
                                         "text": f"*Proposed changes:*\n{bullets}"}})

            footer_parts = []
            if vstatus == "passed":
                footer_parts.append("✅ Validation passed")
            elif vstatus == "diffs_detected":
                footer_parts.append("⚠️ Diffs detected — review before approving")
            elif vstatus == "failed":
                footer_parts.append("❌ Validation failed — see `datadoc.proposals`")
            elif p["tier"] == "yellow":
                if p.get("self_reference"):
                    footer_parts.append("🔄 Self-reference — notebook reads and writes the same table; auto-validation unavailable")
                else:
                    footer_parts.append("📝 No automatic validation")

            if p.get("v2_path"):
                footer_parts.append(f"v2: `{p['v2_path']}`")
            if p.get("proposal_id") and p.get("v2_path") and not p.get("approve_url"):
                footer_parts.append(f"Approve: run `datadoc_approve` with proposal_id `{p['proposal_id']}`")

            if footer_parts:
                att.append({"type": "context",
                            "elements": [{"type": "mrkdwn", "text": "\n".join(footer_parts)}]})

            btn_elements = []
            if p.get("review_url"):
                btn_elements.append({
                    "type": "button", "text": {"type": "plain_text", "text": "🔍 Review v2"},
                    "url": p["review_url"], "action_id": f"review_{(p.get('proposal_id') or '')[:8]}",
                })
            if p.get("approve_url"):
                btn_elements.append({
                    "type": "button", "text": {"type": "plain_text", "text": "✅ Approve"},
                    "style": "primary", "url": p["approve_url"],
                    "action_id": f"approve_{(p.get('proposal_id') or '')[:8]}",
                })
            if p.get("reject_url"):
                btn_elements.append({
                    "type": "button", "text": {"type": "plain_text", "text": "❌ Reject"},
                    "style": "danger", "url": p["reject_url"],
                    "action_id": f"reject_{(p.get('proposal_id') or '')[:8]}",
                })
            if btn_elements:
                att.append({"type": "actions", "elements": btn_elements})

            att.append({"type": "divider"})
            attachments.append({"color": color, "blocks": att})

        attachments.append({
            "color": "#95a5a6",
            "blocks": [{"type": "context",
                        "elements": [{"type": "mrkdwn",
                                      "text": "_Click Approve/Reject to open the pre-loaded job · Buttons expire with the next Data Doctor run_"}]}]
        })
        return {"text": " ", "attachments": attachments}

    def send_proposal(self, proposals, run_ids, top_n, performance_gains=None):
        try:
            payload = self._build_proposal_payload(proposals, run_ids, top_n, performance_gains)
            result  = self._post(self._channel_proposals, payload)
            print(f"[slack] send_proposal: ok={result.get('ok')} {result.get('error', '')}")
            return result
        except Exception as e:
            print(f"[slack] send_proposal error: {e}")
            return {"ok": False, "error": str(e)}

    def send_result(self, action, task_key, proposal_id, notes=""):
        action_emoji = {"approve": "✅", "reject": "❌", "rollback": "↩️"}.get(action, "❔")
        text = (f"{action_emoji} *{action.capitalize()}* — `{task_key}`\n"
                f"proposal `{proposal_id[:8]}`")
        if notes:
            text += f"\n_{notes}_"
        try:
            result = self._post(self._channel_audit, {"text": text})
            return result
        except Exception as e:
            print(f"[slack] send_result error: {e}")
            return {"ok": False, "error": str(e)}
