import azure.functions as func
import urllib.request
import json
import os

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "https://adb-7878126364764762.2.azuredatabricks.net")
CLUSTER_ID      = os.environ.get("CLUSTER_ID", "1120-205732-nkpi1h2k")
NOTEBOOK_PATH   = "/Workspace/Shared/DataDoctor/datadoc_approve"


def _databricks_post(token: str, path: str, body: dict) -> dict:
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(f"{DATABRICKS_HOST}{path}", data=data, method="POST")
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-Type", "application/json; charset=utf-8")
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read().decode("utf-8"))


def _html_response(emoji: str, title: str, subtitle: str, color: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="es"><head><meta charset="utf-8"><title>Data Doctor</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
         display: flex; align-items: center; justify-content: center;
         min-height: 100vh; background: #f0f2f5; }}
  .card {{ background: white; padding: 2.5rem 3rem; border-radius: 16px;
           box-shadow: 0 4px 24px rgba(0,0,0,.08); text-align: center;
           max-width: 380px; width: 90%; }}
  .emoji {{ font-size: 3.5rem; margin-bottom: 1rem; }}
  h2 {{ font-size: 1.4rem; color: {color}; margin-bottom: .5rem; }}
  .sub {{ color: #666; font-size: .95rem; margin-top: .75rem; }}
  code {{ background: #f5f5f5; padding: .15rem .45rem; border-radius: 4px; font-size: .85rem; }}
  .close {{ margin-top: 1.5rem; color: #aaa; font-size: .85rem; }}
</style></head>
<body><div class="card">
  <div class="emoji">{emoji}</div>
  <h2>{title}</h2>
  <p class="sub">{subtitle}</p>
  <p class="close">Podés cerrar esta pestaña.</p>
</div></body></html>"""


@app.route(route="qa-action", methods=["GET"])
def qa_action(req: func.HttpRequest) -> func.HttpResponse:
    proposal_id = req.params.get("proposal_id", "").strip()
    action      = req.params.get("action", "").strip()

    if not proposal_id:
        return func.HttpResponse("Falta proposal_id.", status_code=400)
    if action not in ("approve", "reject"):
        return func.HttpResponse("action debe ser 'approve' o 'reject'.", status_code=400)

    token = os.environ.get("DATABRICKS_TOKEN", "")
    if not token:
        return func.HttpResponse(
            _html_response("⚠️", "Token no configurado",
                           "Contactá al administrador de Data Doctor.", "#e67e22"),
            mimetype="text/html", status_code=500,
        )

    try:
        resp = _databricks_post(token, "/api/2.1/jobs/runs/submit", {
            "run_name": f"datadoc_{action}_{proposal_id[:8]}",
            "existing_cluster_id": CLUSTER_ID,
            "notebook_task": {
                "notebook_path": NOTEBOOK_PATH,
                "base_parameters": {
                    "proposal_id": proposal_id,
                    "accion":      action,
                },
            },
        })
        run_id = resp.get("run_id", "?")

        if action == "approve":
            html = _html_response(
                "✅", "Propuesta aprobada",
                f"Aplicando cambios…  Run ID: <code>{run_id}</code>",
                "#27ae60",
            )
        else:
            html = _html_response(
                "❌", "Propuesta rechazada",
                f"Marcada como rechazada.  Run ID: <code>{run_id}</code>",
                "#e74c3c",
            )
        return func.HttpResponse(html, mimetype="text/html", status_code=200)

    except Exception as e:
        return func.HttpResponse(
            _html_response("💥", "Error al lanzar el job",
                           str(e)[:200], "#e74c3c"),
            mimetype="text/html", status_code=500,
        )
