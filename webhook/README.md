# Data Doctor — Optional Webhook (Azure Function)

By default, Slack approve/reject buttons redirect to ephemeral Databricks jobs (pre-loaded
with the `proposal_id`). Clicking a button opens the job page — one more click on "Run Now"
applies the action.

The Azure Function in this folder replaces that with a **true one-click experience**:
clicking the button calls the function directly, which triggers `datadoc_approve` and
returns a confirmation page.

## Setup

1. Create an Azure Function App (Python 3.11, Consumption plan, Linux)
2. Set these Application Settings in the Azure portal:

| Key | Value |
|---|---|
| `DATABRICKS_HOST` | Your workspace URL |
| `DATABRICKS_TOKEN` | A Databricks PAT with `jobs/runs/submit` permission |
| `CLUSTER_ID` | Your all-purpose cluster ID |

3. Deploy:

```bash
cd webhook/
func azure functionapp publish <your-function-app-name> --python
```

4. Get the function URL with its key:
```
https://<app>.azurewebsites.net/api/qa-action?code=<FUNCTION_KEY>
```

5. Set `AZURE_FUNCTION_URL` in `datadoc.py` and update `create_button_jobs()` to use it
   instead of ephemeral Databricks jobs. See the comment in that function.

## Security note

The `?code=` parameter authenticates requests to the function. Anyone with the URL can
trigger approve/reject actions. Keep it private and rotate it if exposed.
