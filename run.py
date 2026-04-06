import os
import uvicorn

if __name__ == "__main__":
    port = int(os.environ.get("DATABRICKS_APP_PORT", "8000"))
    print(f"[DEBUG] DATABRICKS_APP_PORT = {os.environ.get('DATABRICKS_APP_PORT')}")
    print(f"[DEBUG] Using port = {port}")
    uvicorn.run("app.main:app", host="0.0.0.0", port=port)
