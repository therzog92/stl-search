"""Launch the STL Search web UI."""

import os

import uvicorn

if __name__ == "__main__":
    # 0.0.0.0 = reachable from phone/other devices on the same Wi‑Fi
    # Override with STL_HOST=127.0.0.1 if you want localhost-only again
    host = os.getenv("STL_HOST", "0.0.0.0")
    port = int(os.getenv("STL_PORT", "8787"))
    uvicorn.run("app.main:app", host=host, port=port, reload=False)
