import os
import json
import uuid
import time
import base64
import logging
import urllib.request
import urllib.parse
import websocket
import runpod

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SERVER_ADDRESS = os.getenv("SERVER_ADDRESS", "127.0.0.1")
CLIENT_ID = str(uuid.uuid4())

INPUT_DIR = "/input"
os.makedirs(INPUT_DIR, exist_ok=True)


# ---------------- CUDA CHECK ----------------
def check_cuda():
    import torch
    if not torch.cuda.is_available():
        raise RuntimeError("CUDA is required but not available")
    os.environ["CUDA_VISIBLE_DEVICES"] = "0"
    logger.info("✅ CUDA available")


check_cuda()


# ---------------- COMFY HELPERS ----------------
def queue_prompt(prompt: dict) -> str:
    url = f"http://{SERVER_ADDRESS}:8188/prompt"
    payload = {
        "prompt": prompt,
        "client_id": CLIENT_ID
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data)
    res = json.loads(urllib.request.urlopen(req).read())
    return res["prompt_id"]


def wait_for_execution(ws, prompt_id: str):
    while True:
        msg = ws.recv()
        if isinstance(msg, str):
            data = json.loads(msg)
            if data["type"] == "executing":
                if data["data"]["node"] is None and data["data"]["prompt_id"] == prompt_id:
                    return


def get_history(prompt_id: str) -> dict:
    url = f"http://{SERVER_ADDRESS}:8188/history/{prompt_id}"
    with urllib.request.urlopen(url) as r:
        return json.loads(r.read())[prompt_id]


def get_image(filename, subfolder, folder_type) -> bytes:
    params = urllib.parse.urlencode({
        "filename": filename,
        "subfolder": subfolder,
        "type": folder_type
    })
    url = f"http://{SERVER_ADDRESS}:8188/view?{params}"
    with urllib.request.urlopen(url) as r:
        return r.read()


# ---------------- HANDLER ----------------
def handler(job):
    job_input = job["input"]

    workflow = job_input["workflow"]
    images = job_input.get("images", [])

    logger.info("📥 job received")
    logger.info(f"workflow nodes: {len(workflow)}")
    logger.info(f"images: {len(images)}")

    # ---- save input images ----
    for img in images:
        name = img["name"]
        data = base64.b64decode(img["image"])
        path = os.path.join(INPUT_DIR, name)
        with open(path, "wb") as f:
            f.write(data)
        logger.info(f"🖼 saved input image → {path}")

    # ---- ensure ComfyUI is up ----
    for _ in range(120):
        try:
            urllib.request.urlopen(f"http://{SERVER_ADDRESS}:8188", timeout=2)
            break
        except Exception:
            time.sleep(1)
    else:
        raise RuntimeError("ComfyUI not responding")

    # ---- websocket ----
    ws_url = f"ws://{SERVER_ADDRESS}:8188/ws?clientId={CLIENT_ID}"
    ws = websocket.WebSocket()
    ws.connect(ws_url)

    # ---- queue ----
    prompt_id = queue_prompt(workflow)
    logger.info(f"🚀 queued prompt {prompt_id}")

    wait_for_execution(ws, prompt_id)
    ws.close()

    history = get_history(prompt_id)

    # ---- collect images ----
    output_images = []

    for node in history["outputs"].values():
        if "images" not in node:
            continue
        for img in node["images"]:
            raw = get_image(
                img["filename"],
                img["subfolder"],
                img["type"]
            )
            output_images.append({
                "name": img["filename"],
                "data": base64.b64encode(raw).decode("utf-8")
            })

    if not output_images:
        return {"error": "No images generated"}

    return {
        "images": output_images
    }


runpod.serverless.start({
    "handler": handler
})
