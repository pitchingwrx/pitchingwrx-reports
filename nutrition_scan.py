"""
Reads a photographed Nutrition Facts label via a vision-capable Claude model and returns
structured macro data. This is the model IP boundary the whole Stuff+ scoring endpoint
already established: the request only ever contains a photo the caller took themselves
(never any of this org's own stored data), and the response is only ever the extracted
numbers -- never the model, never the raw prompt.

Model choice: Haiku, not Sonnet/Opus. This is "read printed text off a photo into a few
numeric fields" -- narrow, high-volume (expected to be used heavily by diligent athletes
logging every meal), and doesn't need heavier reasoning. Forced tool-use gets a reliable
JSON shape back instead of parsing free-text, which is the standard robust pattern for
extraction tasks like this.
"""
import os
import io
import base64
from anthropic import Anthropic
from PIL import Image

_client = None
def _get_client():
    global _client
    if _client is None:
        _client = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    return _client

# Long-edge cap in pixels -- plenty of resolution to read printed label text legibly,
# while keeping the per-scan vision cost small and predictable regardless of how large
# the original phone photo is.
MAX_DIM = 1024

def prepare_label_image(raw_bytes):
    img = Image.open(io.BytesIO(raw_bytes))
    img = img.convert('RGB')
    w, h = img.size
    if max(w, h) > MAX_DIM:
        scale = MAX_DIM / max(w, h)
        img = img.resize((max(1, int(w * scale)), max(1, int(h * scale))))
    buf = io.BytesIO()
    img.save(buf, format='JPEG', quality=85)
    return base64.b64encode(buf.getvalue()).decode('utf-8'), 'image/jpeg'

LABEL_TOOL = {
    "name": "record_nutrition_facts",
    "description": "Record the nutrition facts extracted from a Nutrition Facts label photo.",
    "input_schema": {
        "type": "object",
        "properties": {
            "found_label": {
                "type": "boolean",
                "description": "true only if a real, legible Nutrition Facts panel is visible in the image",
            },
            "food_name": {"type": ["string", "null"], "description": "product/food name if visible on the packaging, else null"},
            "serving_desc": {"type": ["string", "null"], "description": "the label's own serving size text, e.g. '2/3 cup (55g)'"},
            "calories": {"type": ["number", "null"]},
            "protein_g": {"type": ["number", "null"]},
            "carb_g": {"type": ["number", "null"]},
            "fat_g": {"type": ["number", "null"]},
        },
        "required": ["found_label"],
    },
}

def read_nutrition_label(raw_bytes):
    """Returns a dict of extracted fields, or None if no real label was found in the photo."""
    image_b64, media_type = prepare_label_image(raw_bytes)
    client = _get_client()
    resp = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=1024,
        tools=[LABEL_TOOL],
        tool_choice={"type": "tool", "name": "record_nutrition_facts"},
        messages=[{
            "role": "user",
            "content": [
                {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": image_b64}},
                {
                    "type": "text",
                    "text": (
                        "This is a photo of a food's Nutrition Facts label. Read the printed "
                        "values exactly as shown, per the label's own listed serving size (not "
                        "per container, unless that's the only value printed), and record them "
                        "using the tool. If no real, legible nutrition label is visible in the "
                        "photo, set found_label to false and leave every other field null -- "
                        "do not guess or estimate values that aren't actually printed."
                    ),
                },
            ],
        }],
    )
    for block in resp.content:
        if getattr(block, "type", None) == "tool_use" and block.name == "record_nutrition_facts":
            data = block.input
            if not data.get("found_label"):
                return None
            return {
                "name": data.get("food_name"),
                "serving_desc": data.get("serving_desc"),
                "calories": data.get("calories"),
                "protein_g": data.get("protein_g"),
                "carb_g": data.get("carb_g"),
                "fat_g": data.get("fat_g"),
            }
    return None
