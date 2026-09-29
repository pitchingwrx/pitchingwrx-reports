"""
Reads a food/nutrition photo via a vision-capable Claude model and returns structured macro
data -- for either of two cases the same "Scan" button now covers:
  1. A printed Nutrition Facts label -- read the values EXACTLY as printed, never estimated.
  2. A plain photo of food with no label -- identify what's visible and ESTIMATE macros, since
     a 2D photo alone can't measure real mass, hidden oils/sauces, or exact portion size.
The model itself decides which case it's looking at (photo_type) in one pass, rather than a
separate classify-then-route step, since the same reasoning that would classify the photo has
to look at it closely anyway. Case 2 is inherently much less accurate than case 1 -- that's a
property of photo-based food estimation generally (true of every vendor doing this, not
something a better prompt fixes), so the response always says which case it got and, for case
2, gives a range and a short breakdown of what was identified instead of one falsely-precise
number -- the caller (pitchingwrx.html) surfaces that distinction to the athlete rather than
presenting both as equally trustworthy.

This is the same model IP/data boundary the Stuff+ scoring endpoint already established: the
request only ever contains a photo the caller took themselves (never any of this org's own
stored data), and the response is only ever the extracted/estimated numbers -- never the model,
never the raw prompt.

Model choice: Sonnet, not Haiku. Reading a label is narrow OCR-style extraction (Haiku was
right for that alone), but estimating a plate of food needs real reasoning -- identifying
multiple distinct items, judging portion size off visual cues (plate/utensil scale), and
combining that into a calorie/macro estimate. Forced tool-use gets a reliable JSON shape back
either way, instead of parsing free-text.
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

# Long-edge cap in pixels -- plenty of resolution to read printed label text or judge a plated
# meal's portions, while keeping per-scan vision cost small and predictable regardless of how
# large the original phone photo is.
MAX_DIM = 1024

def prepare_scan_image(raw_bytes):
    img = Image.open(io.BytesIO(raw_bytes))
    img = img.convert('RGB')
    w, h = img.size
    if max(w, h) > MAX_DIM:
        scale = MAX_DIM / max(w, h)
        img = img.resize((max(1, int(w * scale)), max(1, int(h * scale))))
    buf = io.BytesIO()
    img.save(buf, format='JPEG', quality=85)
    return base64.b64encode(buf.getvalue()).decode('utf-8'), 'image/jpeg'

NUTRITION_SCAN_TOOL = {
    "name": "record_nutrition_scan",
    "description": "Record what was found in a nutrition/food photo -- either a printed Nutrition Facts label (read exactly) or a plate/container of food with no such label (estimate).",
    "input_schema": {
        "type": "object",
        "properties": {
            "photo_type": {
                "type": "string",
                "enum": ["label", "food", "unclear"],
                "description": "'label' if a real, legible Nutrition Facts panel is visible; 'food' if it shows an actual meal/food/drink with no such panel; 'unclear' if neither applies (blurry, not food, empty plate, etc.)",
            },
            "food_name": {"type": ["string", "null"], "description": "for 'label': the product/food name if visible on the packaging. For 'food': a short name for the overall meal, e.g. 'Grilled chicken, rice, broccoli'."},
            "items_desc": {"type": ["string", "null"], "description": "'food' only: a short breakdown of each distinct item identified and its estimated portion, e.g. 'Chicken breast ~4oz, white rice ~1 cup, broccoli ~1 cup'. Null for 'label' or 'unclear'."},
            "serving_desc": {"type": ["string", "null"], "description": "for 'label': the label's own printed serving size text, e.g. '2/3 cup (55g)'. For 'food': the total estimated portion in plain terms, e.g. 'about 1 dinner plate'."},
            "calories": {"type": ["number", "null"], "description": "'label': the exact printed value. 'food': your single best-estimate calorie count for the whole visible portion."},
            "calories_low": {"type": ["number", "null"], "description": "'food' only: low end of a realistic calorie range for this portion (roughly 20-25% below the estimate). Always null for 'label', since that value is exact, not a range."},
            "calories_high": {"type": ["number", "null"], "description": "'food' only: high end of a realistic calorie range for this portion (roughly 20-25% above the estimate). Always null for 'label'."},
            "protein_g": {"type": ["number", "null"]},
            "carb_g": {"type": ["number", "null"]},
            "fat_g": {"type": ["number", "null"]},
        },
        "required": ["photo_type"],
    },
}

_PROMPT_TEXT = (
    "This photo is one of three things: (a) a printed Nutrition Facts label, (b) an actual "
    "plate/container of food or a drink with no such label visible, or (c) neither (blurry, "
    "not food-related, etc.). Decide which, then record it using the tool.\n\n"
    "If (a): read the printed values EXACTLY as shown, per the label's own listed serving size "
    "(not per container, unless that's the only value printed). Leave calories_low and "
    "calories_high null -- do not turn an exact printed value into a range, and do not guess "
    "or estimate any value that isn't actually printed.\n\n"
    "If (b): identify each distinct food/drink item visible and estimate its portion size using "
    "visual cues (plate size, typical utensil/container size, comparison to standard servings). "
    "Estimate total calories and macros for the whole visible portion as your best single guess "
    "in 'calories', then set calories_low/calories_high to a realistic range around it (roughly "
    "20-25% either side) -- a photo alone can't measure exact mass, cooking oil, or sauces, so "
    "the range should reflect genuine uncertainty, not false precision. In items_desc, clearly "
    "list what you identified and its estimated size, so the person can correct anything you "
    "got wrong before logging it.\n\n"
    "If (c): set photo_type to 'unclear' and leave every other field null."
)

def scan_nutrition_photo(raw_bytes):
    """Returns a dict of extracted/estimated fields, or None if the photo was 'unclear'."""
    image_b64, media_type = prepare_scan_image(raw_bytes)
    client = _get_client()
    resp = client.messages.create(
        model="claude-sonnet-5-5",
        max_tokens=1024,
        tools=[NUTRITION_SCAN_TOOL],
        tool_choice={"type": "tool", "name": "record_nutrition_scan"},
        messages=[{
            "role": "user",
            "content": [
                {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": image_b64}},
                {"type": "text", "text": _PROMPT_TEXT},
            ],
        }],
    )
    for block in resp.content:
        if getattr(block, "type", None) == "tool_use" and block.name == "record_nutrition_scan":
            data = block.input
            if data.get("photo_type") not in ("label", "food"):
                return None
            return {
                "photo_type": data.get("photo_type"),
                "name": data.get("food_name"),
                "items_desc": data.get("items_desc"),
                "serving_desc": data.get("serving_desc"),
                "calories": data.get("calories"),
                "calories_low": data.get("calories_low"),
                "calories_high": data.get("calories_high"),
                "protein_g": data.get("protein_g"),
                "carb_g": data.get("carb_g"),
                "fat_g": data.get("fat_g"),
            }
    return None
