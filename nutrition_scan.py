"""
Reads a food/nutrition photo via a vision-capable Claude model and returns structured macro
data -- for any of three cases the same "Scan" button now covers:
  1. A printed Nutrition Facts label -- read the values EXACTLY as printed, never estimated.
  2. A recognized packaged/branded product with no visible label (e.g. a sealed candy wrapper,
     a named snack bag) -- RECALL that specific product's typical published nutrition, rather
     than guessing from the photo. This is recall, not estimation, so it's meaningfully more
     reliable than case 3 -- kept as its own case (not folded into case 1 or 3) so the response
     can say which kind of confidence it actually has.
  3. A plain photo of food with no label and no identifiable branded product -- identify what's
     visible and ESTIMATE macros, since a 2D photo alone can't measure real mass, hidden
     oils/sauces, or exact portion size.
The model itself decides which case it's looking at (photo_type) in one pass, rather than a
separate classify-then-route step, since the same reasoning that would classify the photo has
to look at it closely anyway. Cases 2 and 3 are inherently less accurate than case 1 -- that's
a property of photo-based food identification generally (true of every vendor doing this, not
something a better prompt fixes) -- so the response always says which case it got and, for
cases 2/3, gives a range (tight for 2, wide for 3) and a short note on what was identified
instead of one falsely-precise number -- the caller (pitchingwrx.html) surfaces that distinction
to the athlete rather than presenting all three as equally trustworthy.

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
from PIL import Image, ImageOps

_client = None
def _get_client():
    global _client
    if _client is None:
        _client = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    return _client

# Long-edge cap in pixels. 1024 turned out too aggressive in real use: a label that fills the
# whole frame reads fine at that size, but a real "container of food" photo (the common case)
# has the label as only part of the shot -- once downscaled to 1024 total, that label region
# can shrink to a couple hundred pixels, blurring 8-10pt printed text past legibility even
# though it reads fine on the phone at full resolution. Claude's own vision pipeline already
# handles images cleanly up to ~1568px on the long edge before doing any resizing of its own,
# so capping lower than that was throwing away real resolution for no cost benefit -- anything
# at or under 1568 costs the same either way. Matches that ceiling instead.
MAX_DIM = 1568

def prepare_scan_image(raw_bytes):
    img = Image.open(io.BytesIO(raw_bytes))
    # Most phone cameras (iPhones especially) save many photos in landscape sensor orientation
    # plus an EXIF "Orientation" tag telling viewers to rotate it for display -- every photo
    # app respects that automatically, which is why a photo looks upright on the phone, but
    # PIL.Image.open() does NOT apply it. Without this, the raw pixels handed to the model can
    # be sideways or upside-down while looking completely normal to a human -- almost certainly
    # why a photo Alex confirmed was clearly legible still came back "could not identify."
    # exif_transpose() bakes the rotation/flip into the actual pixels and drops the now-stale
    # orientation tag, so every downstream step (resize, resave) works on a right-side-up image.
    img = ImageOps.exif_transpose(img)
    img = img.convert('RGB')
    w, h = img.size
    if max(w, h) > MAX_DIM:
        scale = MAX_DIM / max(w, h)
        img = img.resize((max(1, int(w * scale)), max(1, int(h * scale))))
    buf = io.BytesIO()
    # Quality bumped slightly (85->92) alongside the resolution increase -- JPEG artifacting at
    # 85 was a secondary, smaller contributor to the same problem: fine printed text is exactly
    # where compression blocking shows up first.
    img.save(buf, format='JPEG', quality=92)
    return base64.b64encode(buf.getvalue()).decode('utf-8'), 'image/jpeg'

NUTRITION_SCAN_TOOL = {
    "name": "record_nutrition_scan",
    "description": "Record what was found in a nutrition/food photo -- a printed Nutrition Facts label (read exactly), a recognized packaged/branded product with no visible label (recall its typical known nutrition), a plate/container of food with no label or brand (visually estimate), or none of those.",
    "input_schema": {
        "type": "object",
        "properties": {
            "photo_type": {
                "type": "string",
                "enum": ["label", "product", "food", "unclear"],
                "description": "'label' if a real, legible Nutrition Facts panel is visible. 'product' if no such panel is visible but you can specifically identify a well-known packaged/branded food or drink product by its packaging/appearance (e.g. a candy wrapper, a named snack bag, a branded bottle) -- even sealed/unopened. 'food' if it shows actual visible food/drink with no label and no identifiable specific branded product (a home-plated meal, loose/unbranded items). 'unclear' if none of those apply (blurry, not food-related, etc.).",
            },
            "food_name": {"type": ["string", "null"], "description": "for 'label' or 'product': the specific product/food name (e.g. 'Peanut M&M's Fun Size'). For 'food': a short name for the overall meal, e.g. 'Grilled chicken, rice, broccoli'."},
            "items_desc": {"type": ["string", "null"], "description": "for 'product': one line naming what was recognized and that the values are that product's typical published nutrition, not a visual guess, e.g. 'Recognized as Peanut M&M's Fun Size (1.48oz/42g bag) -- using this product's typical nutrition.' For 'food': a short breakdown of each distinct item identified and its estimated portion, e.g. 'Chicken breast ~4oz, white rice ~1 cup, broccoli ~1 cup'. Null for 'label' or 'unclear'."},
            "serving_desc": {"type": ["string", "null"], "description": "for 'label': the label's own printed serving size text, e.g. '2/3 cup (55g)'. For 'product': that product's standard single-unit serving, e.g. '1 fun size bag (17g)'. For 'food': the total estimated portion in plain terms, e.g. 'about 1 dinner plate'."},
            "calories": {"type": ["number", "null"], "description": "'label': the exact printed value. 'product': the recognized product's typical calorie count for its standard serving. 'food': your single best-estimate calorie count for the whole visible portion."},
            "calories_low": {"type": ["number", "null"], "description": "'product' or 'food' only: low end of a realistic range around the estimate -- tight (~5-10%) for 'product' since it's recalled fact, not a visual guess, wider (~20-25%) for 'food' since portion/mass is genuinely uncertain from a photo alone. Always null for 'label', since that value is exact, not a range."},
            "calories_high": {"type": ["number", "null"], "description": "'product' or 'food' only: high end of the same range described for calories_low. Always null for 'label'."},
            "protein_g": {"type": ["number", "null"]},
            "carb_g": {"type": ["number", "null"]},
            "fat_g": {"type": ["number", "null"]},
        },
        "required": ["photo_type"],
    },
}

_PROMPT_TEXT = (
    "This photo is one of four things: (a) a printed Nutrition Facts label, (b) a specific, "
    "well-known packaged/branded food or drink product with no such label visible (including "
    "sealed/unopened packaging you can identify by its appearance -- a candy wrapper, a named "
    "snack bag, a branded bottle or can), (c) actual visible food/drink with no label and no "
    "identifiable specific branded product (a home-plated meal, loose or unbranded items), or "
    "(d) none of those (blurry, not food-related, etc.). Decide which, then record it using "
    "the tool.\n\n"
    "If (a): read the printed values EXACTLY as shown, per the label's own listed serving size "
    "(not per container, unless that's the only value printed). Leave calories_low and "
    "calories_high null -- do not turn an exact printed value into a range, and do not guess "
    "or estimate any value that isn't actually printed.\n\n"
    "If (b): you likely already know this exact product's typical published nutrition from "
    "general knowledge (the same way you'd recognize a Snickers bar or a can of Coke) -- use "
    "that, for the product's standard single-unit serving, rather than guessing from what's "
    "visible in the photo. This is recall, not visual estimation, so keep calories_low/high a "
    "tight range (~5-10% either side) reflecting only real uncertainty (e.g. which exact size "
    "variant this is), not portion guesswork. If you cannot confidently name the specific "
    "product, do not force this case -- fall through to (c) or (d) instead.\n\n"
    "If (c): identify each distinct food/drink item visible and estimate its portion size using "
    "visual cues (plate size, typical utensil/container size, comparison to standard servings). "
    "Estimate total calories and macros for the whole visible portion as your best single guess "
    "in 'calories', then set calories_low/calories_high to a realistic range around it (roughly "
    "20-25% either side) -- a photo alone can't measure exact mass, cooking oil, or sauces, so "
    "the range should reflect genuine uncertainty, not false precision. In items_desc, clearly "
    "list what you identified and its estimated size, so the person can correct anything you "
    "got wrong before logging it.\n\n"
    "If (d): set photo_type to 'unclear' and leave every other field null."
)

def scan_nutrition_photo(raw_bytes):
    """Returns a dict of extracted/estimated fields, or None if the photo was 'unclear'."""
    image_b64, media_type = prepare_scan_image(raw_bytes)
    client = _get_client()
    resp = client.messages.create(
        model="claude-sonnet-4-5-20250929",
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
            if data.get("photo_type") not in ("label", "product", "food"):
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
