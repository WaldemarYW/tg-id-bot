import re
from typing import Tuple, Optional


def extract_text_and_media(message) -> Tuple[str, Optional[str], Optional[str], int]:
    """Extract text, media_type, file_id, is_forward from a Telegram message.

    * text is taken from the message caption or text if present.
    * media_type is one of: photo, video, audio, voice, document.  None if no media.
    * file_id is the Telegram file_id for the media (to allow forwarding later).
    * is_forward is 1 if the message is a forward, 0 otherwise.
    """
    text = None
    media_type = None
    file_id = None
    is_forward = 1 if message.forward_date else 0

    # Prefer caption over text for media messages
    if message.caption:
        text = message.caption
    elif message.text:
        text = message.text

    if message.photo:
        media_type = "photo"
        file_id = message.photo[-1].file_id  # take highest resolution
    elif message.video:
        media_type = "video"
        file_id = message.video.file_id
    elif message.audio:
        media_type = "audio"
        file_id = message.audio.file_id
    elif message.voice:
        media_type = "voice"
        file_id = message.voice.file_id
    elif message.document:
        media_type = "document"
        file_id = message.document.file_id

    return text, media_type, file_id, is_forward


def extract_male_ids(text: str) -> list:
    """Return a list of all 10‑digit male IDs found in the given text.  IDs
    are recognised only when delimited by non‑digit characters to avoid
    accidental extraction from longer numbers (e.g. phone numbers).
    """
    if not text:
        return []
    pattern = re.compile(r"(?<!\d)(\d{10})(?!\d)")
    return pattern.findall(text)


def valid_id(val: str) -> bool:
    return bool(re.fullmatch(r"\d{10}", val))