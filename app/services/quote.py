import json
import random
from google import genai
from app.config import QUOTE_PATH, CACHE_DIR, GEMINI_KEY


# Each invoke picks a random lens — forces Gemini into a different world every time
_LENSES = [
    "a trader who has blown up an account and clawed back",
    "a samurai with no master",
    "a boxer entering the ring as a 10-to-1 underdog",
    "a filmmaker betting everything on one shot",
    "a general the night before a battle he may lose",
    "a monk who left the monastery to make money",
    "a criminal who went straight and built an empire",
    "a philosopher who also ran a business",
    "an athlete in the final year of their career",
    "a founder who failed publicly and came back quietly",
    "a poet who only wrote about consequence",
    "a stoic who had everything taken from them twice",
    "a chess grandmaster who plays markets, not boards",
    "an architect who tears down before they build",
    "a sailor who has navigated by stars, not instruments",
]

_MOODS = [
    "cold, precise, and final — like a decision already made",
    "slow-burning, like a fire that doesn't announce itself",
    "defiant — the kind of thing said right before someone proves everyone wrong",
    "quiet and devastating — the kind of line you read once and never forget",
    "blunt and without apology — no metaphor, just fact",
    "ancient but shockingly current — could have been said this morning",
    "cinematic — belongs in the last scene of a great film",
    "streetwise — earned, not studied",
    "almost unbearably honest — the thing people think but never say",
    "the kind of sentence that makes you put down your phone and think",
]

_DOMAINS = [
    "the cost of playing it safe your entire life",
    "what separates those who build from those who watch",
    "the silence and discipline before a large irreversible bet",
    "the loneliness of long-term thinking",
    "why most people confuse movement with progress",
    "the dignity of taking a risk nobody else would take",
    "what money actually measures — and what it doesn't",
    "the violence of hesitation compounded over years",
    "conviction without external validation",
    "what it means to be early, wrong, and still right",
    "the relationship between fear and the size of your ambitions",
    "why the scoreboard is always delayed",
]

PROMPT = """You are a collector of words that cut.

Inhabit the perspective of {lens}.

Find ONE real quote — spoken, written, or whispered by a historical figure, warrior, trader, filmmaker, philosopher, athlete, general, criminal, saint, or left anonymous — that carries this specific energy:

Mood: {mood}

Domain: {domain}

The quote must pass this single test:
Does it make you want to close the tab and go do something that matters?
If not, it does not qualify.

Draw from anywhere — cinema scripts, Stoic letters, locker rooms, trading floors, war rooms, literature, philosophy, the streets, or the silence between great decisions. Reach for the uncommon. The quote that most people have not heard yet.

Hard rules:
- Real quote, real source (or "Anonymous" if origin is genuinely unclear)
- No soft edges. No inspiration-poster energy. No LinkedIn.
- DO NOT return this quote: "{previous_quote}"

Return ONLY valid JSON. Nothing else.
"""


class QuoteStore:
    def __init__(self, model: str = "gemini-2.5-flash"):
        self.client = genai.Client(api_key=GEMINI_KEY)
        self.model = model

    def write(self) -> dict:
        previous = ""
        if QUOTE_PATH.exists():
            try:
                previous = json.loads(QUOTE_PATH.read_text(encoding="utf-8")).get(
                    "quote", ""
                )
            except Exception:
                pass

        prompt = PROMPT.format(
            lens=random.choice(_LENSES),
            mood=random.choice(_MOODS),
            domain=random.choice(_DOMAINS),
            previous_quote=previous,
        )

        response = self.client.models.generate_content(
            model=self.model,
            contents=prompt,
            config={
                "response_mime_type": "application/json",
                "temperature": 1.9,
                "response_json_schema": {
                    "type": "object",
                    "properties": {
                        "quote": {"type": "string"},
                        "author": {"type": "string"},
                    },
                    "required": ["quote", "author"],
                },
            },
        )
        data = json.loads(response.text)
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        QUOTE_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")
        return data

    @staticmethod
    def read() -> dict:
        return json.loads(QUOTE_PATH.read_text(encoding="utf-8"))
