import json
import random
from app.config import QUOTE_PATH, GEMINI_KEY
from app.data.files import atomic_write_json
from google import genai
from google.genai import types

# Use SystemRandom so entropy is re-pulled from the OS on every single call,
# even if this process stays warm and write() is called repeatedly.
_rng = random.SystemRandom()

HISTORY_PATH = QUOTE_PATH.with_name("quote_history.json")
HISTORY_LIMIT = 10  # remember last N authors, ban them from the next pull

_LENSES = [
    # --- Street / Hustle ---
    "a first-generation immigrant who built a business with no safety net and no fallback",
    "a drug dealer who went legitimate and never lost the hunger",
    "a dropout who outperformed every person who doubted them — quietly, without announcement",
    "someone who grew up with nothing and refused to let that be a ceiling",
    "a street fighter who learned discipline and became dangerous in a different way",
    # --- Markets / Finance ---
    "a trader who blew up an account, lost everything, and came back colder and better",
    "a short-seller who was right for three years before the market agreed",
    "a venture capitalist who backed the thing everyone called stupid — and was proven right",
    "a poker player who understands that variance is not the same as being wrong",
    "an options trader who treats uncertainty as raw material, not as fear",
    # --- War / Strategy ---
    "a general the night before a battle they may not survive",
    "a soldier who has seen what hesitation costs — in blood, not theory",
    "Hannibal crossing the Alps, doing the thing every advisor said was impossible",
    "Leonidas at Thermopylae — 300 men, a choice already made, no regrets left to have",
    "a special forces operator who operates in the space between decision and consequence",
    "Sun Tzu — not the aphorisms, but the man who understood that war is decided before it begins",
    # --- Myth / Archetype ---
    "Prometheus — who stole fire from the gods, knowing the punishment, and did it anyway",
    "Icarus — not as a cautionary tale, but as someone who chose altitude over safety and meant it",
    "Achilles — told he would die young at Troy, who ran toward it because the alternative was obscurity",
    "Odysseus — who clawed home through ten years of wreckage by being smarter and more stubborn",
    "Sisyphus — reframed: not cursed, but a man who found his entire identity in the push",
    "Heracles — who was given impossible labors and completed every single one",
    "Ajax — who was the strongest man at Troy and still lost, and what that means",
    "Orpheus — who went into the underworld for love and almost made it back",
    "Daedalus — the craftsman who built the wings, watching his son fly higher than advised",
    "Atalanta — the woman who outran every man and lost only because she wanted to",
    # --- Philosophy / Thinkers ---
    "Marcus Aurelius — emperor, general, philosopher — writing his private thoughts at war",
    "Seneca — writing his final letters, knowing Nero will send the order to die soon",
    "Epictetus — a man who was literally enslaved and built the most radical philosophy of freedom",
    "Nietzsche — at the edge of madness, writing the most honest things anyone had said",
    "Camus — who looked at the absurdity of existence and said: revolt anyway",
    "Dostoevsky — who faced a firing squad, had his sentence commuted at the last second, and then wrote",
    "Kierkegaard — who understood that anxiety is the dizziness of freedom",
    "Schopenhauer — who named the will to live before anyone wanted to hear it",
    # --- Athletes ---
    "an athlete in the final year of their career, leaving absolutely nothing in reserve",
    "a marathon runner at mile 20, when the body wants to stop and the mind decides everything",
    "Kobe Bryant — in the gym at 4am, alone, building something no one would see until game day",
    "Muhammad Ali — not the showman, but the man who refused induction and paid for it with years",
    "a fighter who has been knocked down in the ring and chooses, in that second, to get up",
    "an Olympic athlete who peaked on the one day that mattered and spent a lifetime understanding that",
    # --- Creators / Artists ---
    "a filmmaker who bet their entire life savings on one film and had no plan B",
    "a writer who rewrote the same book for seven years because they refused to let it be less than great",
    "a musician who was dropped from every label and released the album anyway",
    "Bukowski — who worked the post office for a decade, writing at night, not asking for permission",
    "Rilke — writing letters to a young poet about patience and the questions that must be lived",
    "a painter who destroyed work that didn't reach the standard no one else could even see",
    "a sculptor who said: I don't add, I remove everything that isn't the statue",
    # --- Founders / Builders ---
    "a founder who failed publicly, went quiet for two years, and came back with something undeniable",
    "someone building a company in a field where the consensus is that it cannot be done",
    "Nikola Tesla — writing letters that would not be understood for decades",
    "a first-time founder who had one chance to pitch, knew it, and walked in anyway",
    "a builder who has been told no 47 times and is dialing the 48th number right now",
    # --- The Condemned / The Defiant ---
    "a death row prisoner writing their final letter — not asking for mercy, but leaving something true",
    "a samurai writing a death poem (jisei) the morning of their final battle",
    "someone facing ruin who chose to face it with dignity rather than beg",
    "a man who burned his own ships so there was no debate about whether to advance",
    "a whistleblower who told the truth knowing exactly what it would cost them",
]

_MOODS = [
    "cold, precise, and final — like a decision already executed, not debated",
    "slow-burning — the kind of quiet that precedes something that changes everything",
    "defiant — the specific energy of someone who was told no and is still here",
    "quiet and devastating — a single line that lands and rearranges something inside you",
    "blunt and undecorated — no metaphor, no softening, the fact delivered at full force",
    "ancient but shockingly current — something said 2,000 years ago that describes this morning",
    "cinematic — the final line before the cut to black in the greatest film never made",
    "mythic — something that belongs carved above a gate that only certain people walk through",
    "physically earned — wisdom that could only exist after having bled for something real",
    "almost unbearably honest — the thing ambitious people feel at 2am but never say out loud",
    "the kind of line that makes you physically rise from wherever you are sitting",
    "the specific feeling of standing at the edge of something irreversible and choosing to jump",
    "the controlled fury of someone who was written off and kept a list",
    "grief transformed into momentum — the kind of sorrow that becomes fuel",
    "the stillness of someone who has already decided and is simply waiting for the moment",
    "reverent and ruthless at once — like a prayer said through clenched teeth",
    "the loneliness of being the only person who believes in something — and doing it anyway",
    "the specific terror and joy of having no fallback position left",
    "like reading someone's private journal entry the night before they changed history",
    "the voice of someone who accepted death, failure, or loss — and became free because of it",
]

_DOMAINS = [
    "the hidden tax of playing it safe, paid daily in small surrenders across an entire life",
    "what separates those who actually build from those who study those who build",
    "the iron silence and private discipline required before a large irreversible decision",
    "the loneliness of compounding — doing the right thing for years before it shows",
    "why motion and progress are different things, and most people live in the illusion of the first",
    "the specific dignity of taking a risk that everyone around you thinks is insane",
    "what money actually reveals about a person — and the things it will never be able to measure",
    "the violence of hesitation — what it costs, compounded quietly, over a decade",
    "conviction that doesn't need an audience, a validator, or a reaction to survive",
    "what it means to be early, mocked, apparently wrong — and to hold the line anyway",
    "the gap between the fear you feel and the size of what you're actually capable of",
    "why the proof always comes late — and the work must happen in the absence of proof",
    "the exact moment a person stops negotiating with their own potential",
    "the price of greatness — paid in full, in private, in the years before anyone is watching",
    "what ancient myths understood about mortality and urgency that modern life has sedated",
    "the difference between a life that happened to you and a life you went and took",
    "what it feels like to be in the middle of something hard with no guarantee it works",
    "the relationship between how much you want something and how much of yourself you're willing to burn",
    "the specific grief of the person who almost — and what they do with that",
    "why the people who change things were always called delusional first",
    "the identity that forms in the gap between who you are and who you're becoming",
    "what you owe the version of yourself that believed in you before you had any proof",
    "the architecture of a mind that refuses to accept the ceiling others have built for it",
    "what it means to bet on yourself when no one else will — and be right",
    "the sacred, terrifying quality of a life fully committed to one direction",
]

# NOTE: this used to be one giant static block pasted into EVERY prompt.
# That's what was causing the repetition — the model kept completing
# toward the same names because they were always sitting in the context.
# Now it's a pool we sample 3 territories from per call, so the model
# never sees the same reference set twice in a row.
_SOURCE_POOLS = [
    "Greek & Roman myth — Prometheus, Icarus, Achilles, Odysseus, Sisyphus, "
    "Heracles, Daedalus, Orpheus, Leonidas, Atalanta, Ajax, Medea, Antigone. "
    "Reach for the unfamous passage, not the anthology line.",
    "Stoics under real pressure — Marcus Aurelius at the front, Seneca's "
    "final letters to Lucilius, Epictetus, Cato choosing death over mercy.",
    "War & strategy — Sun Tzu, Hannibal, Patton's unedited speeches, "
    "Wellington, Caesar's private letters, soldiers' last letters home.",
    "Literature that cuts — Dostoevsky, Camus, Nietzsche's notebooks, Kafka, "
    "Hemingway, Faulkner, Borges.",
    "Poetry of consequence — Rilke, Bukowski, Whitman, Dylan Thomas, Keats "
    "writing while dying, Milton, Plath.",
    "Sacred texts, the violent honest parts — Bhagavad Gita, Job, "
    "Ecclesiastes, Psalms of desperation, Tao Te Ching.",
    "Cinema that felt mythic — Apocalypse Now, There Will Be Blood, "
    "Whiplash, Raging Bull, No Country for Old Men, Ikiru, Seven Samurai.",
    "Anonymous & oral tradition — Spartan laconic sayings, samurai jisei "
    "death poems, last words of the condemned, prison letters.",
    "Founders & builders at the edge — Tesla's letters, Jobs's wilderness "
    "years, Bezos's original shareholder letters, Burry's pre-2008 letters.",
]

PROMPT = """You are a collector of words that hit like a physical force — lines that have been carved by real consequence.

Inhabit this perspective completely: {lens}

Surface ONE quote — spoken aloud, written in a letter, carved in stone, whispered before battle, or left anonymous — that carries this exact energy:

Mood: {mood}
Domain: {domain}

---

CALIBRATION — know exactly what you are hunting and what to throw back:

IMMEDIATELY DISCARD anything that resembles:
- "If you don't take risks, you'll have a wasted soul." [No teeth. No cost. No real stakes.]
- "The secret of getting ahead is getting started." [Hollow. Could be on a coffee mug.]
- "Believe in yourself and anything is possible." [Dishonest. Empty. Costs nothing to say.]
- "Success is not final, failure is not fatal." [Overexposed. The edge was sanded off long ago.]
- "Work hard, stay humble." [Bumper sticker. Not a quote. Not wisdom. Discard immediately.]
- "Chase your dreams." [If this surfaces, you have failed. Start over.]
- Anything that would appear on a LinkedIn carousel without irony.
- Anything a life coach would say without having lived anything.
- Anything that asks nothing of the person reading it.

Reach into THESE territories only this round — go to the buried, unfamous passage inside them,
not the greatest-hits line everyone already knows:
{territories}

THE ONE TEST — apply this ruthlessly:
Does reading this create a PHYSICAL RESPONSE? Chest tightens. Jaw sets. Something shifts in
the sternum. The urge to stand up and move. Not: does it sound smart? Not: is it famous?
Does it name something the reader has felt but never had language for? Does it cost something
to read — like it's asking something of you?
If yes to all: this is the quote. If no to any: discard and go deeper.

FINAL MANDATE:
Reach for the uncommon. The quote most people have never encountered. Not the famous line
everyone knows from these figures — the one buried in the notebook, the private letter, the
verse that gets cut from anthologies. Go to the edge of the source.

Hard rules:
- Real quote or real literary/mythic line with a genuine source.
- Use "Anonymous" only when origin is genuinely unknown.
- DO NOT use any of these authors/sources — all used in the last {n} weeks: {banned}
- Author field: be specific — name the person, the work, the context if needed.

Return ONLY valid JSON. Nothing else. No preamble. No explanation. No markdown."""


class QuoteStore:
    def __init__(self, model: str = "gemini-3.7-flash"):
        self.client = genai.Client(api_key=GEMINI_KEY)
        self.model = model

    def _load_history(self) -> list[str]:
        if HISTORY_PATH.exists():
            try:
                return json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
            except Exception:
                return []
        return []

    def _save_history(self, history: list[str], new_author: str):
        history.append(new_author)
        atomic_write_json(HISTORY_PATH, history[-HISTORY_LIMIT:])

    def write(self) -> dict:
        history = self._load_history()
        banned = ", ".join(history) if history else "none yet"

        territories = "\n".join(f"- {t}" for t in _rng.sample(_SOURCE_POOLS, 3))

        prompt = PROMPT.format(
            lens=_rng.choice(_LENSES),
            mood=_rng.choice(_MOODS),
            domain=_rng.choice(_DOMAINS),
            territories=territories,
            banned=banned,
            n=len(history),
        )

        response = self.client.models.generate_content(
            model=self.model,
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                temperature=1.9,
                response_schema=types.Schema(
                    type=types.Type.OBJECT,
                    properties={
                        "quote": types.Schema(type=types.Type.STRING),
                        "author": types.Schema(type=types.Type.STRING),
                    },
                    required=["quote", "author"],
                ),
            ),
        )
        data = json.loads(response.text)
        atomic_write_json(QUOTE_PATH, data)
        self._save_history(history, data.get("author", "Unknown"))
        return data

    @staticmethod
    def read() -> dict:
        return json.loads(QUOTE_PATH.read_text(encoding="utf-8"))


if __name__ == "__main__":
    store = QuoteStore()
    quote = store.write()
    print(json.dumps(quote, indent=2))
