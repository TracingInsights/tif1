"""Year-specific plotting constants for teams and compounds.

This module contains historically accurate team colors and compound colors
for each F1 season from 2018 onwards.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from typing import TypedDict

    class TeamColors(TypedDict):
        """Team color definition."""

        official: str
        fastf1: str

    class TeamData(TypedDict):
        """Team data definition."""

        short_name: str
        colors: TeamColors

    class YearData(TypedDict):
        """Year-specific data definition."""

        compound_colors: dict[str, str]
        teams: dict[str, TeamData]


# Year-specific constants from TracingInsights data
YEAR_CONSTANTS: dict[int, dict[str, Any]] = {
    2018: {
        "compound_colors": {
            "HYPERSOFT": "#feb1c1",
            "ULTRASOFT": "#b24ba7",
            "SUPERSOFT": "#fc2b2a",
            "SOFT": "#ffd318",
            "MEDIUM": "#f0f0f0",
            "HARD": "#00a2f5",
            "SUPERHARD": "#fd7d3c",
            "INTERMEDIATE": "#43b02a",
            "WET": "#0067ad",
            "UNKNOWN": "#00ffff",
            "TEST-UNKNOWN": "#434649",
        },
        "teams": {
            "ferrari": {
                "short_name": "Ferrari",
                "colors": {"official": "#dc0000", "fastf1": "#dc0000"},
            },
            "force india": {
                "short_name": "Force India",
                "colors": {"official": "#f596c8", "fastf1": "#ff87bc"},
            },
            "haas": {
                "short_name": "Haas",
                "colors": {"official": "#828282", "fastf1": "#b6babd"},
            },
            "mclaren": {
                "short_name": "McLaren",
                "colors": {"official": "#ff8000", "fastf1": "#ff8000"},
            },
            "mercedes": {
                "short_name": "Mercedes",
                "colors": {"official": "#00d2be", "fastf1": "#00f5d0"},
            },
            "racing point": {
                "short_name": "Racing Point",
                "colors": {"official": "#f596c8", "fastf1": "#ff87bc"},
            },
            "red bull": {
                "short_name": "Red Bull",
                "colors": {"official": "#1e41ff", "fastf1": "#1e41ff"},
            },
            "renault": {
                "short_name": "Renault",
                "colors": {"official": "#fff500", "fastf1": "#fff500"},
            },
            "sauber": {
                "short_name": "Sauber",
                "colors": {"official": "#9b0000", "fastf1": "#900000"},
            },
            "toro rosso": {
                "short_name": "Toro Rosso",
                "colors": {"official": "#469bff", "fastf1": "#2b4562"},
            },
            "williams": {
                "short_name": "Williams",
                "colors": {"official": "#ffffff", "fastf1": "#00a0dd"},
            },
        },
    },
    2019: {
        "compound_colors": {
            "SOFT": "#da291c",
            "MEDIUM": "#ffd12e",
            "HARD": "#f0f0ec",
            "INTERMEDIATE": "#43b02a",
            "WET": "#0067ad",
            "UNKNOWN": "#00ffff",
            "TEST-UNKNOWN": "#434649",
        },
        "teams": {
            "alfa romeo": {
                "short_name": "Alfa Romeo",
                "colors": {"official": "#9b0000", "fastf1": "#900000"},
            },
            "haas": {
                "short_name": "Haas",
                "colors": {"official": "#bd9e57", "fastf1": "#bd9e57"},
            },
            "ferrari": {
                "short_name": "Ferrari",
                "colors": {"official": "#dc0000", "fastf1": "#da291c"},
            },
            "mclaren": {
                "short_name": "McLaren",
                "colors": {"official": "#ff8700", "fastf1": "#ff8000"},
            },
            "mercedes": {
                "short_name": "Mercedes",
                "colors": {"official": "#00d2be", "fastf1": "#00d2be"},
            },
            "racing point": {
                "short_name": "Racing Point",
                "colors": {"official": "#f596c8", "fastf1": "#ff87bc"},
            },
            "red bull": {
                "short_name": "Red Bull",
                "colors": {"official": "#1e41ff", "fastf1": "#1e41ff"},
            },
            "renault": {
                "short_name": "Renault",
                "colors": {"official": "#fff500", "fastf1": "#fff500"},
            },
            "toro rosso": {
                "short_name": "Toro Rosso",
                "colors": {"official": "#469bff", "fastf1": "#2b4562"},
            },
            "williams": {
                "short_name": "Williams",
                "colors": {"official": "#ffffff", "fastf1": "#00a0dd"},
            },
        },
    },
    2020: {
        "compound_colors": {
            "SOFT": "#da291c",
            "MEDIUM": "#ffd12e",
            "HARD": "#f0f0ec",
            "INTERMEDIATE": "#43b02a",
            "WET": "#0067ad",
            "UNKNOWN": "#00ffff",
            "TEST-UNKNOWN": "#434649",
        },
        "teams": {
            "alfa romeo": {
                "short_name": "Alfa Romeo",
                "colors": {"official": "#9b0000", "fastf1": "#900000"},
            },
            "alphatauri": {
                "short_name": "AlphaTauri",
                "colors": {"official": "#ffffff", "fastf1": "#2b4562"},
            },
            "ferrari": {
                "short_name": "Ferrari",
                "colors": {"official": "#dc0000", "fastf1": "#dc0000"},
            },
            "haas": {
                "short_name": "Haas",
                "colors": {"official": "#787878", "fastf1": "#b6babd"},
            },
            "mclaren": {
                "short_name": "McLaren",
                "colors": {"official": "#ff8700", "fastf1": "#ff8000"},
            },
            "mercedes": {
                "short_name": "Mercedes",
                "colors": {"official": "#00d2be", "fastf1": "#00d2be"},
            },
            "racing point": {
                "short_name": "Racing Point",
                "colors": {"official": "#f596c8", "fastf1": "#ff87bc"},
            },
            "red bull": {
                "short_name": "Red Bull",
                "colors": {"official": "#1e41ff", "fastf1": "#1e41ff"},
            },
            "renault": {
                "short_name": "Renault",
                "colors": {"official": "#fff500", "fastf1": "#fff500"},
            },
            "williams": {
                "short_name": "Williams",
                "colors": {"official": "#0082fa", "fastf1": "#00a0dd"},
            },
        },
    },
    2021: {
        "compound_colors": {
            "SOFT": "#da291c",
            "MEDIUM": "#ffd12e",
            "HARD": "#f0f0ec",
            "INTERMEDIATE": "#43b02a",
            "WET": "#0067ad",
            "UNKNOWN": "#00ffff",
            "TEST-UNKNOWN": "#434649",
        },
        "teams": {
            "alfa romeo": {
                "short_name": "Alfa Romeo",
                "colors": {"official": "#900000", "fastf1": "#900000"},
            },
            "alphatauri": {
                "short_name": "AlphaTauri",
                "colors": {"official": "#2b4562", "fastf1": "#2b4562"},
            },
            "alpine": {
                "short_name": "Alpine",
                "colors": {"official": "#0090ff", "fastf1": "#0755ab"},
            },
            "aston martin": {
                "short_name": "Aston Martin",
                "colors": {"official": "#006f62", "fastf1": "#00665e"},
            },
            "ferrari": {
                "short_name": "Ferrari",
                "colors": {"official": "#dc0004", "fastf1": "#dc0004"},
            },
            "haas": {
                "short_name": "Haas",
                "colors": {"official": "#ffffff", "fastf1": "#b6babd"},
            },
            "mclaren": {
                "short_name": "McLaren",
                "colors": {"official": "#ff9800", "fastf1": "#ff8000"},
            },
            "mercedes": {
                "short_name": "Mercedes",
                "colors": {"official": "#00d2be", "fastf1": "#00f5d0"},
            },
            "red bull": {
                "short_name": "Red Bull",
                "colors": {"official": "#0600ef", "fastf1": "#0600ef"},
            },
            "williams": {
                "short_name": "Williams",
                "colors": {"official": "#005aff", "fastf1": "#00a0dd"},
            },
        },
    },
    2022: {
        "compound_colors": {
            "SOFT": "#da291c",
            "MEDIUM": "#ffd12e",
            "HARD": "#f0f0ec",
            "INTERMEDIATE": "#43b02a",
            "WET": "#0067ad",
            "UNKNOWN": "#00ffff",
            "TEST-UNKNOWN": "#434649",
        },
        "teams": {
            "alfa romeo": {
                "short_name": "Alfa Romeo",
                "colors": {"official": "#b12039", "fastf1": "#900000"},
            },
            "alphatauri": {
                "short_name": "AlphaTauri",
                "colors": {"official": "#4e7c9b", "fastf1": "#2b4562"},
            },
            "alpine": {
                "short_name": "Alpine",
                "colors": {"official": "#2293d1", "fastf1": "#fe86bc"},
            },
            "aston martin": {
                "short_name": "Aston Martin",
                "colors": {"official": "#2d826d", "fastf1": "#00665e"},
            },
            "ferrari": {
                "short_name": "Ferrari",
                "colors": {"official": "#ed1c24", "fastf1": "#da291c"},
            },
            "haas": {
                "short_name": "Haas",
                "colors": {"official": "#b6babd", "fastf1": "#b6babd"},
            },
            "mclaren": {
                "short_name": "McLaren",
                "colors": {"official": "#f58020", "fastf1": "#ff8000"},
            },
            "mercedes": {
                "short_name": "Mercedes",
                "colors": {"official": "#6cd3bf", "fastf1": "#00f5d0"},
            },
            "red bull": {
                "short_name": "Red Bull",
                "colors": {"official": "#1e5bc6", "fastf1": "#0600ef"},
            },
            "williams": {
                "short_name": "Williams",
                "colors": {"official": "#37bedd", "fastf1": "#00a0dd"},
            },
        },
    },
    2023: {
        "compound_colors": {
            "SOFT": "#da291c",
            "MEDIUM": "#ffd12e",
            "HARD": "#f0f0ec",
            "INTERMEDIATE": "#43b02a",
            "WET": "#0067ad",
            "UNKNOWN": "#00ffff",
            "TEST-UNKNOWN": "#434649",
        },
        "teams": {
            "alfa romeo": {
                "short_name": "Alfa Romeo",
                "colors": {"official": "#c92d4b", "fastf1": "#900000"},
            },
            "alphatauri": {
                "short_name": "AlphaTauri",
                "colors": {"official": "#5e8faa", "fastf1": "#2b4562"},
            },
            "alpine": {
                "short_name": "Alpine",
                "colors": {"official": "#2293d1", "fastf1": "#fe86bc"},
            },
            "aston martin": {
                "short_name": "Aston Martin",
                "colors": {"official": "#358c75", "fastf1": "#00665e"},
            },
            "ferrari": {
                "short_name": "Ferrari",
                "colors": {"official": "#f91536", "fastf1": "#da291c"},
            },
            "haas": {
                "short_name": "Haas",
                "colors": {"official": "#b6babd", "fastf1": "#b6babd"},
            },
            "mclaren": {
                "short_name": "McLaren",
                "colors": {"official": "#f58020", "fastf1": "#ff8000"},
            },
            "mercedes": {
                "short_name": "Mercedes",
                "colors": {"official": "#6cd3bf", "fastf1": "#00f5d0"},
            },
            "red bull": {
                "short_name": "Red Bull",
                "colors": {"official": "#3671c6", "fastf1": "#0600ef"},
            },
            "williams": {
                "short_name": "Williams",
                "colors": {"official": "#37bedd", "fastf1": "#00a0dd"},
            },
        },
    },
    2024: {
        "compound_colors": {
            "SOFT": "#da291c",
            "MEDIUM": "#ffd12e",
            "HARD": "#f0f0ec",
            "INTERMEDIATE": "#43b02a",
            "WET": "#0067ad",
            "UNKNOWN": "#00ffff",
            "TEST-UNKNOWN": "#434649",
        },
        "teams": {
            "alpine": {
                "short_name": "Alpine",
                "colors": {"official": "#0093cc", "fastf1": "#ff87bc"},
            },
            "aston martin": {
                "short_name": "Aston Martin",
                "colors": {"official": "#229971", "fastf1": "#00665f"},
            },
            "ferrari": {
                "short_name": "Ferrari",
                "colors": {"official": "#e8002d", "fastf1": "#e8002d"},
            },
            "haas": {
                "short_name": "Haas",
                "colors": {"official": "#b6babd", "fastf1": "#b6babd"},
            },
            "mclaren": {
                "short_name": "McLaren",
                "colors": {"official": "#ff8000", "fastf1": "#ff8000"},
            },
            "mercedes": {
                "short_name": "Mercedes",
                "colors": {"official": "#27f4d2", "fastf1": "#27f4d2"},
            },
            "rb": {
                "short_name": "RB",
                "colors": {"official": "#6692ff", "fastf1": "#364aa9"},
            },
            "red bull": {
                "short_name": "Red Bull",
                "colors": {"official": "#3671c6", "fastf1": "#0600ef"},
            },
            "kick sauber": {
                "short_name": "Sauber",
                "colors": {"official": "#52e252", "fastf1": "#00e700"},
            },
            "williams": {
                "short_name": "Williams",
                "colors": {"official": "#64c4ff", "fastf1": "#00a0dd"},
            },
        },
    },
    2025: {
        "compound_colors": {
            "SOFT": "#da291c",
            "MEDIUM": "#ffd12e",
            "HARD": "#f0f0ec",
            "INTERMEDIATE": "#43b02a",
            "WET": "#0067ad",
            "UNKNOWN": "#00ffff",
            "TEST-UNKNOWN": "#434649",
        },
        "teams": {
            "alpine": {
                "short_name": "Alpine",
                "colors": {"official": "#0093cc", "fastf1": "#ff87bc"},
            },
            "aston martin": {
                "short_name": "Aston Martin",
                "colors": {"official": "#229971", "fastf1": "#00665f"},
            },
            "ferrari": {
                "short_name": "Ferrari",
                "colors": {"official": "#e80020", "fastf1": "#e80020"},
            },
            "haas": {
                "short_name": "Haas",
                "colors": {"official": "#b6babd", "fastf1": "#b6babd"},
            },
            "mclaren": {
                "short_name": "McLaren",
                "colors": {"official": "#ff8000", "fastf1": "#ff8000"},
            },
            "mercedes": {
                "short_name": "Mercedes",
                "colors": {"official": "#27f4d2", "fastf1": "#27f4d2"},
            },
            "racing bulls": {
                "short_name": "RB",
                "colors": {"official": "#6692ff", "fastf1": "#fcd700"},
            },
            "red bull": {
                "short_name": "Red Bull",
                "colors": {"official": "#3671c6", "fastf1": "#0600ef"},
            },
            "kick sauber": {
                "short_name": "Sauber",
                "colors": {"official": "#52e252", "fastf1": "#00e700"},
            },
            "williams": {
                "short_name": "Williams",
                "colors": {"official": "#64c4ff", "fastf1": "#00a0dd"},
            },
        },
    },
    2026: {
        "compound_colors": {
            "SOFT": "#da291c",
            "MEDIUM": "#ffd12e",
            "HARD": "#f0f0ec",
            "INTERMEDIATE": "#43b02a",
            "WET": "#0067ad",
            "UNKNOWN": "#00ffff",
            "TEST-UNKNOWN": "#434649",
        },
        "teams": {
            "alpine": {
                "short_name": "Alpine",
                "colors": {"official": "#0093cc", "fastf1": "#ff87bc"},
            },
            "aston martin": {
                "short_name": "Aston Martin",
                "colors": {"official": "#229971", "fastf1": "#00665f"},
            },
            "audi": {
                "short_name": "Audi",
                "colors": {"official": "#ff2d00", "fastf1": "#ff2d00"},
            },
            "cadillac": {
                "short_name": "Cadillac",
                "colors": {"official": "#444444", "fastf1": "#444444"},
            },
            "ferrari": {
                "short_name": "Ferrari",
                "colors": {"official": "#e80020", "fastf1": "#e80020"},
            },
            "haas": {
                "short_name": "Haas",
                "colors": {"official": "#b6babd", "fastf1": "#b6babd"},
            },
            "mclaren": {
                "short_name": "McLaren",
                "colors": {"official": "#ff8000", "fastf1": "#ff8000"},
            },
            "mercedes": {
                "short_name": "Mercedes",
                "colors": {"official": "#27f4d2", "fastf1": "#27f4d2"},
            },
            "racing bulls": {
                "short_name": "RB",
                "colors": {"official": "#6692ff", "fastf1": "#fcd700"},
            },
            "red bull": {
                "short_name": "Red Bull",
                "colors": {"official": "#3671c6", "fastf1": "#0600ef"},
            },
            "williams": {
                "short_name": "Williams",
                "colors": {"official": "#64c4ff", "fastf1": "#00a0dd"},
            },
        },
    },
}

# Default fallback colors (used when year is not available or for unknown teams)
DEFAULT_COMPOUND_COLORS = {
    "SOFT": "#da291c",
    "MEDIUM": "#ffd12e",
    "HARD": "#f0f0ec",
    "INTERMEDIATE": "#43b02a",
    "WET": "#0067ad",
    "UNKNOWN": "#00ffff",
    "TEST-UNKNOWN": "#434649",
}


# The TracingInsights v2 chart palette (``utils.team_colors`` in the v2
# analysis scripts, used by ``Fastest_Lap.py`` for bar colours). Canonical
# timing-data team names map to the bright v2 palette.
TEAM_COLORS: dict[int, dict[str, str]] = {
    2018: {
        "Red Bull Racing": "#000099",
        "Renault": "#ffe119",
        "Toro Rosso": "#dcbeff",
        "Force India": "#f032e6",
        "Sauber": "#800000",
        "Mercedes": "#00c0bf",
        "Ferrari": "#e6194b",
        "McLaren": "#f58231",
        "Haas F1 Team": "#ffffff",
        "Williams": "#4363d8",
    },
    2019: {
        "Red Bull Racing": "#000099",
        "Renault": "#ffe119",
        "Racing Point": "#f032e6",
        "Toro Rosso": "#dcbeff",
        "Mercedes": "#00c0bf",
        "Ferrari": "#e6194b",
        "McLaren": "#f58231",
        "Alfa Romeo Racing": "#800000",
        "Haas F1 Team": "#ffffff",
        "Williams": "#4363d8",
    },
    2020: {
        "Red Bull Racing": "#000099",
        "Renault": "#ffe119",
        "Racing Point": "#f032e6",
        "Mercedes": "#00c0bf",
        "Ferrari": "#e6194b",
        "McLaren": "#f58231",
        "Alfa Romeo Racing": "#800000",
        "Haas F1 Team": "#ffffff",
        "AlphaTauri": "#dcbeff",
        "Williams": "#4363d8",
    },
    2021: {
        "Red Bull Racing": "#ffe119",
        "Mercedes": "#00c0bf",
        "Ferrari": "#e6194b",
        "Alpine": "#f032e6",
        "McLaren": "#f58231",
        "Alfa Romeo Racing": "#800000",
        "Aston Martin": "#3cb44b",
        "Haas F1 Team": "#ffffff",
        "AlphaTauri": "#dcbeff",
        "Williams": "#4363d8",
    },
    2022: {
        "Red Bull Racing": "#ffe119",
        "Ferrari": "#e6194b",
        "Aston Martin": "#3cb44b",
        "Mercedes": "#00c0bf",
        "Alpine": "#f032e6",
        "Haas F1 Team": "#ffffff",
        "McLaren": "#f58231",
        "Alfa Romeo": "#800000",
        "AlphaTauri": "#dcbeff",
        "Williams": "#4363d8",
    },
    2023: {
        "Red Bull Racing": "#ffe119",
        "Ferrari": "#e6194b",
        "Aston Martin": "#3cb44b",
        "Mercedes": "#00c0bf",
        "Alpine": "#f032e6",
        "Haas F1 Team": "#ffffff",
        "McLaren": "#f58231",
        "Alfa Romeo": "#800000",
        "AlphaTauri": "#dcbeff",
        "Williams": "#4363d8",
    },
    2024: {
        "Red Bull Racing": "#ffe119",
        "Ferrari": "#e6194b",
        "Aston Martin": "#3cb44b",
        "Mercedes": "#00c0bf",
        "Alpine": "#f032e6",
        "Haas F1 Team": "#ffffff",
        "McLaren": "#f58231",
        "Kick Sauber": "#00ff00",
        "RB": "#dcbeff",
        "Williams": "#4363d8",
    },
    2025: {
        "Red Bull Racing": "#ffe119",
        "Ferrari": "#e6194b",
        "Aston Martin": "#3cb44b",
        "Mercedes": "#00c0bf",
        "Alpine": "#f032e6",
        "Haas F1 Team": "#ffffff",
        "McLaren": "#f58231",
        "Kick Sauber": "#00ff00",
        "Racing Bulls": "#dcbeff",
        "Williams": "#4363d8",
    },
    2026: {
        "Red Bull Racing": "#ffe119",
        "Ferrari": "#e6194b",
        "Aston Martin": "#3cb44b",
        "Mercedes": "#00c0bf",
        "Alpine": "#f032e6",
        "Haas F1 Team": "#ffffff",
        "McLaren": "#f58231",
        "Kick Sauber": "#00ff00",
        "Racing Bulls": "#dcbeff",
        "Williams": "#4363d8",
        "Audi": "#9a9a9a",
        "Cadillac": "#C5A253",
    },
}


# Mapping of timing-data team names to the bundled car image codes (file stems
# inside ``tif1/assets/cars/<year>/``). Ported from the TracingInsights v2
# analysis scripts (``F1-analysis/v2/utils.py``) so that the same team names
# appearing in lap data resolve to the correct car artwork.
TEAM_CODES: dict[int, dict[str, str]] = {
    2018: {
        "Red Bull Racing": "RBR",
        "Red Bull Racing TAG Heuer": "RBR",
        "Red Bull": "RBR",
        "Renault": "REN",
        "Toro Rosso": "TR",
        "Scuderia Toro Rosso Honda": "TR",
        "Force India": "FI",
        "Force India Sahara": "FI",
        "Force India Mercedes": "FI",
        "Sauber": "SB",
        "Sauber Ferrari": "SB",
        "Mercedes": "MER",
        "Ferrari": "FER",
        "McLaren": "MCL",
        "McLaren Renault": "MCL",
        "Haas F1 Team": "HAA",
        "Haas Ferrari": "HAA",
        "Williams": "WIL",
        "Williams Mercedes": "WIL",
    },
    2019: {
        "Red Bull Racing": "RBR",
        "Red Bull Racing Honda": "RBR",
        "Red Bull": "RBR",
        "Renault": "REN",
        "Racing Point": "RP",
        "Racing Point BWT Mercedes": "RP",
        "Toro Rosso": "TR",
        "Scuderia Toro Rosso Honda": "TR",
        "Mercedes": "MER",
        "Ferrari": "FER",
        "McLaren": "MCL",
        "McLaren Renault": "MCL",
        "Alfa Romeo Racing": "ARR",
        "Alfa Romeo Racing Ferrari": "ARR",
        "Alfa Romeo": "ARR",
        "Haas F1 Team": "HAA",
        "Haas Ferrari": "HAA",
        "Williams": "WIL",
        "Williams Mercedes": "WIL",
    },
    2020: {
        "Red Bull Racing": "RBR",
        "Red Bull Racing Honda": "RBR",
        "Red Bull": "RBR",
        "Renault": "REN",
        "Racing Point": "RP",
        "Racing Point BWT Mercedes": "RP",
        "Mercedes": "MER",
        "Ferrari": "FER",
        "McLaren": "MCL",
        "McLaren Renault": "MCL",
        "Alfa Romeo Racing": "ARR",
        "Alfa Romeo Racing Ferrari": "ARR",
        "Alfa Romeo": "ARR",
        "Haas F1 Team": "HAA",
        "Haas Ferrari": "HAA",
        "AlphaTauri": "APT",
        "AlphaTauri Honda": "APT",
        "Williams": "WIL",
        "Williams Mercedes": "WIL",
    },
    2021: {
        "Red Bull Racing": "RBR",
        "Red Bull Racing Honda": "RBR",
        "Red Bull": "RBR",
        "Mercedes": "MER",
        "Ferrari": "FER",
        "Alpine": "APN",
        "Alpine Renault": "APN",
        "Alpine F1 Team": "APN",
        "McLaren": "MCL",
        "McLaren Mercedes": "MCL",
        "Alfa Romeo Racing": "ARR",
        "Alfa Romeo Racing Ferrari": "ARR",
        "Alfa Romeo": "ARR",
        "Aston Martin": "AMR",
        "Aston Martin Mercedes": "AMR",
        "Haas F1 Team": "HAA",
        "Haas Ferrari": "HAA",
        "AlphaTauri": "APT",
        "AlphaTauri Honda": "APT",
        "Williams": "WIL",
        "Williams Mercedes": "WIL",
    },
    2022: {
        "Red Bull Racing": "RBR",
        "Red Bull Racing RBPT": "RBR",
        "Red Bull": "RBR",
        "Ferrari": "FER",
        "Aston Martin": "AMR",
        "Aston Martin Aramco Mercedes": "AMR",
        "Mercedes": "MER",
        "Alpine": "APN",
        "Alpine Renault": "APN",
        "Alpine F1 Team": "APN",
        "Haas F1 Team": "HAA",
        "Haas Ferrari": "HAA",
        "McLaren": "MCL",
        "McLaren Mercedes": "MCL",
        "Alfa Romeo": "ARR",
        "Alfa Romeo Ferrari": "ARR",
        "AlphaTauri": "APT",
        "AlphaTauri RBPT": "APT",
        "Williams": "WIL",
        "Williams Mercedes": "WIL",
    },
    2023: {
        "Red Bull Racing": "RBR",
        "Red Bull Racing Honda RBPT": "RBR",
        "Red Bull": "RBR",
        "Ferrari": "FER",
        "Aston Martin": "AMR",
        "Aston Martin Aramco Mercedes": "AMR",
        "Mercedes": "MER",
        "Alpine": "APN",
        "Alpine Renault": "APN",
        "Alpine F1 Team": "APN",
        "Haas F1 Team": "HAA",
        "Haas Ferrari": "HAA",
        "McLaren": "MCL",
        "McLaren Mercedes": "MCL",
        "Alfa Romeo": "ARR",
        "Alfa Romeo Ferrari": "ARR",
        "AlphaTauri": "APT",
        "AlphaTauri Honda RBPT": "APT",
        "Williams": "WIL",
        "Williams Mercedes": "WIL",
    },
    2024: {
        "Red Bull Racing": "RBR",
        "Red Bull Racing Honda RBPT": "RBR",
        "Red Bull": "RBR",
        "Ferrari": "FER",
        "Aston Martin": "AMR",
        "Aston Martin Aramco Mercedes": "AMR",
        "Mercedes": "MER",
        "Alpine": "APN",
        "Alpine Renault": "APN",
        "Alpine F1 Team": "APN",
        "Haas F1 Team": "HAA",
        "Haas Ferrari": "HAA",
        "McLaren": "MCL",
        "McLaren Mercedes": "MCL",
        "Kick Sauber": "KS",
        "Kick Sauber Ferrari": "KS",
        "Alfa Romeo Ferrari": "KS",
        "RB": "RB",
        "AlphaTauri Honda RBPT": "RB",
        "Racing Bulls Honda RBPT": "RB",
        "Williams": "WIL",
        "Williams Mercedes": "WIL",
    },
    2025: {
        "Red Bull Racing": "RBR",
        "Red Bull Racing Honda RBPT": "RBR",
        "Red Bull": "RBR",
        "Ferrari": "FER",
        "Aston Martin": "AMR",
        "Aston Martin Aramco Mercedes": "AMR",
        "Mercedes": "MER",
        "Alpine": "APN",
        "Alpine Renault": "APN",
        "Alpine F1 Team": "APN",
        "Haas F1 Team": "HAA",
        "Haas Ferrari": "HAA",
        "McLaren": "MCL",
        "McLaren Mercedes": "MCL",
        "Kick Sauber": "KS",
        "Kick Sauber Ferrari": "KS",
        "Racing Bulls": "RB",
        "Racing Bulls Honda RBPT": "RB",
        "RB Honda RBPT": "RB",
        "Williams": "WIL",
        "Williams Mercedes": "WIL",
    },
    2026: {
        "Red Bull Racing": "RBR",
        "Red Bull Racing Honda RBPT": "RBR",
        "Red Bull": "RBR",
        "Ferrari": "FER",
        "Aston Martin": "AMR",
        "Aston Martin Aramco Mercedes": "AMR",
        "Mercedes": "MER",
        "Alpine": "APN",
        "Alpine Renault": "APN",
        "Alpine F1 Team": "APN",
        "Haas F1 Team": "HAA",
        "Haas Ferrari": "HAA",
        "McLaren": "MCL",
        "McLaren Mercedes": "MCL",
        "Kick Sauber": "KS",
        "Kick Sauber Ferrari": "KS",
        "Racing Bulls": "RB",
        "Racing Bulls Honda RBPT": "RB",
        "Williams": "WIL",
        "Williams Mercedes": "WIL",
        "Audi": "AUD",
        "Cadillac": "CAD",
    },
}
