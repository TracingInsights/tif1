"""Demo of the plotting color API compatible with FastF1."""

import pandas as pd

from tif1 import plotting


class MockSession:
    """Mock session for demo."""

    def __init__(self, year=None):
        self.year = year
        self.drivers_df = pd.DataFrame(
            {
                "Driver": ["VER", "PER", "HAM", "RUS", "LEC", "SAI"],
                "Team": [
                    "Red Bull Racing",
                    "Red Bull Racing",
                    "Mercedes",
                    "Mercedes",
                    "Ferrari",
                    "Ferrari",
                ],
                "FirstName": ["Max", "Sergio", "Lewis", "George", "Charles", "Carlos"],
                "LastName": ["Verstappen", "Perez", "Hamilton", "Russell", "Leclerc", "Sainz"],
                "DriverNumber": ["1", "11", "44", "63", "16", "55"],
                "TeamColor": ["#0600ef", "#0600ef", "#00d2be", "#00d2be", "#dc0000", "#dc0000"],
                "HeadshotUrl": [""] * 6,
            }
        )


def main():
    """Demonstrate plotting color API."""
    session = MockSession(year=2024)

    print("=== TIF1 Plotting Color API Demo ===\n")

    # Set default colormap
    print("1. Setting default colormap to 'fastf1'")
    plotting.set_default_colormap("fastf1")
    print(f"   Default colormap: {plotting._DEFAULT_COLORMAP}\n")

    # Get team colors
    print("2. Team Colors (2024 season):")
    for team in ["Red Bull Racing", "Ferrari", "Mercedes", "McLaren"]:
        color = plotting.get_team_color(team, session)
        print(f"   {team:20s} -> {color}")
    print()

    # Year-specific colors
    print("3. Year-Specific Team Colors:")
    session_2018 = MockSession(year=2018)
    session_2024 = MockSession(year=2024)
    print(f"   Ferrari 2018: {plotting.get_team_color('Ferrari', session_2018)}")
    print(f"   Ferrari 2024: {plotting.get_team_color('Ferrari', session_2024)}")
    print()

    # Get driver colors (returns team color)
    print("4. Driver Colors (same as team colors):")
    for driver in ["VER", "HAM", "LEC"]:
        color = plotting.get_driver_color(driver, session)
        name = plotting.get_driver_name(driver, session)
        print(f"   {driver} ({name:20s}) -> {color}")
    print()

    # Get compound colors
    print("5. Compound Colors (2024 season):")
    for compound in ["SOFT", "MEDIUM", "HARD", "INTERMEDIATE", "WET"]:
        color = plotting.get_compound_color(compound, session)
        print(f"   {compound:12s} -> {color}")
    print()

    # Year-specific compound colors
    print("6. Year-Specific Compound Colors:")
    print("   2018 (old system):")
    for compound in ["HYPERSOFT", "ULTRASOFT", "SUPERSOFT", "SOFT"]:
        color = plotting.get_compound_color(compound, session_2018)
        print(f"     {compound:12s} -> {color}")
    print("   2024 (current system):")
    for compound in ["SOFT", "MEDIUM", "HARD"]:
        color = plotting.get_compound_color(compound, session_2024)
        print(f"     {compound:12s} -> {color}")
    print()

    # List all drivers
    print("7. All Drivers in Session:")
    drivers = plotting.list_driver_abbreviations(session)
    print(f"   {', '.join(drivers)}\n")

    # List all teams
    print("8. All Teams in Session:")
    teams = plotting.list_team_names(session)
    for team in teams:
        print(f"   - {team}")
    print()

    # Get drivers by team
    print("9. Drivers by Team:")
    for team in ["Red Bull", "Ferrari"]:
        drivers = plotting.get_driver_abbreviations_by_team(team, session)
        print(f"   {team}: {', '.join(drivers)}")
    print()

    # Get driver color mapping
    print("10. Driver Color Mapping:")
    mapping = plotting.get_driver_color_mapping(session)
    for driver, color in list(mapping.items())[:3]:
        print(f"   {driver} -> {color}")
    print()

    # Get driver style
    print("11. Driver Styles (for plotting):")
    for driver in ["VER", "PER"]:
        style = plotting.get_driver_style(driver, ["color", "marker"], session)
        print(f"   {driver}: {style}")
    print()

    # Custom driver styles
    print("12. Custom Driver Styles:")
    custom_styles = [
        {"linestyle": "solid", "color": "auto", "linewidth": 2},
        {"linestyle": "dashed", "color": "auto", "linewidth": 2},
    ]
    for driver in ["VER", "PER"]:
        style = plotting.get_driver_style(driver, custom_styles, session)
        print(f"   {driver}: {style}")
    print()

    # New teams in 2026
    print("13. New Teams in 2026:")
    session_2026 = MockSession(year=2026)
    print(f"   Audi:     {plotting.get_team_color('Audi', session_2026)}")
    print(f"   Cadillac: {plotting.get_team_color('Cadillac', session_2026)}")
    print()

    print("=== Demo Complete ===")


if __name__ == "__main__":
    main()
