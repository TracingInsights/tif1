#!/usr/bin/env bash
# One-time asset download for tif1 bundled plot assets.
# Downloads car images (2018-2026), tyre compound images, and copies fonts.
set -euo pipefail

cd "$(dirname "$0")/.."

mkdir -p src/tif1/assets/cars src/tif1/assets/tyres src/tif1/assets/fonts

BASE="https://raw.githubusercontent.com/tracinginsights/F1/main/cars"

declare -A CODES
CODES[2018]="FER FI HAA MCL MER RBR REN SB TR WIL"
CODES[2019]="ARR FER HAA MCL MER RBR REN RP TR WIL"
CODES[2020]="APT ARR FER HAA MCL MER RBR REN RP WIL"
CODES[2021]="AMR APN APT ARR FER HAA MCL MER RBR WIL"
CODES[2022]="AMR APN APT ARR FER HAA MCL MER RBR WIL"
CODES[2023]="AMR APN APT ARR FER HAA MCL MER RBR WIL"
CODES[2024]="AMR APN FER HAA KS MCL MER RB RBR WIL"
CODES[2025]="AMR APN FER HAA KS MCL MER RB RBR WIL"
CODES[2026]="AMR APN AUD CAD FER HAA MCL MER RB RBR WIL"

for year in "${!CODES[@]}"; do
  mkdir -p "src/tif1/assets/cars/$year"
  for code in ${CODES[$year]}; do
    curl -sf "$BASE/$year/$code.png" -o "src/tif1/assets/cars/$year/$code.png" \
      || echo "FAILED: $year/$code"
  done
done

for compound in SOFT MEDIUM HARD INTERMEDIATE WET HARD1 None; do
  curl -sf "https://raw.githubusercontent.com/tracinginsights/F1/main/v4/$compound.png" \
    -o "src/tif1/assets/tyres/$compound.png" || echo "FAILED: tyre $compound"
done

for font in "F1-analysis/Tenada.ttf" "F1-analysis/coolvetica rg.otf" "F1-analysis/Azonix.otf" "F1-analysis/GreatVibes-Regular.ttf"; do
  cp "$font" "src/tif1/assets/fonts/" 2>/dev/null || echo "missing font: $font"
done

echo "--- summary ---"
find src/tif1/assets -type f | sort
echo "--- total size ---"
du -sh src/tif1/assets
