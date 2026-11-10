#!/usr/bin/env bash
# Renders each chart in charts.html to <chart>-light.png and <chart>-dark.png
# at 2x, for the README. Needs Firefox, ImageMagick, and network access for
# the fonts. Edit the data in charts.html, then run this script.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
# Firefox installed as a snap reads only visible paths under $HOME, so the
# page renders from a copy there.
work=$(mktemp -d "$HOME/bighouse-render.XXXXXX")
trap 'rm -rf "$work"' EXIT
mkdir "$work/profile"
cp "$here/charts.html" "$work/"

capture() { # chart theme page-color window-height
  local chart=$1 theme=$2 bg=$3 height=$4
  timeout 90 firefox --headless --no-remote --profile "$work/profile" --window-size="2200,$height" \
    --screenshot "$work/shot.png" "file://$work/charts.html#$chart-$theme" >/dev/null 2>&1
  magick "$work/shot.png" -fuzz 1% -trim +repage -bordercolor "$bg" -border 48 -strip "$here/$chart-$theme.png"
  identify -format "%f %wx%h %b\n" "$here/$chart-$theme.png"
}

capture fleet light '#ffffff' 1800
capture fleet dark '#0d1117' 1800
capture metadata light '#ffffff' 2000
capture metadata dark '#0d1117' 2000
