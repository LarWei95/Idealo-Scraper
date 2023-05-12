#!/bin/bash
vim \
    -c "edit dbbot_start.sh" \
    -c "tabedit mains/main.py" \
    -c "tabedit control/scraping.py" \
    -c "tabedit control/loader.py"
