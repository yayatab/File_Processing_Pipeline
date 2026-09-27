# AI Tool Usage
## Tools I Used
used AntiGravity and gemini-cli
## What Helped Most
a. plugins that make sure the output is as concise as possible. and as human-readable as possible. (less comments)
b. plan mode with several iterations. as carpenters say - measure 7 times, cut once.
## What I Had to Fix
change List to list (with `find . -path "*.venv*" -prune -o -path "*src*" -name "*.py" -type f -exec sed -i '' 's/List/list/g' {} \;`)</br>
include ruff</br>
becuase/thanks to YAGNI (you ain't gonna need it later), th AI struggled with implementing things ahead.</br>
instead of robust functions, i had to split them myself. (could have aske the ai to do it. but it was faster this way)
## What AI Struggled With
prefered to do stuff itself instead of using builtin functions. (i.e. create a dict instead of using model_dump)</br>
