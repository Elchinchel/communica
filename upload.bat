del /q /f dist

python -m build

pause

python -m twine upload dist/*

pause